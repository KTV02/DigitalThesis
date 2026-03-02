#!/usr/bin/env python3
"""
metrics_from_csvs.py

Compute daily health metrics from individually supplied CSV files (raw or interpolated).

Inputs (all optional except --sleep-csv):
  --sleep-csv            Path to sleep_episodes.csv  (required for sleep metrics & sunrise deviation)
  --heart-rate-csv       Path to heart_rate.csv      (needed for resting HR, cosinor, VO2 fallback)
  --steps-csv            Path to steps.csv           (preferred for MVPA minutes)
  --vo2max-csv           Path to vo2max.csv          (native VO2 if present)
  --resting-hr-csv       Path to resting_hr.csv      (native resting HR if present)

Output (written to --out-dir):
  - mvpa_minutes.csv
  - resting_hr.csv
  - vo2max.csv
  - sleep_efficiency.csv
  - sleep_deviation_vs_sun.csv
  - hr_cosinor.csv
  - summary.json

Dates:
  --start/--end as YYYY-MM-DD (inclusive). All times are interpreted as local-naive.

MVPA:
  If --steps-csv provided, MVPA is computed as the number of minutes whose per-minute steps
  meet/exceed --mvpa-cadence-threshold (default 100 steps/min).
  If steps absent, fallback to heart-rate–based MVPA via HRR>=0.40 (requires HR).

Example:
  python metrics_from_csvs.py \
    --sleep-csv ./sleep_episodes.csv \
    --heart-rate-csv ./heart_rate_minute.csv \
    --steps-csv ./steps.csv \
    --vo2max-csv ./vo2max_daily.csv \
    --out-dir ./metrics_out \
    --start 2025-09-01 --end 2025-09-30 \
    --lat 52.52 --lon 13.405
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Optional, List, Tuple, Dict

import numpy as np
import pandas as pd

# ----------------------------- utilities -----------------------------

def _daterange_str(start: Optional[str], end: Optional[str]) -> List[str]:
    if not start or not end:
        return []
    s = pd.to_datetime(start).date()
    e = pd.to_datetime(end).date()
    return pd.date_range(s, e, freq="D").date.astype(str).tolist()

def _read_csv(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        print(f"[WARN] CSV not found: {p}")
        return None
    try:
        return pd.read_csv(p)
    except Exception as e:
        print(f"[WARN] Failed reading {p}: {e}")
        return None

def _coerce_dtcol(df: pd.DataFrame, candidates: List[str]) -> Tuple[pd.DataFrame, Optional[str]]:
    for c in candidates:
        if c in df.columns:
            col = c
            break
    else:
        return df, None
    if not np.issubdtype(df[col].dtype, np.datetime64):
        ser = pd.to_datetime(df[col], errors="coerce")
        if ser.isna().mean() > 0.5:
            ser = pd.to_datetime(df[col].astype(str).str.replace("T", " "), errors="coerce")
        df[col] = ser
    return df, col

def _safe(df: Optional[pd.DataFrame], cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame({c: [] for c in cols})
    for c in cols:
        if c not in df.columns:
            df[c] = []
    return df[cols]

def _write_csv(df: Optional[pd.DataFrame], path: Path, cols: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    out = _safe(df, cols)
    out.to_csv(path, index=False)
    print(f"[write] {path}  rows={len(out)}")

# ----------------------------- loaders -----------------------------

def _load_hr_df(hr_csv: Optional[Path]) -> Optional[pd.DataFrame]:
    df = _read_csv(hr_csv)
    if df is None or df.empty:
        return None
    ren = {}
    if "hr" in df.columns and "bpm" not in df.columns:
        ren["hr"] = "bpm"
    # recognize 'minute' as timestamp too
    if "ts" in df.columns and "time" not in df.columns and "datetime" not in df.columns and "minute" not in df.columns:
        ren["ts"] = "time"
    df = df.rename(columns=ren)
    df, tcol = _coerce_dtcol(df, ["time", "datetime", "minute", "ts"])
    if tcol is None or "bpm" not in df.columns:
        print(f"[WARN] heart-rate CSV missing timestamp or bpm (have: {list(df.columns)})")
        return None
    df = df.rename(columns={tcol: "time"})
    return df[["time", "bpm"]].dropna(subset=["time"])

def _load_steps_df(steps_csv: Optional[Path]) -> Optional[pd.DataFrame]:
    """
    Accepts either minute-level steps (preferred):
      columns: minute|time|datetime , steps
    or interval-level:
      columns: start_datetime|start , end_datetime|end , steps
    Returns a DataFrame with columns ['time','steps'] at per-minute resolution.
    """
    df = _read_csv(steps_csv)
    if df is None or df.empty:
        return None

    cols = {c.lower(): c for c in df.columns}
    # Minute-level?
    time_col = None
    for cand in ["minute", "time", "datetime"]:
        if cand in cols:
            time_col = cols[cand]
            break
    if time_col and ("steps" in df.columns):
        df, tcol = _coerce_dtcol(df, [time_col])
        if tcol is None:
            return None
        out = df.rename(columns={tcol: "time"})[["time", "steps"]].copy()
        out["time"] = out["time"].dt.floor("min")
        out["steps"] = pd.to_numeric(out["steps"], errors="coerce")
        out = out.dropna(subset=["time"])
        return out.groupby("time", as_index=False)["steps"].sum()

    # Interval-level?
    s_col = None; e_col = None
    for cand in ["start_datetime","start"]:
        if cand in cols: s_col = cols[cand]; break
    for cand in ["end_datetime","end"]:
        if cand in cols: e_col = cols[cand]; break
    if s_col and e_col and ("steps" in df.columns):
        tmp = df[[s_col, e_col, "steps"]].copy()
        tmp, sc = _coerce_dtcol(tmp, [s_col])
        tmp, ec = _coerce_dtcol(tmp, [e_col])
        tmp = tmp.rename(columns={sc: "start", ec: "end"})
        tmp = tmp.dropna(subset=["start","end"])
        tmp = tmp[tmp["end"] > tmp["start"]]
        tmp["steps"] = pd.to_numeric(tmp["steps"], errors="coerce").fillna(0).astype(float)

        # Evenly spread steps across minutes in the interval
        rows = []
        for _, r in tmp.iterrows():
            start = r["start"].floor("min")
            end   = r["end"].ceil("min")
            nmin = int(max(1, (end - start).total_seconds() // 60))
            per_min = r["steps"] / nmin
            mins = pd.date_range(start, periods=nmin, freq="T")
            rows.append(pd.DataFrame({"time": mins, "steps": per_min}))
        out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["time","steps"])
        return out.groupby("time", as_index=False)["steps"].sum()

    print(f"[WARN] steps CSV not recognized (need minute+steps OR start/end/steps). Have: {list(df.columns)}")
    return None

def _load_sleep_df(sleep_csv: Path) -> Optional[pd.DataFrame]:
    df = _read_csv(sleep_csv)
    if df is None or df.empty:
        return None
    cols_map = {c.lower(): c for c in df.columns}
    start_col = cols_map.get("start", cols_map.get("sleep_start"))
    end_col   = cols_map.get("end",   cols_map.get("sleep_end"))
    if not (start_col and end_col):
        print(f"[WARN] sleep CSV missing start/end columns (have: {list(df.columns)})")
        return None
    df, s_col = _coerce_dtcol(df, [start_col])
    df, e_col = _coerce_dtcol(df, [end_col])
    if s_col is None or e_col is None:
        return None
    df = df.rename(columns={s_col: "start", e_col: "end"})
    df = df.dropna(subset=["start", "end"])
    df = df[df["end"] > df["start"]]
    return df[["start","end"] + [c for c in df.columns if c not in ("start","end")]]

def _load_vo2_native(vo2_csv: Optional[Path]) -> Optional[pd.DataFrame]:
    df = _read_csv(vo2_csv)
    if df is None or df.empty:
        return None
    if "vo2max_est" not in df.columns:
        for alt in ["vo2max", "value"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "vo2max_est"})
                break
    if "date" not in df.columns:
        df, tcol = _coerce_dtcol(df, ["time","datetime","ts","minute"])
        if tcol is not None:
            df["date"] = pd.to_datetime(df[tcol]).dt.date.astype(str)
    if "date" in df.columns and "vo2max_est" in df.columns:
        df["date"] = df["date"].astype(str)
        return df[["date","vo2max_est"]]
    print(f"[WARN] vo2max CSV lacks (date, vo2max_est) columns")
    return None

def _load_resting_native(rest_csv: Optional[Path]) -> Optional[pd.DataFrame]:
    df = _read_csv(rest_csv)
    if df is None or df.empty:
        return None
    if "resting_bpm" not in df.columns:
        for alt in ["resting_hr","resting","value"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "resting_bpm"})
                break
    if "date" in df.columns and "resting_bpm" in df.columns:
        df["date"] = df["date"].astype(str)
        return df[["date","resting_bpm"]]
    print(f"[WARN] resting HR CSV lacks (date, resting_bpm)")
    return None

# ----------------------------- date filters -----------------------------

def _filter_hr_by_day(hr_df: pd.DataFrame, day: str) -> Optional[pd.DataFrame]:
    if hr_df is None or hr_df.empty: return None
    df = hr_df.copy()
    df["__date"] = df["time"].dt.date.astype(str)
    out = df[df["__date"] == day]
    return out[["time","bpm"]] if not out.empty else None

def _filter_steps_by_day(steps_df: pd.DataFrame, day: str) -> Optional[pd.DataFrame]:
    if steps_df is None or steps_df.empty: return None
    df = steps_df.copy()
    df["__date"] = df["time"].dt.date.astype(str)
    out = df[df["__date"] == day]
    return out[["time","steps"]] if not out.empty else None

# ----------------------------- sunrise model -----------------------------

def _sunrise_local_solar(day: str, lat_deg: float, lon_deg: float) -> Optional[pd.Timestamp]:
    """
    Approximate sunrise time (local naive) using NOAA-style equations.

    """
    try:
        date = pd.to_datetime(day).date()
    except Exception:
        return None

    if lat_deg is None or lon_deg is None or abs(lat_deg) > 90 or abs(lon_deg) > 180:
        return None

    lat = math.radians(lat_deg)
    n = pd.Timestamp(date).day_of_year

    # Fractional year (NOAA). Keep your original structure.
    gamma = 2.0 * math.pi / 365.0 * (n - 1 + (6 - 12) / 24.0)

    eq_time = 229.18 * (
        0.000075
        + 0.001868 * math.cos(gamma)
        - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2 * gamma)
        - 0.040849 * math.sin(2 * gamma)
    )

    decl = (
        0.006918
        - 0.399912 * math.cos(gamma)
        + 0.070257 * math.sin(gamma)
        - 0.006758 * math.cos(2 * gamma)
        + 0.000907 * math.sin(2 * gamma)
        - 0.002697 * math.cos(3 * gamma)
        + 0.00148 * math.sin(3 * gamma)
    )

    # Sun altitude for "official" sunrise: -0.833 degrees
    ha_arg = (math.cos(math.radians(90.833)) / (math.cos(lat) * math.cos(decl))) - math.tan(lat) * math.tan(decl)

    # No sunrise/sunset (polar day/night) or invalid
    if not np.isfinite(ha_arg) or ha_arg < -1 or ha_arg > 1:
        return None

    hour_angle_deg = math.degrees(math.acos(ha_arg))

    # Minutes from midnight (local solar time approximation)
    solar_noon_min = 720.0 - 4.0 * lon_deg - eq_time
    sunrise_min_raw = solar_noon_min - 4.0 * hour_angle_deg

    if sunrise_min_raw < -7200 or sunrise_min_raw > 7200:
        return None

    # Wrap into [0, 1440)
    sunrise_min = sunrise_min_raw % 1440.0

    # Convert to hh:mm with robust rounding
    total_minutes_int = int(round(sunrise_min))
    total_minutes_int %= 1440  # protect against rounding to 1440

    hh = total_minutes_int // 60
    mm = total_minutes_int % 60

    return pd.Timestamp(year=date.year, month=date.month, day=date.day, hour=int(hh), minute=int(mm))

# ----------------------------- metrics -----------------------------

def compute_resting_hr(hr_df: Optional[pd.DataFrame], date_list: List[str],
                       native_rest: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols = ["date","resting_bpm"]
    if native_rest is not None and not native_rest.empty:
        out = native_rest.copy()
        if date_list:
            out = out[out["date"].astype(str).isin(date_list)]
        return out[cols].sort_values("date")

    if hr_df is None or hr_df.empty or not date_list:
        return _safe(None, cols)

    rows = []
    for day in date_list:
        d = _filter_hr_by_day(hr_df, day)
        if d is None or d.empty:
            continue
        bpm = pd.to_numeric(d["bpm"], errors="coerce").dropna()
        if bpm.empty:
            continue
        rows.append({"date": day, "resting_bpm": round(float(np.percentile(bpm, 5)), 1)})
    return _safe(pd.DataFrame(rows), cols).sort_values("date")

def compute_mvpa_from_steps(steps_df: Optional[pd.DataFrame], date_list: List[str],
                            cadence_threshold: int) -> pd.DataFrame:
    cols = ["date","mvpa_min"]
    if steps_df is None or steps_df.empty or not date_list:
        return _safe(None, cols)
    rows = []
    for day in date_list:
        d = _filter_steps_by_day(steps_df, day)
        if d is None or d.empty:
            continue
        spm = pd.to_numeric(d["steps"], errors="coerce")
        mvpa_min = int((spm >= cadence_threshold).sum())
        rows.append({"date": day, "mvpa_min": mvpa_min})
    return _safe(pd.DataFrame(rows), cols).sort_values("date")

def compute_mvpa_from_hr(hr_df: Optional[pd.DataFrame], date_list: List[str],
                         resting_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["date","mvpa_min"]
    if hr_df is None or hr_df.empty or not date_list:
        return _safe(None, cols)
    rest_map = {}
    if not resting_df.empty:
        rest_map = dict(zip(resting_df["date"].astype(str), pd.to_numeric(resting_df["resting_bpm"], errors="coerce")))
    rows = []
    for day in date_list:
        d = _filter_hr_by_day(hr_df, day)
        if d is None or d.empty:
            continue
        d = d.copy()
        d["minute"] = d["time"].dt.floor("min")
        obs_max = np.nanpercentile(pd.to_numeric(d["bpm"], errors="coerce"), 95)
        hrmax_est = max(160.0, float(obs_max))
        hrrest = float(rest_map.get(day, np.nan))
        if not np.isfinite(hrrest):
            hrrest = float(np.nanpercentile(pd.to_numeric(d["bpm"], errors="coerce"), 5))
        if not np.isfinite(hrrest) or hrmax_est <= hrrest:
            continue
        bpm = pd.to_numeric(d["bpm"], errors="coerce")
        hrr = (bpm - hrrest) / (hrmax_est - hrrest)
        per_min = pd.DataFrame({"minute": d["minute"], "hrr": hrr}).groupby("minute", as_index=False)["hrr"].max()
        mvpa_min = int((per_min["hrr"] >= 0.40).sum())
        rows.append({"date": day, "mvpa_min": mvpa_min})
    return _safe(pd.DataFrame(rows), cols).sort_values("date")

def compute_vo2max(native_vo2: Optional[pd.DataFrame], hr_df: Optional[pd.DataFrame],
                   date_list: List[str], resting_df: pd.DataFrame, mvpa_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["date","vo2max_est"]
    if native_vo2 is not None and not native_vo2.empty:
        out = native_vo2.copy()
        if date_list:
            out = out[out["date"].astype(str).isin(date_list)]
        return out[cols].sort_values("date")

    if hr_df is None or hr_df.empty or not date_list:
        return _safe(None, cols)

    rest_map = dict(zip(resting_df["date"].astype(str), pd.to_numeric(resting_df["resting_bpm"], errors="coerce"))) if not resting_df.empty else {}
    mvpa_map = dict(zip(mvpa_df["date"].astype(str), pd.to_numeric(mvpa_df["mvpa_min"], errors="coerce"))) if not mvpa_df.empty else {}

    rows = []
    for day in date_list:
        d = _filter_hr_by_day(hr_df, day)
        if d is None or d.empty:
            continue
        obs_max = np.nanpercentile(pd.to_numeric(d["bpm"], errors="coerce"), 95)
        hrmax_est = max(160.0, float(obs_max))
        hrrest = float(rest_map.get(day, np.nan))
        if not np.isfinite(hrrest):
            hrrest = float(np.nanpercentile(pd.to_numeric(d["bpm"], errors="coerce"), 5))
        if not np.isfinite(hrrest) or hrmax_est <= hrrest:
            continue
        mvpa_min = float(mvpa_map.get(day, 0.0))
        intensity_factor = (hrmax_est - hrrest) / hrmax_est
        vo2 = 3.5 + 0.2 * (mvpa_min / 30.0) * (intensity_factor * 100.0)
        rows.append({"date": day, "vo2max_est": round(float(vo2), 1)})
    return _safe(pd.DataFrame(rows), cols).sort_values("date")

def compute_sleep_efficiency(sleep_df: Optional[pd.DataFrame], date_list: List[str]) -> pd.DataFrame:
    cols = ["date","efficiency_0_100","sleep_min","restless_min"]
    if sleep_df is None or sleep_df.empty:
        return _safe(None, cols)
    df = sleep_df.copy()
    df["date"] = df["start"].dt.date.astype(str)
    df["sleep_min"] = (df["end"] - df["start"]).dt.total_seconds() / 60.0
    rest_col = None
    for cand in ["restless_seconds","restless_min","restless_minutes","restless"]:
        if cand in df.columns:
            rest_col = cand
            break
    if rest_col is None:
        df["restless_min"] = 0.0
    else:
        if "seconds" in rest_col:
            df["restless_min"] = pd.to_numeric(df[rest_col], errors="coerce") / 60.0
        else:
            df["restless_min"] = pd.to_numeric(df[rest_col], errors="coerce")
    agg = df.groupby("date", as_index=False).agg({"sleep_min":"sum","restless_min":"sum"})
    agg["efficiency_0_100"] = (np.maximum(agg["sleep_min"] - agg["restless_min"], 0.0) / agg["sleep_min"]) * 100.0
    if date_list:
        agg = agg[agg["date"].isin(date_list)]
    return _safe(agg, cols).sort_values("date")

def _circ_diff_min(a: pd.Timestamp, b: pd.Timestamp) -> float:
    """Minimal difference in minutes on a 24h clock."""
    am = a.hour * 60 + a.minute + a.second / 60.0
    bm = b.hour * 60 + b.minute + b.second / 60.0
    d = abs(am - bm)
    return float(min(d, 1440.0 - d))


def compute_sleep_deviation_vs_sun(
    sleep_df: Optional[pd.DataFrame],
    date_list: List[str],
    lat: Optional[float],
    lon: Optional[float],
) -> pd.DataFrame:
    cols = ["date", "onset_dev_min", "offset_dev_min", "sunrise_local"]
    if sleep_df is None or sleep_df.empty or lat is None or lon is None:
        return _safe(None, cols)

    df = sleep_df.copy()
    df = df.dropna(subset=["start", "end"])
    df = df[df["end"] > df["start"]].copy()

    # Anchor by wake date (as before)
    df["date"] = df["end"].dt.date.astype(str)

    rows = []
    for day, g in df.groupby("date"):
        sunrise = _sunrise_local_solar(day, lat, lon)
        if sunrise is None:
            continue

        # --- pick MAIN sleep episode (avoid naps dominating max(end)) ---
        gg = g.copy()
        gg["dur_min"] = (gg["end"] - gg["start"]).dt.total_seconds() / 60.0
        gg = gg[gg["dur_min"] >= 60]  # optional: ignore tiny dozes < 60 min
        if gg.empty:
            continue

        main = gg.loc[gg["dur_min"].idxmax()]

        # --- optionally merge episodes close to main sleep (split sleep) ---
        # keep episodes that overlap or are within 2h of the main episode
        gap_limit = pd.Timedelta(hours=2)
        keep = gg[
            (gg["start"] <= main["end"] + gap_limit) &
            (gg["end"]   >= main["start"] - gap_limit)
        ].copy()

        # merge kept episodes into one window
        start_ts = keep["start"].min()
        end_ts   = keep["end"].max()

        # Ideal bedtime = 8 hours before sunrise (same concept)
        bedtime = sunrise - pd.Timedelta(hours=8)

        # Use circular deviation in MINUTES-OF-DAY (prevents midnight wrap spikes)
        onset_dev  = _circ_diff_min(start_ts, bedtime)
        offset_dev = _circ_diff_min(end_ts, sunrise)

        rows.append({
            "date": day,
            "onset_dev_min": round(onset_dev, 1),
            "offset_dev_min": round(offset_dev, 1),
            "sunrise_local": sunrise.strftime("%H:%M"),
        })

    out = pd.DataFrame(rows)
    if date_list:
        out = out[out["date"].astype(str).isin(date_list)]
    return _safe(out, cols).sort_values("date")

def fit_daily_cosinor(hr_df: Optional[pd.DataFrame], date_list: List[str]) -> pd.DataFrame:
    cols = ["date","amplitude","acrophase_h","mesor","r_squared"]
    if hr_df is None or hr_df.empty or not date_list:
        return _safe(None, cols)
    rows = []
    w = 2.0 * math.pi / 24.0
    for day in date_list:
        d = _filter_hr_by_day(hr_df, day)
        if d is None or d.empty:
            continue
        y = pd.to_numeric(d["bpm"], errors="coerce").astype(float)
        t = d["time"].dt.hour + d["time"].dt.minute/60.0 + d["time"].dt.second/3600.0
        mask = np.isfinite(y) & np.isfinite(t)
        y = y[mask].values
        t = t[mask].values
        if y.size < 12:
            continue
        cos_col = np.cos(w * t); sin_col = np.sin(w * t)
        X = np.column_stack([np.ones_like(t), cos_col, sin_col])
        beta, *_ = np.linalg.lstsq(X, y, rcond=None)
        mesor = float(beta[0]); b1, b2 = float(beta[1]), float(beta[2])
        amplitude = math.sqrt(b1*b1 + b2*b2)
        phi = math.atan2(b2, b1)
        acrophase_h = (phi / w) % 24.0
        y_hat = X @ beta
        ss_res = float(np.sum((y - y_hat)**2))
        ss_tot = float(np.sum((y - np.mean(y))**2))
        r2 = 1.0 - ss_res/ss_tot if ss_tot > 0 else np.nan
        rows.append({
            "date": day,
            "amplitude": round(amplitude, 3),
            "acrophase_h": round(acrophase_h, 2),
            "mesor": round(mesor, 2),
            "r_squared": round(r2, 3) if np.isfinite(r2) else np.nan
        })
    return _safe(pd.DataFrame(rows), cols).sort_values("date")

# ----------------------------- CLI / main -----------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Compute daily metrics from individually supplied CSVs.")
    ap.add_argument("--sleep-csv", required=True, help="sleep_episodes.csv")
    ap.add_argument("--heart-rate-csv", help="heart_rate.csv")
    ap.add_argument("--steps-csv", help="steps.csv (minute-level preferred; intervals allowed)")
    ap.add_argument("--vo2max-csv", help="vo2max.csv (native)")
    ap.add_argument("--resting-hr-csv", help="resting_hr.csv (native)")
    ap.add_argument("--out-dir", required=True, help="Directory to write metrics")
    ap.add_argument("--start", type=str, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end", type=str, help="YYYY-MM-DD inclusive")
    ap.add_argument("--lat", type=float, help="Latitude for sunrise metrics (optional)")
    ap.add_argument("--lon", type=float, help="Longitude for sunrise metrics (optional)")
    ap.add_argument("--mvpa-cadence-threshold", type=int, default=100,
                    help="Steps/min threshold for MVPA (default: 100)")
    return ap.parse_args()

def main():
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    date_list = _daterange_str(args.start, args.end)

    sleep_df = _load_sleep_df(Path(args.sleep_csv))
    hr_df    = _load_hr_df(Path(args.heart_rate_csv)) if args.heart_rate_csv else None
    steps_df = _load_steps_df(Path(args.steps_csv)) if args.steps_csv else None
    vo2_nat  = _load_vo2_native(Path(args.vo2max_csv)) if args.vo2max_csv else None
    rest_nat = _load_resting_native(Path(args.resting_hr_csv)) if args.resting_hr_csv else None

    # Resting HR
    resting = compute_resting_hr(hr_df, date_list, rest_nat)
    _write_csv(resting, out_dir / "resting_hr.csv", ["date","resting_bpm"])

    # MVPA (steps preferred, fallback HR)
    mvpa_steps = compute_mvpa_from_steps(steps_df, date_list, args.mvpa_cadence_threshold)
    if mvpa_steps.empty:
        mvpa = compute_mvpa_from_hr(hr_df, date_list, resting)
    else:
        mvpa = mvpa_steps
    _write_csv(mvpa, out_dir / "mvpa_minutes.csv", ["date","mvpa_min"])

    # VO2 (native or estimate using HR + MVPA)
    vo2 = compute_vo2max(vo2_nat, hr_df, date_list, resting, mvpa)
    _write_csv(vo2, out_dir / "vo2max.csv", ["date","vo2max_est"])

    # Sleep metrics
    eff = compute_sleep_efficiency(sleep_df, date_list)
    _write_csv(eff, out_dir / "sleep_efficiency.csv", ["date","efficiency_0_100","sleep_min","restless_min"])

    sun = compute_sleep_deviation_vs_sun(sleep_df, date_list, args.lat, args.lon)
    _write_csv(sun, out_dir / "sleep_deviation_vs_sun.csv", ["date","onset_dev_min","offset_dev_min","sunrise_local"])

    # HR cosinor
    cos = fit_daily_cosinor(hr_df, date_list)
    _write_csv(cos, out_dir / "hr_cosinor.csv", ["date","amplitude","acrophase_h","mesor","r_squared"])

    summary = {
        "resting_hr_rows": int(len(resting)) if resting is not None else 0,
        "mvpa_rows": int(len(mvpa)) if mvpa is not None else 0,
        "vo2max_rows": int(len(vo2)) if vo2 is not None else 0,
        "sleep_efficiency_rows": int(len(eff)) if eff is not None else 0,
        "sun_deviation_rows": int(len(sun)) if sun is not None else 0,
        "hr_cosinor_rows": int(len(cos)) if cos is not None else 0,
    }
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print("[metrics] summary:", json.dumps(summary, indent=2))
    print("\n[METRICS DONE]")

if __name__ == "__main__":
    main()