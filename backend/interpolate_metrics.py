#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
interpolate_metrics.py

Build minute-level series and tidy, daily outputs from the processed CSVs
(agnostic to Apple/Google sources). No DBs involved—this script ONLY reads
the standardized CSVs produced by *raw* exporters.

INPUTS (in --in-dir)
--------------------
Expected (all optional, but some features depend on having them):
  • heart_rate.csv           user,datetime,bpm
  • hrv.csv                  user,datetime,rmssd_ms
  • body_temperature.csv     user,datetime,temperature_c
  • steps.csv                EITHER:
        (interval) user,start_datetime,end_datetime,steps
     OR (points)   user,datetime,steps
  • sleep_episodes.csv       user,start,end,sleep_id,restless_seconds
  • sleep_stages.csv         user,start,end,stage
  • vo2max.csv               user,datetime,vo2max

OUTPUTS (in --out-dir)
----------------------
  • heart_rate_minute.csv        user,minute,bpm
  • hrv_minute.csv               user,minute,rmssd_ms
  • body_temperature_minute.csv  user,minute,temperature_c
  • steps_minute.csv             user,minute,steps
  • sleep_episodes_merged.csv    user,start,end,sleep_id,restless_seconds
  • sleep_stages_merged.csv      user,start,end,stage
  • vo2max_daily.csv             user,date,vo2max

BEHAVIOR / RULES
----------------
A) Duplicate timestamp aggregation (pre-interpolation)
   - For metrics with an *instant* timestamp (HR, HRV, temperature):
     If multiple samples land on the same “dedup time bucket”, aggregate them
     by one of: mean | median | max | min. The dedup bucket is configurable:
     none | second | minute. Default is 'minute'.
   - NOT applied to any interval metric (e.g., steps intervals).

B) Minute-level interpolation (HR, HRV, body temp)
   - Build a per-minute index from the first to last sample (bounded by
     optional --start/--end).
   - Supported: linear | polynomial | newtons | neighbor | pchip | cubic
     • linear / neighbor → pure pandas/numpy (no SciPy).
     • polynomial/newtons → global least-squares polynomial of degree --poly-degree
       (default 3). ("newtons" is an alias using the same solver, but logged as
       Newton-form interpolation for traceability.)
     • pchip / cubic → require SciPy. If SciPy is unavailable and request
       one of these, fallback to linear (unless --strict-scipy is set).
   - Interpolation is done on wall-clock (converted to seconds). Extrapolation is
     disabled; endpoints are forward/backward filled ONLY if --edge-fill is given.

C) Sleep merging
   - Episodes: merge adjacent episodes when the gap between them is strictly
     less than --sleep-merge-threshold-mins. The in-between gap (awake time)
     is added to restless_seconds of the merged episode. sleep_id is renumbered.
   - Stages: merge only *consecutive* segments with the same `stage` when:
       • The gap between them is strictly less than --sleep-merge-threshold-mins
       • There are no other records between them (i.e., they are neighbors)
     Rapid sequences like light→deep→light will NOT merge the two light segments.

D) Steps minute series (overlap-aware, cap for *distributed* steps only)
   - Accepts either:
       (1) intervals: start_datetime, end_datetime, steps
       (2) points:    datetime, steps   (already minute-accurate device events)
   - For intervals:
       • First, aggregate duplicates where (start,end) are identical using
         --steps-dupe-agg: mean | max | min (default: mean). (Values are rounded.)
       • Resolve overlaps by *priority to the shorter interval*. Process intervals
         from shortest duration to longest:
           - When placing steps for a longer interval, subtract any steps already
             allocated by shorter intervals on the overlapped minutes and DO NOT
             allocate to those minutes again. The remaining steps are spread
             evenly by overlap seconds across the remaining minutes in that
             longer interval.
       • Optional cap: --steps-max-per-minute N applies only to steps that were
         *distributed* from intervals. Direct point steps (device-recorded) are
         NOT capped. The final minute value is:
               steps = direct_steps + min(distributed_steps, cap)   (if cap set)
   - Optional sleep-assisted mode (flag: --steps-sleep-assisted):
       • When distributing *remaining* steps from long intervals, minutes that
         fall inside any episode in sleep_episodes.csv are treated as forbidden
         and receive no distributed steps. (Device point steps remain untouched.)

E) VO2 max daily
   - Exactly one row per calendar day in the chosen window:
       • If vo2max csv has a value that day, keep the *max* of that day.
       • If not, estimate using the day’s HR data:
             VO2max ≈ 15 * (MHR / RHR)
         where MHR = max heart rate that day, RHR = min heart rate that day.
         If no HR is available for that day, that day remains missing.
   - Output column 'date' is YYYY-MM-DD (local, naive).

FLAGS
-----
  --in-dir                Folder with the processed CSVs (from raw exporters)
  --out-dir               Destination folder
  --start / --end         Optional date bounds (YYYY-MM-DD), inclusive
  --dedup-round           none|second|minute      (default: minute)
  --dedup-agg             mean|median|max|min     (default: mean)

  --hr-interp             linear|polynomial|newtons|neighbor|pchip|cubic (default: linear)
  --hrv-interp            same choices (default: linear)
  --temp-interp           same choices (default: linear)
  --poly-degree           Degree for polynomial/newtons (default: 3)
  --edge-fill             Fill edges after interpolation: none|ffill|bfill|both (default: none)
  --strict-scipy          If set and SciPy not present for pchip/cubic → error

  --sleep-merge-threshold-mins   int minutes (default: 10)

  --steps-max-per-minute  Optional cap for *distributed* steps (int)
  --steps-dupe-agg        mean|max|min (default: mean)
  --steps-sleep-assisted  If set, do not distribute interval steps into minutes
                          that fall inside any sleep episode in sleep_episodes.csv.

NOTES
-----
- All times are treated as local-naive wall clock strings from raw stage.
- For interpolation, DO NOT extrapolate beyond first/last sample unless
  request --edge-fill (limited to end caps).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List

import numpy as np
import pandas as pd

# Optional SciPy methods
_have_scipy = False
try:
    from scipy.interpolate import PchipInterpolator, CubicSpline  # type: ignore
    _have_scipy = True
except Exception:
    _have_scipy = False


# --------------- small io helpers ---------------

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def _write_csv(df: pd.DataFrame, path: Path):
    _ensure_dir(path.parent)
    df.to_csv(path, index=False)
    print(f"[write] {path}  rows={len(df)}")


# --------------- time parsing / windows ---------------

def _to_dt(s):
    return pd.to_datetime(s, errors="coerce", utc=False)

def _clip_window(df: pd.DataFrame, col: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if start:
        out = out[_to_dt(out[col]) >= pd.to_datetime(start)]
    if end:
        out = out[_to_dt(out[col]) < (pd.to_datetime(end) + pd.Timedelta(days=1))]
    return out

def _clip_window_interval(df: pd.DataFrame, start_col: str, end_col: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if start:
        out = out[_to_dt(out[end_col]) >= pd.to_datetime(start)]
    if end:
        out = out[_to_dt(out[start_col]) < (pd.to_datetime(end) + pd.Timedelta(days=1))]
    return out

def _format_minute(ts: pd.Series) -> pd.Series:
    return pd.to_datetime(ts, errors="coerce").dt.floor("min").dt.strftime("%Y-%m-%d %H:%M")

def _format_iso_sec(ts: pd.Series) -> pd.Series:
    return pd.to_datetime(ts, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")


# --------------- duplicate aggregation ---------------

def _dedup_points(df: pd.DataFrame, time_col: str, value_col: str,
                  round_mode: str, agg: str) -> pd.DataFrame:
    """
    Round timestamps into buckets and aggregate duplicates.
    round_mode: none|second|minute
    agg: mean|median|max|min
    """
    if df is None or df.empty:
        return df

    d = df.copy()
    dt = _to_dt(d[time_col])
    if round_mode == "none":
        key = dt
    elif round_mode == "second":
        key = dt.dt.floor("S")
    else:
        key = dt.dt.floor("min")

    d["_bucket"] = key
    aggfn = {"mean": "mean", "median": "median", "max": "max", "min": "min"}[agg]
    if "user" in d.columns and d["user"].nunique() > 1:
        g = d.groupby(["user","_bucket"], as_index=False)[value_col].agg(aggfn)
        g.rename(columns={"_bucket": time_col}, inplace=True)
    else:
        g = d.groupby("_bucket", as_index=False)[value_col].agg(aggfn)
        g.rename(columns={"_bucket": time_col}, inplace=True)
        if "user" in d.columns:
            g.insert(0, "user", d["user"].iloc[0])
    return g


# --------------- interpolation core ---------------

@dataclass
class InterpSpec:
    method: str                 # linear|polynomial|newtons|neighbor|pchip|cubic
    poly_degree: int = 3
    edge_fill: str = "none"     # none|ffill|bfill|both
    strict_scipy: bool = False

def _interp_series_minutely(df: pd.DataFrame, time_col: str, value_col: str,
                            spec: InterpSpec,
                            start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    """
    Build dense per-minute series with chosen interpolation.
    No extrapolation beyond endpoints unless edge_fill requests it.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["minute", value_col])

    d = df.copy()
    d[time_col] = _to_dt(d[time_col])
    d = d.dropna(subset=[time_col, value_col]).sort_values(time_col)

    # Clip window (after parse)
    if start:
        d = d[d[time_col] >= pd.to_datetime(start)]
    if end:
        d = d[d[time_col] < (pd.to_datetime(end) + pd.Timedelta(days=1))]
    if d.empty:
        return pd.DataFrame(columns=["minute", value_col])

    # Build minute index
    full_idx = pd.date_range(d[time_col].min().floor("min"),
                             d[time_col].max().ceil("min"),
                             freq="min")
    s = d.set_index(time_col)[value_col].astype(float)
    s = s.groupby(level=0).mean()  # collapse duplicates

    s_full = s.reindex(full_idx)
    method = spec.method.lower()

    if method in ("linear", "neighbor"):
        kind = "time" if method == "linear" else "nearest"
        out = s_full.interpolate(method=kind, limit_direction="both")
    elif method in ("polynomial", "newtons"):
        t0 = full_idx[0]
        x_full = (full_idx - t0).total_seconds().astype(float)
        x = (s.index - t0).total_seconds().astype(float)
        y = s.values
        deg = max(1, int(spec.poly_degree))
        deg = min(deg, max(1, len(x) - 1))
        try:
            coeff = np.polyfit(x, y, deg=deg)
            yhat = np.polyval(coeff, x_full)
            out = pd.Series(yhat, index=full_idx, dtype=float)
            out.loc[full_idx < s.index.min()] = np.nan
            out.loc[full_idx > s.index.max()] = np.nan
        except Exception:
            out = s_full.interpolate(method="time", limit_direction="both")
    elif method in ("pchip", "cubic"):
        if not _have_scipy:
            if spec.strict_scipy:
                raise RuntimeError(f"SciPy not available for '{method}' interpolation.")
            out = s_full.interpolate(method="time", limit_direction="both")
        else:
            t0 = full_idx[0]
            x_full = (full_idx - t0).total_seconds().astype(float)
            x = (s.index - t0).total_seconds().astype(float)
            y = s.values.astype(float)
            try:
                if method == "pchip":
                    f = PchipInterpolator(x, y, extrapolate=False)
                else:
                    f = CubicSpline(x, y, extrapolate=False)
                yhat = f(x_full)
                out = pd.Series(yhat, index=full_idx, dtype=float)
            except Exception:
                out = s_full.interpolate(method="time", limit_direction="both")
    else:
        out = s_full.interpolate(method="time", limit_direction="both")

    # Edge fill policy
    if spec.edge_fill in ("ffill", "both"):
        out = out.ffill()
    if spec.edge_fill in ("bfill", "both"):
        out = out.bfill()

    return (out.rename(value_col)
               .reset_index()
               .rename(columns={"index": "minute"}))


# --------------- sleep merging ---------------

def _merge_sleep_episodes(df: pd.DataFrame, threshold_min: int) -> pd.DataFrame:
    """
    Merge sleep episodes more permissively:
      - merge if gap <= threshold (so gap == threshold merges)
      - merge if episodes overlap (negative gap allowed)
      - merge through short records in between by treating everything as a single chain
        as long as each next episode begins no later than (current_end + threshold)
    Restless seconds:
      - sum restless_seconds across episodes
      - add *positive* awake gaps between episodes to restless_seconds
      - overlaps do not subtract anything (no negative restless)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["user", "start", "end", "sleep_id", "restless_seconds"])

    d = df.copy()
    d["start"] = _to_dt(d["start"])
    d["end"] = _to_dt(d["end"])
    d = d.dropna(subset=["start", "end"]).sort_values("start")

    if d.empty:
        return pd.DataFrame(columns=["user", "start", "end", "sleep_id", "restless_seconds"])

    thr = pd.Timedelta(minutes=int(threshold_min))
    rows = []
    sid = 1

    # helper for safe restless extraction
    def _rest(row) -> float:
        v = row.get("restless_seconds", 0)
        try:
            return float(v) if v is not None and v != "" else 0.0
        except Exception:
            return 0.0

    i = 0
    while i < len(d):
        user = d.iloc[i]["user"] if "user" in d.columns else "USER"

        cur_start = d.iloc[i]["start"]
        cur_end = d.iloc[i]["end"]
        total_restless = _rest(d.iloc[i])

        j = i + 1

        while j < len(d):
            s1 = d.iloc[j]["start"]
            e1 = d.iloc[j]["end"]

            # how far is next start from the current merged end?
            gap = s1 - cur_end

            # MERGE CONDITION:
            # - if overlap: gap < 0  -> merge
            # - if close:   gap <= thr -> merge (includes equality)
            if gap <= thr:
                # add next episode's restless
                total_restless += _rest(d.iloc[j])

                # add only positive awake gap time (overlaps don't add negative time)
                if gap > pd.Timedelta(0):
                    total_restless += gap.total_seconds()

                # extend merged end
                if e1 > cur_end:
                    cur_end = e1

                j += 1
            else:
                break

        rows.append({
            "user": user,
            "start": cur_start.strftime("%Y-%m-%d %H:%M:%S"),
            "end": cur_end.strftime("%Y-%m-%d %H:%M:%S"),
            "sleep_id": sid,
            "restless_seconds": int(round(total_restless))
        })
        sid += 1
        i = j

    return pd.DataFrame(rows, columns=["user", "start", "end", "sleep_id", "restless_seconds"])

def _merge_sleep_stages(df: pd.DataFrame, threshold_min: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["user","start","end","stage"])
    d = df.copy()
    d["start"] = _to_dt(d["start"])
    d["end"]   = _to_dt(d["end"])
    d = d.dropna(subset=["start","end","stage"]).sort_values("start")

    thr = pd.Timedelta(minutes=int(threshold_min))
    out = []
    i = 0
    while i < len(d):
        s0 = d.iloc[i]["start"]
        e0 = d.iloc[i]["end"]
        stg0 = str(d.iloc[i]["stage"]).strip().lower()
        j = i + 1
        cur_end = e0

        while j < len(d):
            s1 = d.iloc[j]["start"]
            e1 = d.iloc[j]["end"]
            stg1 = str(d.iloc[j]["stage"]).strip().lower()
            if stg1 == stg0 and (s1 - cur_end) < thr and (s1 - cur_end) >= pd.Timedelta(0):
                cur_end = max(cur_end, e1)
                j += 1
            else:
                break

        out.append({
            "user": d.iloc[i]["user"] if "user" in d.columns else "USER",
            "start": s0.strftime("%Y-%m-%d %H:%M:%S"),
            "end":   cur_end.strftime("%Y-%m-%d %H:%M:%S"),
            "stage": stg0,
        })
        i = j

    return pd.DataFrame(out, columns=["user","start","end","stage"])


# --------------- steps helpers ---------------

def _distribute_interval_to_minutes(st: pd.Timestamp, et: pd.Timestamp) -> pd.DatetimeIndex:
    """Return minute bins [st_floor, et_floor) respecting tight end."""
    st0 = pd.Timestamp(st).floor("min")
    et0 = pd.Timestamp(et).floor("min")
    if et <= st:
        return pd.DatetimeIndex([])
    rng = pd.date_range(st0, et0, freq="min")
    if et == et0 and len(rng) > 0:
        rng = rng[:-1]
    return rng

def _sleep_mask_for_minutes(full_idx: pd.DatetimeIndex, sleep_episodes: Optional[pd.DataFrame],
                            start: Optional[str], end: Optional[str]) -> pd.Series:
    """
    Build a boolean Series indexed by full_idx where True marks minutes that
    fall inside any sleep episode in sleep_episodes.csv (original, unmerged).
    """
    mask = pd.Series(False, index=full_idx)
    if sleep_episodes is None or sleep_episodes.empty:
        return mask

    s = sleep_episodes.copy()
    if "start" not in s.columns or "end" not in s.columns:
        return mask
    s["start"] = _to_dt(s["start"])
    s["end"]   = _to_dt(s["end"])
    s = s.dropna(subset=["start","end"])
    if start:
        s = s[s["end"] >= pd.to_datetime(start)]
    if end:
        s = s[s["start"] < (pd.to_datetime(end) + pd.Timedelta(days=1))]
    if s.empty:
        return mask

    for _, row in s.iterrows():
        st = pd.Timestamp(row["start"]).floor("min")
        et = pd.Timestamp(row["end"]).floor("min")
        if et <= full_idx[0] or st >= full_idx[-1] + pd.Timedelta(minutes=1):
            continue
        # Mark [st, et) as asleep
        rng = pd.date_range(max(st, full_idx[0]),
                            min(et, full_idx[-1] + pd.Timedelta(minutes=1)),
                            freq="min", inclusive="left")
        rng = rng.intersection(full_idx)
        if len(rng) > 0:
            mask.loc[rng] = True
    return mask

def _build_steps_minute(df_steps: Optional[pd.DataFrame],
                        sleep_episodes_df: Optional[pd.DataFrame],
                        start: Optional[str], end: Optional[str],
                        steps_dupe_agg: str,
                        steps_cap: Optional[int],
                        sleep_assisted: bool) -> pd.DataFrame:
    """
    Combine interval and point step sources into a per-minute series.
    Overlaps resolved by shortest-interval-first strategy (subtract already placed steps).
    Cap applies ONLY to distributed steps. If sleep_assisted=True, distributed
    steps avoid minutes inside sleep episodes.
    """
    if df_steps is None or df_steps.empty:
        return pd.DataFrame(columns=["minute","steps"])

    d = df_steps.copy()

    # Identify schema
    has_interval = {"start_datetime","end_datetime","steps"}.issubset(set(d.columns))
    has_points   = {"datetime","steps"}.issubset(set(d.columns))

    # Parse & clip
    if has_interval:
        d["start_datetime"] = _to_dt(d["start_datetime"])
        d["end_datetime"]   = _to_dt(d["end_datetime"])
        d = d.dropna(subset=["start_datetime","end_datetime","steps"])
        d = _clip_window_interval(d, "start_datetime", "end_datetime", start, end)
    if has_points:
        dp = d[["user","datetime","steps"]].dropna().copy()
        dp["datetime"] = _to_dt(dp["datetime"])
        dp = _clip_window(dp, "datetime", start, end)
    else:
        dp = pd.DataFrame(columns=["user","datetime","steps"])

    # Build minute index spanning all step data
    mins = []
    if has_interval and not d.empty:
        mins += [d["start_datetime"].min(), d["end_datetime"].max()]
    if has_points and not dp.empty:
        mins += [dp["datetime"].min(), dp["datetime"].max()]
    if not mins:
        return pd.DataFrame(columns=["minute","steps"])

    min_start = pd.Timestamp(min(mins)).floor("min")
    min_end   = pd.Timestamp(max(mins)).floor("min")
    full_idx  = pd.date_range(min_start, min_end, freq="min")

    # Direct device minutes (points) — unaffected by cap and sleep-mask
    direct = pd.Series(0.0, index=full_idx)
    if has_points and not dp.empty:
        dp["minute"] = dp["datetime"].dt.floor("min")
        direct = direct.add(dp.groupby("minute")["steps"].sum(), fill_value=0.0)

    # Interval distribution (subject to cap and sleep-avoid)
    distributed = pd.Series(0.0, index=full_idx)

    # Sleep mask (True = asleep, forbidden for distribution)
    sleep_mask = _sleep_mask_for_minutes(full_idx, sleep_episodes_df, start, end) if sleep_assisted else pd.Series(False, index=full_idx)

    if has_interval and not d.empty:
        # Aggregate exact duplicate intervals
        if steps_dupe_agg == "mean":
            d = (d.groupby(["start_datetime","end_datetime"], as_index=False)["steps"]
                   .mean())
        elif steps_dupe_agg == "max":
            d = (d.groupby(["start_datetime","end_datetime"], as_index=False)["steps"]
                   .max())
        else:  # min
            d = (d.groupby(["start_datetime","end_datetime"], as_index=False)["steps"]
                   .min())
        d["steps"] = d["steps"].round().astype(int)

        # Sort by interval duration ascending (priority to shorter)
        d["dur_s"] = (_to_dt(d["end_datetime"]) - _to_dt(d["start_datetime"])).dt.total_seconds()
        d = d.sort_values("dur_s")

        # Already-placed minute steps tracker (from shorter intervals)
        placed = pd.Series(0.0, index=full_idx)

        for _, row in d.iterrows():
            st = pd.Timestamp(row["start_datetime"])
            et = pd.Timestamp(row["end_datetime"])
            total = float(row["steps"])
            if et <= st or total <= 0:
                continue

            rng = _distribute_interval_to_minutes(st, et)
            if len(rng) == 0:
                # within a single minute; treat as distributed (still subject to cap)
                k = st.floor("min")
                if not sleep_mask.loc[k]:  # allow if not asleep minute
                    distributed.loc[k] = distributed.get(k, 0.0) + total
                    placed.loc[k]      = placed.get(k, 0.0) + total
                continue

            # Exclude minutes already filled by shorter intervals
            free_rng = rng[placed.loc[rng] <= 0]
            # Exclude sleep minutes if sleep-assisted
            if sleep_assisted:
                free_rng = free_rng[~sleep_mask.loc[free_rng].values]

            overlapped_minutes = rng.difference(free_rng)
            subtract_already = placed.loc[overlapped_minutes].sum()
            remaining = max(0.0, total - subtract_already)

            if len(free_rng) == 0 or remaining <= 0:
                continue

            # Distribute evenly by overlap seconds across free_rng
            sec_weights = {}
            prev = st
            for m in free_rng:
                seg_end = min((m + pd.Timedelta(minutes=1)), et)
                if seg_end > prev:
                    sec_weights[m] = (seg_end - prev).total_seconds()
                    prev = seg_end
            total_sec = sum(sec_weights.values())
            if total_sec <= 0:
                continue
            for m, sec in sec_weights.items():
                add = remaining * (sec / total_sec)
                distributed.loc[m] = distributed.get(m, 0.0) + add
                placed.loc[m]      = placed.get(m, 0.0) + add

    # Apply cap only to the distributed component
    if steps_cap is not None:
        distributed = distributed.clip(upper=float(steps_cap))

    steps = (direct + distributed).fillna(0.0)
    out = steps.rename("steps").reset_index().rename(columns={"index": "minute"})
    out["steps"] = np.floor(out["steps"] + 0.5).astype(int)
    return out[["minute","steps"]]


# --------------- VO2max daily ---------------

def _vo2max_daily(vo2: Optional[pd.DataFrame],
                  hr_points: Optional[pd.DataFrame],
                  start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    """
    Ensure exactly one row per date.
      - If vo2max.csv has readings per day, keep the DAILY MAX.
      - Else, estimate via 15 * (MHR/RHR) using heart_rate.csv of that day.
    """
    dates = None
    if start and end:
        dates = pd.date_range(pd.to_datetime(start).date(),
                              pd.to_datetime(end).date(), freq="D")
    else:
        cands = []
        if vo2 is not None and not vo2.empty:
            cands += [_to_dt(vo2["datetime"]).min().date(),
                      _to_dt(vo2["datetime"]).max().date()]
        if hr_points is not None and not hr_points.empty:
            cands += [_to_dt(hr_points["datetime"]).min().date(),
                      _to_dt(hr_points["datetime"]).max().date()]
        if not cands:
            return pd.DataFrame(columns=["user","date","vo2max"])
        dates = pd.date_range(min(cands), max(cands), freq="D")

    vo2_daily = {}
    user_val = None

    if vo2 is not None and not vo2.empty:
        d = vo2.copy()
        d["datetime"] = _to_dt(d["datetime"])
        d = d.dropna(subset=["datetime","vo2max"])
        if start:
            d = d[d["datetime"] >= pd.to_datetime(start)]
        if end:
            d = d[d["datetime"] < (pd.to_datetime(end) + pd.Timedelta(days=1))]
        if not d.empty:
            if "user" in d.columns:
                user_val = d["user"].iloc[0]
            d["date"] = d["datetime"].dt.date
            daily = d.groupby("date", as_index=False)["vo2max"].max()
            vo2_daily = dict(zip(daily["date"], daily["vo2max"]))

    hr_daily = {}
    if hr_points is not None and not hr_points.empty:
        h = hr_points.copy()
        h["datetime"] = _to_dt(h["datetime"])
        h = h.dropna(subset=["datetime","bpm"])
        if start:
            h = h[h["datetime"] >= pd.to_datetime(start)]
        if end:
            h = h[h["datetime"] < (pd.to_datetime(end) + pd.Timedelta(days=1))]
        if not h.empty:
            if user_val is None and "user" in h.columns:
                user_val = h["user"].iloc[0]
            h["date"] = h["datetime"].dt.date
            agg = h.groupby("date").agg(MHR=("bpm","max"), RHR=("bpm","min")).reset_index()
            hr_daily = {row["date"]: (row["MHR"], row["RHR"]) for _, row in agg.iterrows()}

    rows = []
    for d in dates:
        date_key = d.date()
        if date_key in vo2_daily:
            rows.append({"user": user_val or "USER", "date": str(date_key), "vo2max": float(vo2_daily[date_key])})
        else:
            if date_key in hr_daily:
                MHR, RHR = hr_daily[date_key]
                if RHR and RHR > 0:
                    vo2_est = 15.0 * (float(MHR) / float(RHR))
                    rows.append({"user": user_val or "USER", "date": str(date_key), "vo2max": vo2_est})

    return pd.DataFrame(rows, columns=["user","date","vo2max"])


# --------------- CLI & main ---------------

def parse_args() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Interpolate + harmonize processed CSVs to minute-level and daily outputs.")
    ap.add_argument("--in-dir", required=True, help="Folder containing processed CSVs")
    ap.add_argument("--out-dir", required=True, help="Destination folder for outputs")
    ap.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD), inclusive")
    ap.add_argument("--end",   type=str, default=None, help="End date (YYYY-MM-DD), inclusive")

    ap.add_argument("--dedup-round", choices=["none","second","minute"], default="minute",
                    help="Rounding bucket for duplicate timestamps (instant metrics).")
    ap.add_argument("--dedup-agg", choices=["mean","median","max","min"], default="mean",
                    help="Aggregation for duplicates within the same bucket.")

    for m in ["hr","hrv","temp"]:
        ap.add_argument(f"--{m}-interp",
                        choices=["linear","polynomial","newtons","neighbor","pchip","cubic"],
                        default="linear",
                        help=f"Interpolation method for {m.upper()} minute series.")
    ap.add_argument("--poly-degree", type=int, default=3, help="Degree for polynomial/newtons.")
    ap.add_argument("--edge-fill", choices=["none","ffill","bfill","both"], default="none",
                    help="Optionally fill edges after interpolation.")
    ap.add_argument("--strict-scipy", action="store_true",
                    help="If true, require SciPy for pchip/cubic; else fallback to linear.")

    ap.add_argument("--sleep-merge-threshold-mins", type=int, default=60,
                    help="Gap threshold for merging sleep episodes and same-stage neighbors.")

    ap.add_argument("--steps-max-per-minute", type=int, default=None,
                    help="Cap for per-minute steps applied ONLY to distributed steps (not device points).")
    ap.add_argument("--steps-dupe-agg", choices=["mean","max","min"], default="mean",
                    help="Aggregate value when intervals have identical (start,end).")
    ap.add_argument("--steps-sleep-assisted", action="store_true",
                    help="If set, DO NOT distribute interval steps into minutes that fall inside any sleep episode.")

    return ap

def main():
    args = parse_args().parse_args()
    in_dir  = Path(args.in_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    _ensure_dir(out_dir)

    # --- Load inputs (any missing are treated as absent) ---
    hr  = _read_csv(in_dir / "heart_rate.csv")
    hrv = _read_csv(in_dir / "hrv.csv")
    tmp = _read_csv(in_dir / "body_temperature.csv")
    stp = _read_csv(in_dir / "steps.csv")
    slp_e = _read_csv(in_dir / "sleep_episodes.csv")
    slp_s = _read_csv(in_dir / "sleep_stages.csv")
    vo2 = _read_csv(in_dir / "vo2max.csv")

    # --- Dedup & interpolate: HR, HRV, TEMP ---
    # HR
    if hr is not None and not hr.empty:
        hr = _clip_window(hr, "datetime", args.start, args.end)
        if not hr.empty:
            user_hr = hr["user"].iloc[0] if "user" in hr.columns else "USER"
            hr_dedup = _dedup_points(hr[["user","datetime","bpm"]], "datetime", "bpm",
                                     args.dedup_round, args.dedup_agg)
            hr_spec = InterpSpec(args.hr_interp, args.poly_degree, args.edge_fill, args.strict_scipy)
            hr_min = _interp_series_minutely(hr_dedup, "datetime", "bpm", hr_spec, args.start, args.end)
            if not hr_min.empty:
                hr_min.insert(0, "user", user_hr)
                hr_min["minute"] = _format_minute(hr_min["minute"])
                _write_csv(hr_min[["user","minute","bpm"]], out_dir / "heart_rate_minute.csv")
            else:
                _write_csv(pd.DataFrame(columns=["user","minute","bpm"]), out_dir / "heart_rate_minute.csv")
        else:
            _write_csv(pd.DataFrame(columns=["user","minute","bpm"]), out_dir / "heart_rate_minute.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","minute","bpm"]), out_dir / "heart_rate_minute.csv")

    # HRV
    if hrv is not None and not hrv.empty:
        hrv = _clip_window(hrv, "datetime", args.start, args.end)
        if not hrv.empty:
            user_hrv = hrv["user"].iloc[0] if "user" in hrv.columns else "USER"
            hrv_dedup = _dedup_points(hrv[["user","datetime","rmssd_ms"]], "datetime", "rmssd_ms",
                                      args.dedup_round, args.dedup_agg)
            hrv_spec = InterpSpec(args.hrv_interp, args.poly_degree, args.edge_fill, args.strict_scipy)
            hrv_min = _interp_series_minutely(hrv_dedup, "datetime", "rmssd_ms", hrv_spec, args.start, args.end)
            if not hrv_min.empty:
                hrv_min.insert(0, "user", user_hrv)
                hrv_min["minute"] = _format_minute(hrv_min["minute"])
                _write_csv(hrv_min[["user","minute","rmssd_ms"]], out_dir / "hrv_minute.csv")
            else:
                _write_csv(pd.DataFrame(columns=["user","minute","rmssd_ms"]), out_dir / "hrv_minute.csv")
        else:
            _write_csv(pd.DataFrame(columns=["user","minute","rmssd_ms"]), out_dir / "hrv_minute.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","minute","rmssd_ms"]), out_dir / "hrv_minute.csv")

    # Temperature
    if tmp is not None and not tmp.empty:
        tmp = _clip_window(tmp, "datetime", args.start, args.end)
        if not tmp.empty:
            user_tmp = tmp["user"].iloc[0] if "user" in tmp.columns else "USER"
            tmp_dedup = _dedup_points(tmp[["user","datetime","temperature_c"]], "datetime", "temperature_c",
                                      args.dedup_round, args.dedup_agg)
            tmp_spec = InterpSpec(args.temp_interp, args.poly_degree, args.edge_fill, args.strict_scipy)
            tmp_min = _interp_series_minutely(tmp_dedup, "datetime", "temperature_c", tmp_spec, args.start, args.end)
            if not tmp_min.empty:
                tmp_min.insert(0, "user", user_tmp)
                tmp_min["minute"] = _format_minute(tmp_min["minute"])
                _write_csv(tmp_min[["user","minute","temperature_c"]], out_dir / "body_temperature_minute.csv")
            else:
                _write_csv(pd.DataFrame(columns=["user","minute","temperature_c"]), out_dir / "body_temperature_minute.csv")
        else:
            _write_csv(pd.DataFrame(columns=["user","minute","temperature_c"]), out_dir / "body_temperature_minute.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","minute","temperature_c"]), out_dir / "body_temperature_minute.csv")

    # --- Sleep merge (write merged variants for downstream convenience) ---
    if slp_e is not None and not slp_e.empty:
        slp_e_clip = _clip_window_interval(slp_e, "start", "end", args.start, args.end)
        ep_merged = _merge_sleep_episodes(slp_e_clip, args.sleep_merge_threshold_mins)
        _write_csv(ep_merged, out_dir / "sleep_episodes_merged.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","start","end","sleep_id","restless_seconds"]),
                   out_dir / "sleep_episodes_merged.csv")

    if slp_s is not None and not slp_s.empty:
        slp_s_clip = _clip_window_interval(slp_s, "start", "end", args.start, args.end)
        stg_merged = _merge_sleep_stages(slp_s_clip, args.sleep_merge_threshold_mins)
        _write_csv(stg_merged, out_dir / "sleep_stages_merged.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","start","end","stage"]), out_dir / "sleep_stages_merged.csv")

    # --- Steps per-minute (sleep-assisted distribution optional) ---
    steps_min = _build_steps_minute(
        stp, slp_e, args.start, args.end,
        args.steps_dupe_agg, args.steps_max_per_minute,
        args.steps_sleep_assisted
    )
    if not steps_min.empty:
        user_steps = None
        if stp is not None and not stp.empty and "user" in stp.columns:
            user_steps = stp["user"].iloc[0]
        steps_min.insert(0, "user", user_steps if user_steps else "USER")
        steps_min["minute"] = _format_minute(steps_min["minute"])
        _write_csv(steps_min[["user","minute","steps"]], out_dir / "steps_minute.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","minute","steps"]), out_dir / "steps_minute.csv")

    # --- VO2 max daily ---
    vo2_daily = _vo2max_daily(vo2, hr, args.start, args.end)
    if not vo2_daily.empty:
        out = vo2_daily[["user", "date", "vo2max"]].copy()
        # ensure numeric, then round/cast only the vo2max column
        out["vo2max"] = pd.to_numeric(out["vo2max"], errors="coerce").round().astype("Int64")
        out["user"] = out["user"].astype(str)
        out["date"] = out["date"].astype(str)
        _write_csv(out, out_dir / "vo2max_daily.csv")
    else:
        _write_csv(pd.DataFrame(columns=["user","date","vo2max"]), out_dir / "vo2max_daily.csv")

    print("\n[OK] Interpolation + aggregation complete.")
    print(f"Output dir: {out_dir}")

if __name__ == "__main__":
    main()