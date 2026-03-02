#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
blaupunkt_export_raw.py

Export vendor-specific "Blaupunkt" SQLite DB (.db) to the same *raw CSV style*
used by the Apple/Google exporters.

Implements:
  • HRV from table "SchedualHRV"
      - columns: date (TEXT, e.g. "2025-08-17"), HRV (varchar with 48 comma-separated values)
      - interpretation: 48 values correspond to 00:00, 00:30, ..., 23:30 of that day
      - output: hrv.csv  columns: user,datetime,rmssd_ms

  • SpO2 from table "BloodOxygen"
      - columns: TimeInterval (integer epoch timestamp), soa2 (double)
      - ignore soa2 == 0 (and NULL)
      - output: blood_oxygen.csv  columns: user,datetime,spo2

  • Steps from table "step"
      - columns: start_time (VARCHAR(30) with date+time), count (INTEGER step count for that interval)
      - output: steps.csv columns: user,start_datetime,end_datetime,steps
        (end_datetime is derived as start_datetime + --steps-interval-min minutes; default 60)

Timestamps:
  - SchedualHRV: built from date + fixed 30-min slots
  - BloodOxygen: epoch auto-detected (sec/ms/us/ns) and converted to naive datetime.
    Optionally add a fixed offset in seconds via --zone-offset-seconds (e.g., 3600 for UTC+1).
  - step.start_time: parsed from common string formats (ISO-like and a few vendor-ish ones).

Usage:
  python blaupunkt_export_raw.py --db qifit_default.db --out-dir out --user-id USER123 \
      --start 2025-08-01 --end 2025-09-01
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence, Any, List, Tuple

import pandas as pd

EPOCH0 = datetime(1970, 1, 1, tzinfo=timezone.utc)

# ------------------------ small helpers ------------------------

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _write_csv(df: pd.DataFrame, out_path: Path, sort_col: Optional[str] = None) -> None:
    _ensure_dir(out_path.parent)
    d = df.copy()
    if sort_col and sort_col in d.columns:
        d = d.sort_values(sort_col)
    d.to_csv(out_path, index=False)
    print(f"[write] {out_path}  rows={len(d)}")

def _as_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        s = s.replace(",", "")
        return int(float(s))
    except Exception:
        return None

def _as_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None

def _detect_unit_to_seconds(epoch_num: int) -> Optional[float]:
    """
    Convert unknown epoch unit to seconds:
      - >= 18 digits: ns
      - >= 15 digits: us
      - >= 13 digits: ms
      - else: seconds
    """
    if epoch_num is None:
        return None
    n = len(str(abs(int(epoch_num))))
    if n >= 18:
        return epoch_num / 1e9
    if n >= 15:
        return epoch_num / 1e6
    if n >= 13:
        return epoch_num / 1e3
    return float(epoch_num)

def _epoch_any_to_naive(epoch_any: Any, zone_offset_seconds: Optional[int]) -> Optional[datetime]:
    raw = _as_int(epoch_any)
    if raw is None:
        return None
    secs = _detect_unit_to_seconds(raw)
    if secs is None:
        return None
    dt_utc = EPOCH0 + timedelta(seconds=secs)
    if zone_offset_seconds is not None:
        dt_utc = dt_utc + timedelta(seconds=int(zone_offset_seconds))
    return dt_utc.replace(tzinfo=None)

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(q, (table,)).fetchone() is not None

def _get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]

def _first_present(cols: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lc = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None

def _parse_date_yyyy_mm_dd(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d")
    except Exception:
        return None

def _within_date_range(dt: datetime, start: Optional[str], end: Optional[str]) -> bool:
    if start:
        s0 = datetime.fromisoformat(start)
        if dt < s0:
            return False
    if end:
        e0 = datetime.fromisoformat(end) + timedelta(days=1)
        if dt >= e0:
            return False
    return True

def _parse_start_time_any(s: Any) -> Optional[datetime]:
    """
    Parse vendor string timestamps from step.start_time.

    Tries common patterns:
      - YYYY-MM-DD HH:MM:SS
      - YYYY-MM-DD HH:MM
      - YYYY-MM-DDTHH:MM:SS
      - YYYY-MM-DDTHH:MM:SS.sss
      - YYYY/MM/DD HH:MM:SS
      - YYYY/MM/DD HH:MM
      - DD.MM.YYYY HH:MM:SS
      - DD.MM.YYYY HH:MM
    Returns naive datetime or None.
    """
    if s is None:
        return None
    ss = str(s).strip()
    if ss == "" or ss.lower() == "nan":
        return None

    # fast path: fromisoformat for many variants (incl. "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DDTHH:MM:SS")
    try:
        # strip trailing Z if present (treat as naive after parse)
        iso = ss[:-1] if ss.endswith("Z") else ss
        dt = datetime.fromisoformat(iso)
        # if timezone-aware, drop tzinfo
        return dt.replace(tzinfo=None)
    except Exception:
        pass

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
    ]
    for f in fmts:
        try:
            return datetime.strptime(ss, f)
        except Exception:
            continue

    return None

# ------------------------ HRV: SchedualHRV ------------------------

def export_hrv_from_schedualhrv(
    conn: sqlite3.Connection,
    out_dir: Path,
    user_id: str,
    start: Optional[str],
    end: Optional[str],
) -> pd.DataFrame:
    out_path = out_dir / "hrv.csv"

    table = "SchedualHRV"
    if not _table_exists(conn, table):
        df_out = pd.DataFrame(columns=["user", "datetime", "rmssd_ms"])
        _write_csv(df_out, out_path)
        return df_out

    cols = _get_table_columns(conn, table)
    date_col = _first_present(cols, ["date", "day"])
    hrv_col  = _first_present(cols, ["HRV", "hrv"])

    if not date_col or not hrv_col:
        print(f"[warn] {table}: could not resolve required columns (date, HRV). Found: {cols}")
        df_out = pd.DataFrame(columns=["user", "datetime", "rmssd_ms"])
        _write_csv(df_out, out_path)
        return df_out

    df = pd.read_sql_query(f'SELECT "{date_col}" AS _date, "{hrv_col}" AS _hrv FROM "{table}"', conn)
    rows: List[Tuple[datetime, float]] = []

    for _, r in df.iterrows():
        day0 = _parse_date_yyyy_mm_dd(r["_date"])
        if day0 is None:
            continue
        if not _within_date_range(day0, start, end):
            continue

        raw = r["_hrv"]
        if raw is None:
            continue

        parts = [p.strip() for p in str(raw).split(",")]
        parts = parts[:48]

        for i, p in enumerate(parts):
            val = _as_float(p)
            if val is None:
                continue
            if val <= 0:
                continue

            dt = day0 + timedelta(minutes=30 * i)
            rows.append((dt, float(val)))

    if rows:
        out = pd.DataFrame(rows, columns=["datetime", "rmssd_ms"]).sort_values("datetime")
        out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        out.insert(0, "user", user_id)
        df_out = out[["user", "datetime", "rmssd_ms"]]
    else:
        df_out = pd.DataFrame(columns=["user", "datetime", "rmssd_ms"])

    _write_csv(df_out, out_path, sort_col="datetime")
    return df_out

# ------------------------ SpO2: BloodOxygen ------------------------

def export_spo2_from_bloodoxygen(
    conn: sqlite3.Connection,
    out_dir: Path,
    user_id: str,
    start: Optional[str],
    end: Optional[str],
    zone_offset_seconds: Optional[int],
) -> pd.DataFrame:
    out_path = out_dir / "blood_oxygen.csv"

    table = "BloodOxygen"
    if not _table_exists(conn, table):
        df_out = pd.DataFrame(columns=["user", "datetime", "spo2"])
        _write_csv(df_out, out_path)
        return df_out

    cols = _get_table_columns(conn, table)
    t_col = _first_present(cols, ["TimeInterval", "timeinterval", "timestamp", "time", "datetime"])
    v_col = _first_present(cols, ["soa2", "spo2", "SpO2", "sao2", "oxygen"])

    if not t_col or not v_col:
        print(f"[warn] {table}: could not resolve required columns (TimeInterval, soa2). Found: {cols}")
        df_out = pd.DataFrame(columns=["user", "datetime", "spo2"])
        _write_csv(df_out, out_path)
        return df_out

    df = pd.read_sql_query(f'SELECT "{t_col}" AS _t, "{v_col}" AS _v FROM "{table}"', conn)

    rows: List[Tuple[datetime, float]] = []
    for _, r in df.iterrows():
        dt = _epoch_any_to_naive(r["_t"], zone_offset_seconds)
        if dt is None:
            continue

        if start or end:
            if not _within_date_range(dt, start, end):
                continue

        v = _as_float(r["_v"])
        if v is None:
            continue
        if v == 0:
            continue

        rows.append((dt, float(v)))

    if rows:
        out = pd.DataFrame(rows, columns=["datetime", "spo2"]).sort_values("datetime")
        out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        out.insert(0, "user", user_id)
        df_out = out[["user", "datetime", "spo2"]]
    else:
        df_out = pd.DataFrame(columns=["user", "datetime", "spo2"])

    _write_csv(df_out, out_path, sort_col="datetime")
    return df_out

# ------------------------ Steps: step ------------------------

def export_steps_from_step_table(
    conn: sqlite3.Connection,
    out_dir: Path,
    user_id: str,
    start: Optional[str],
    end: Optional[str],
    steps_interval_min: int,
) -> pd.DataFrame:
    """
    Export steps into common interval format:
      steps.csv: user,start_datetime,end_datetime,steps
    """
    out_path = out_dir / "steps.csv"

    table = "step"
    if not _table_exists(conn, table):
        df_out = pd.DataFrame(columns=["user", "start_datetime", "end_datetime", "steps"])
        _write_csv(df_out, out_path)
        return df_out

    cols = _get_table_columns(conn, table)
    st_col = _first_present(cols, ["start_time", "start", "time", "datetime"])
    cnt_col = _first_present(cols, ["count", "steps", "value"])

    if not st_col or not cnt_col:
        print(f"[warn] {table}: could not resolve required columns (start_time, count). Found: {cols}")
        df_out = pd.DataFrame(columns=["user", "start_datetime", "end_datetime", "steps"])
        _write_csv(df_out, out_path)
        return df_out

    df = pd.read_sql_query(f'SELECT "{st_col}" AS _start, "{cnt_col}" AS _count FROM "{table}"', conn)

    rows: List[Tuple[datetime, datetime, int]] = []
    delta = timedelta(minutes=int(steps_interval_min))

    for _, r in df.iterrows():
        sdt = _parse_start_time_any(r["_start"])
        if sdt is None:
            continue

        if start or end:
            if not _within_date_range(sdt, start, end):
                continue

        c = _as_int(r["_count"])
        if c is None:
            continue
        if c < 0:
            continue

        edt = sdt + delta
        rows.append((sdt, edt, int(c)))

    if rows:
        out = pd.DataFrame(rows, columns=["start_datetime", "end_datetime", "steps"]).sort_values("start_datetime")
        out["start_datetime"] = out["start_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        out["end_datetime"]   = out["end_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        out.insert(0, "user", user_id)
        df_out = out[["user", "start_datetime", "end_datetime", "steps"]]
    else:
        df_out = pd.DataFrame(columns=["user", "start_datetime", "end_datetime", "steps"])

    _write_csv(df_out, out_path, sort_col="start_datetime")
    return df_out

# ------------------------ CLI ------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Blaupunkt vendor DB exporter → CSVs (HRV + SpO2 + Steps)")
    ap.add_argument("--db", required=True, help="Path to Blaupunkt .db (SQLite)")
    ap.add_argument("--out-dir", required=True, help="Output directory for CSVs")
    ap.add_argument("--user-id", default="USER123", help="Value for 'user' column in outputs")

    ap.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD), inclusive")
    ap.add_argument("--end",   type=str, default=None, help="End date (YYYY-MM-DD), inclusive")

    ap.add_argument(
        "--zone-offset-seconds",
        type=int,
        default=None,
        help="Optional fixed offset (seconds) added to BloodOxygen epoch timestamps. "
             "Example: UTC+2 → 7200. If omitted, epochs are treated as UTC and tzinfo is dropped.",
    )

    ap.add_argument(
        "--steps-interval-min",
        type=int,
        default=60,
        help="Interval length in minutes for each row in the step table (default: 60). "
             "Used to derive end_datetime = start_datetime + interval.",
    )

    return ap.parse_args()

def main() -> None:
    args = parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    out_dir = Path(args.out_dir).expanduser().resolve()
    _ensure_dir(out_dir)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    print(f"[db] {db_path}")
    print(f"[out] {out_dir}")
    if args.start or args.end:
        print(f"[range] start={args.start} end={args.end}")
    if args.zone_offset_seconds is not None:
        print(f"[tz] BloodOxygen epoch offset = {args.zone_offset_seconds} seconds")
    print(f"[steps] interval = {args.steps_interval_min} min")

    export_hrv_from_schedualhrv(conn, out_dir, args.user_id, args.start, args.end)
    export_spo2_from_bloodoxygen(conn, out_dir, args.user_id, args.start, args.end, args.zone_offset_seconds)
    export_steps_from_step_table(conn, out_dir, args.user_id, args.start, args.end, args.steps_interval_min)

    conn.close()
    print("\n[OK] Export complete.")
    print(f"Output dir: {out_dir}")

if __name__ == "__main__":
    main()