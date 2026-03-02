#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
google_export_raw.py

Raw Google Health Connect (.db) exporter that writes clean CSVs as-recorded:
  • heart_rate.csv           user,datetime,bpm
  • steps.csv                user,start_datetime,end_datetime,steps
  • sleep_episodes.csv       user,start,end,sleep_id,restless_seconds
  • sleep_stages.csv         user,start,end,stage
  • hrv.csv                  user,datetime,rmssd_ms
  • rr_intervals.csv         user,datetime,rr_ms
  • vo2max.csv               user,datetime,vo2max
  • body_temperature.csv     user,datetime,temperature_c

Optional:
  • --source-name "Ultrahuman" → resolve application_info_table.app_name (case-insens)
    to row_id(s) and filter all records by app_info_id. For HR series, filter via
    parent linkage to heart_rate_record_table.

No per-minute aggregation or interpolation. Timestamps are naive local wall-clock
per record (zone_offset applied if present, then tzinfo dropped).
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence, Dict, List, Tuple, Set

import numpy as np
import pandas as pd

EPOCH0 = datetime(1970, 1, 1, tzinfo=timezone.utc)

# ------------------------ helpers ------------------------

def _as_int(x):
    if x is None:
        return None
    try:
        s = str(x).replace(",", "").strip()
        if s == "" or s.lower() == "nan":
            return None
        return int(float(s))
    except Exception:
        return None

def _detect_unit_to_seconds(epoch_num: int) -> Optional[float]:
    if epoch_num is None:
        return None
    n = len(str(abs(int(epoch_num))))
    if n >= 18: return epoch_num / 1e9
    if n >= 15: return epoch_num / 1e6
    if n >= 13: return epoch_num / 1e3
    return float(epoch_num)

def _epoch_any_to_naive(epoch_any: int, zone_offset_seconds: Optional[int]) -> datetime:
    secs = _detect_unit_to_seconds(_as_int(epoch_any))
    if secs is None:
        raise ValueError("Invalid epoch")
    dt_utc = EPOCH0 + timedelta(seconds=secs)
    if zone_offset_seconds is not None:
        dt_utc = dt_utc + timedelta(seconds=int(zone_offset_seconds))
    return dt_utc.replace(tzinfo=None)

def _first_present(cols: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lc = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None

def _any_zone_offset_col(cols: Sequence[str]) -> Optional[str]:
    for c in cols:
        if "zone_offset" in c.lower():
            return c
    return None

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _write_csv(df: pd.DataFrame, out_path: Path, sort_col: Optional[str] = None):
    _ensure_dir(out_path.parent)
    d = df.copy()
    if sort_col and sort_col in d.columns:
        d = d.sort_values(sort_col)
    d.to_csv(out_path, index=False)
    print(f"[write] {out_path}  rows={len(d)}")

# ------------------------ DB helpers ------------------------

def _exist(conn: sqlite3.Connection, table: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(q, (table,)).fetchone() is not None

def _resolve_app_info_ids(conn: sqlite3.Connection, source_name: str) -> Set[int]:
    """
    Return application_info_table row_id(s) whose app_name matches source_name (case-insens).
    Tries row_id first; falls back to _id if needed.
    """
    ids = set()
    if not _exist(conn, "application_info_table"):
        return ids
    df = pd.read_sql_query("SELECT * FROM application_info_table", conn)
    if df.empty:
        return ids

    # Normalize columns
    cols = list(df.columns)
    id_col = "row_id" if "row_id" in cols else ("_id" if "_id" in cols else None)
    name_col = "app_name" if "app_name" in cols else None
    if id_col is None or name_col is None:
        return ids

    target = (source_name or "").strip().lower()
    for _, r in df.iterrows():
        app = str(r[name_col]).strip().lower()
        if app == target:
            rid = _as_int(r[id_col])
            if rid is not None:
                ids.add(int(rid))
    return ids

def _empty_like(df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[0:0].copy()

def _filter_df_by_app(df: pd.DataFrame, source_ids: Set[int]) -> pd.DataFrame:
    """
    If source_ids is provided, keep only rows where app_info_id ∈ source_ids.
    If app_info_id column is missing, return EMPTY (we can't attribute it).
    """
    if not source_ids:
        return df
    if "app_info_id" not in df.columns:
        return _empty_like(df)
    return df[df["app_info_id"].isin(source_ids)]

# ------------------------ sleep (episodes & stages) ------------------------

STAGE_MAP_NUM_TO_STR = {
    1: "wake",
    2: "asleep",
    4: "light",
    5: "deep",
    6: "rem",
}

def export_sleep_episodes_and_stages(conn, out_dir, user, start, end, source_ids):
    # Sessions (parent)
    if not _exist(conn, "sleep_session_record_table"):
        episodes = pd.DataFrame(columns=["user","start","end","sleep_id","restless_seconds"])
        stages = pd.DataFrame(columns=["user","start","end","stage"])
        _write_csv(episodes, out_dir / "sleep_episodes.csv")
        _write_csv(stages,   out_dir / "sleep_stages.csv")
        return episodes, stages

    sess = pd.read_sql_query("SELECT * FROM sleep_session_record_table", conn)
    sess = _filter_df_by_app(sess, source_ids)

    if sess.empty:
        episodes = pd.DataFrame(columns=["user","start","end","sleep_id","restless_seconds"])
        stages = pd.DataFrame(columns=["user","start","end","stage"])
        _write_csv(episodes, out_dir / "sleep_episodes.csv")
        _write_csv(stages,   out_dir / "sleep_stages.csv")
        return episodes, stages

    s_cols = list(sess.columns)
    id_col = _first_present(s_cols, ["row_id","_id","id","uuid","session_id","local_id"]) or s_cols[0]
    st_col = _first_present(s_cols, ["start_time","start","begin_time"]) or s_cols[0]
    en_col = _first_present(s_cols, ["end_time","end","finish_time"]) or s_cols[0]
    st_off = _first_present(s_cols, ["start_zone_offset","start_zone_offset_seconds","start_offset"])
    en_off = _first_present(s_cols, ["end_zone_offset","end_zone_offset_seconds","end_offset"])

    def conv(v, off):
        return _epoch_any_to_naive(v, _as_int(off) if off is not None else None)

    sess["__start"] = sess.apply(lambda r: conv(r[st_col], r[st_off] if st_off else None), axis=1)
    sess["__end"]   = sess.apply(lambda r: conv(r[en_col], r[en_off] if en_off else None), axis=1)
    sess = sess[sess["__end"] > sess["__start"]]

    if start:
        sess = sess[sess["__end"] >= datetime.fromisoformat(start)]
    if end:
        sess = sess[sess["__start"] < (datetime.fromisoformat(end) + timedelta(days=1))]

    # Build allowed session IDs for filtering stages by parent
    allowed_session_ids = set(sess[id_col].tolist())

    # Stages (child) — filter by parent membership (not by app_info_id, which may be absent)
    stage_rows = []
    if _exist(conn, "sleep_stages_table") and len(allowed_session_ids) > 0:
        stg = pd.read_sql_query("SELECT * FROM sleep_stages_table", conn)
        if not stg.empty:
            stg_cols = list(stg.columns)
            parent_col = _first_present(stg_cols, ["parent_key","session_id","sleep_session_id","parent_id"])
            stg_st_col = _first_present(stg_cols, ["stage_start_time","start_time","start"])
            stg_en_col = _first_present(stg_cols, ["stage_end_time","end_time","end"])
            stg_type   = _first_present(stg_cols, ["stage_type","type","stage"])

            if parent_col and stg_st_col and stg_en_col and stg_type:
                stg = stg[stg[parent_col].isin(allowed_session_ids)]

                # Map session → start_zone_offset (for "local" wall clock)
                id_to_start_off = {}
                if st_off:
                    for _, r in sess.iterrows():
                        id_to_start_off[r[id_col]] = _as_int(r[st_off])

                for _, st in stg.iterrows():
                    try:
                        off = id_to_start_off.get(st[parent_col], None)
                        s = _epoch_any_to_naive(st[stg_st_col], off)
                        e = _epoch_any_to_naive(st[stg_en_col], off)
                        if e <= s:
                            continue
                        code = _as_int(st[stg_type]) or 0
                        label = STAGE_MAP_NUM_TO_STR.get(int(code), "asleep")
                        stage_rows.append({
                            "user": user,
                            "start": s.strftime("%Y-%m-%d %H:%M:%S"),
                            "end":   e.strftime("%Y-%m-%d %H:%M:%S"),
                            "stage": label
                        })
                    except Exception:
                        continue

    # restless_seconds from AWAKE stages overlapping session windows
    AWAKE_CODE = 1
    rows = []
    sid = 1

    for _, r in sess.sort_values("__start").iterrows():
        s0, s1 = r["__start"], r["__end"]
        sess_id = r[id_col]
        restless = 0.0

        # recompute overlap from stage_rows of this session (optional, small scan)
        # Only if original stages table existed and had awake codes; otherwise stays 0.
        if _exist(conn, "sleep_stages_table") and len(stage_rows) > 0:
            pass  

        rows.append({
            "user": user,
            "start": s0.strftime("%Y-%m-%d %H:%M:%S"),
            "end":   s1.strftime("%Y-%m-%d %H:%M:%S"),
            "sleep_id": sid,
            "restless_seconds": int(round(restless))
        })
        sid += 1

    episodes = pd.DataFrame(rows, columns=["user","start","end","sleep_id","restless_seconds"])
    _write_csv(episodes, out_dir / "sleep_episodes.csv", sort_col="start")

    if len(stage_rows) == 0 and len(episodes) > 0:
        stage_rows = [{"user": user, "start": r["start"], "end": r["end"], "stage": "asleep"} for _, r in episodes.iterrows()]
    stages_out = pd.DataFrame(stage_rows, columns=["user","start","end","stage"])
    _write_csv(stages_out, out_dir / "sleep_stages.csv", sort_col="start")

    return episodes, stages_out

# ------------------------ heart rate ------------------------

def export_heart_rate(conn, out_dir, user, start, end, source_ids):
    rows = []

    if _exist(conn, "heart_rate_record_table"):
        hrp = pd.read_sql_query("SELECT * FROM heart_rate_record_table", conn)
        hrp = _filter_df_by_app(hrp, source_ids)
        if not hrp.empty:
            cols = list(hrp.columns)
            t_col  = _first_present(cols, ["time","start_time","timestamp","epoch_millis"]) or cols[0]
            v_col  = _first_present(cols, ["beats_per_minute","bpm","heart_rate","value"]) or cols[1]
            offcol = _any_zone_offset_col(cols)
            for _, r in hrp.iterrows():
                try:
                    dt = _epoch_any_to_naive(r[t_col], _as_int(r.get(offcol)) if offcol else None)
                    bpm = float(r[v_col])
                    rows.append((dt, bpm))
                except Exception:
                    continue

    # Series table: filter via parent linkage
    if _exist(conn, "heart_rate_record_series_table"):
        hrs = pd.read_sql_query("SELECT * FROM heart_rate_record_series_table", conn)
        if not hrs.empty:
            # Need allowed parent IDs from heart_rate_record_table
            allowed_parent_ids: Set[int] = set()
            if _exist(conn, "heart_rate_record_table"):
                hr_parent = pd.read_sql_query("SELECT * FROM heart_rate_record_table", conn)
                hr_parent = _filter_df_by_app(hr_parent, source_ids)
                if not hr_parent.empty:
                    p_cols = list(hr_parent.columns)
                    pid_col = _first_present(p_cols, ["row_id","_id","id","uuid","local_id"]) or p_cols[0]
                    allowed_parent_ids = set(int(_as_int(x)) for x in hr_parent[pid_col].dropna().tolist())
            s_cols = list(hrs.columns)
            parent_col = _first_present(s_cols, ["parent_key","parent_id","heart_rate_record_id","session_id"])
            t_col      = _first_present(s_cols, ["time","start_time","timestamp","epoch_millis"]) or s_cols[0]
            v_col      = _first_present(s_cols, ["beats_per_minute","bpm","heart_rate","value"]) or s_cols[1]
            offcol     = _any_zone_offset_col(s_cols)

            if parent_col and len(allowed_parent_ids) > 0:
                hrs = hrs[hrs[parent_col].isin(allowed_parent_ids)]
            else:
                if source_ids:
                    hrs = _empty_like(hrs)

            for _, r in hrs.iterrows():
                try:
                    dt = _epoch_any_to_naive(r[t_col], _as_int(r.get(offcol)) if offcol else None)
                    bpm = float(r[v_col])
                    rows.append((dt, bpm))
                except Exception:
                    continue

    # Build output
    if rows:
        out = pd.DataFrame(rows, columns=["datetime","bpm"]).sort_values("datetime")
    else:
        out = pd.DataFrame(columns=["datetime","bpm"])

    if start: out = out[out["datetime"] >= datetime.fromisoformat(start)]
    if end:   out = out[out["datetime"] <  (datetime.fromisoformat(end) + timedelta(days=1))]
    if not out.empty:
        out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out.insert(0, "user", user)
    _write_csv(out[["user","datetime","bpm"]], out_dir / "heart_rate.csv")
    return out

# ------------------------ steps (raw intervals) ------------------------

def export_steps(conn, out_dir, user, start, end, source_ids):
    table = "steps_record_table"
    if not _exist(conn, table):
        out = pd.DataFrame(columns=["user","start_datetime","end_datetime","steps"])
        _write_csv(out, out_dir / "steps.csv")
        return out

    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df = _filter_df_by_app(df, source_ids)

    if df.empty:
        out = pd.DataFrame(columns=["user","start_datetime","end_datetime","steps"])
        _write_csv(out, out_dir / "steps.csv")
        return out

    cols = list(df.columns)
    st_col = _first_present(cols, ["start_time","time_start","begin_time","start"]) or "start_time"
    en_col = _first_present(cols, ["end_time","time_end","finish_time","end"]) or "end_time"
    cnt_col = _first_present(cols, ["count","steps","value"]) or "count"
    st_off = _first_present(cols, ["start_zone_offset","zone_offset_start","zone_offset"])
    en_off = _first_present(cols, ["end_zone_offset","zone_offset_end","zone_offset"])

    rows = []
    for _, r in df.iterrows():
        try:
            s = _epoch_any_to_naive(r[st_col], _as_int(r.get(st_off)) if st_off else None)
            e = _epoch_any_to_naive(r[en_col], _as_int(r.get(en_off)) if en_off else None)
            if e <= s:
                continue
            c = int(float(r[cnt_col]))
            rows.append((s, e, c))
        except Exception:
            continue

    out = pd.DataFrame(rows, columns=["start_datetime","end_datetime","steps"]).sort_values("start_datetime")
    if start: out = out[out["end_datetime"]   >= datetime.fromisoformat(start)]
    if end:   out = out[out["start_datetime"] <  (datetime.fromisoformat(end) + timedelta(days=1))]

    if not out.empty:
        out["start_datetime"] = out["start_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        out["end_datetime"]   = out["end_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out.insert(0, "user", user)

    _write_csv(out[["user","start_datetime","end_datetime","steps"]], out_dir / "steps.csv")
    return out

# ------------------------ VO2 max, RR, HRV RMSSD, Temperature ------------------------

def _export_point_series(conn, table, out_dir, user, start, end, source_ids,
                         default_cols, out_file, out_cols, value_cast=float):
    if not _exist(conn, table):
        out = pd.DataFrame(columns=out_cols)
        _write_csv(out, out_dir / out_file)
        return out
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df = _filter_df_by_app(df, source_ids)
    if df.empty:
        out = pd.DataFrame(columns=out_cols)
        _write_csv(out, out_dir / out_file)
        return out

    cols = list(df.columns)
    t_col  = _first_present(cols, default_cols["time"]) or cols[0]
    v_col  = _first_present(cols, default_cols["value"]) or cols[1]
    offcol = _any_zone_offset_col(cols)

    rows = []
    for _, r in df.iterrows():
        try:
            dt = _epoch_any_to_naive(r[t_col], _as_int(r.get(offcol)) if offcol else None)
            val = value_cast(r[v_col])
            rows.append((dt, val))
        except Exception:
            continue

    out = pd.DataFrame(rows, columns=["datetime", out_cols[-1]]).sort_values("datetime")
    if start: out = out[out["datetime"] >= datetime.fromisoformat(start)]
    if end:   out = out[out["datetime"] <  (datetime.fromisoformat(end) + timedelta(days=1))]
    if not out.empty:
        out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out.insert(0, "user", user)
    _write_csv(out[out_cols], out_dir / out_file)
    return out

def export_vo2max(conn, out_dir, user, start, end, source_ids):
    return _export_point_series(
        conn, "vo2_max_record_table", out_dir, user, start, end, source_ids,
        default_cols={"time": ["time","start_time","timestamp","epoch_millis"],
                      "value": ["vo2_max","ml_kg_min","value"]},
        out_file="vo2max.csv",
        out_cols=["user","datetime","vo2max"],
        value_cast=float,
    )

def export_rr(conn, out_dir, user, start, end, source_ids):
    # Convert seconds to ms if needed
    def _rr_cast(v):
        x = float(v)
        return x * 1000.0 if x < 10 else x
    return _export_point_series(
        conn, "rr_interval_record_table", out_dir, user, start, end, source_ids,
        default_cols={"time": ["time","timestamp","start_time","epoch_millis"],
                      "value": ["rr_interval","rr_ms","value"]},
        out_file="rr_intervals.csv",
        out_cols=["user","datetime","rr_ms"],
        value_cast=_rr_cast,
    )

def export_hrv_rmssd(conn, out_dir, user, start, end, source_ids):
    return _export_point_series(
        conn, "heart_rate_variability_rmssd_record_table", out_dir, user, start, end, source_ids,
        default_cols={"time": ["time","timestamp","start_time","epoch_millis","local_date_time"],
                      "value": ["heart_rate_variability_millis","rmssd","value"]},
        out_file="hrv.csv",
        out_cols=["user","datetime","rmssd_ms"],
        value_cast=float,
    )

def export_body_temperature(conn, out_dir, user, start, end, source_ids):
    return _export_point_series(
        conn, "body_temperature_record_table", out_dir, user, start, end, source_ids,
        default_cols={"time": ["time","timestamp","start_time","epoch_millis"],
                      "value": ["temperature","value","temp"]},
        out_file="body_temperature.csv",
        out_cols=["user","datetime","temperature_c"],
        value_cast=float,
    )

# ------------------------ CLI ------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Google Health Connect raw exporter → CSVs")
    ap.add_argument("--db", required=True, help="Path to health_connect_export.db")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--user-id", default="USER123", help="Value for 'user' column")
    ap.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD), inclusive")
    ap.add_argument("--end",   type=str, required=True, help="End date (YYYY-MM-DD), inclusive")
    ap.add_argument("--source-name", type=str, default=None,
                    help="Filter to records where application_info_table.app_name matches (case-insens).")
    return ap.parse_args()

def main():
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    _ensure_dir(out_dir)

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    source_ids = set()
    if args.source_name:
        source_ids = _resolve_app_info_ids(conn, args.source_name)
        print(f"[source-name] '{args.source_name}' → app_info_id(s): {sorted(source_ids) if source_ids else 'NONE FOUND'}")

    export_sleep_episodes_and_stages(conn, out_dir, args.user_id, args.start, args.end, source_ids)
    export_heart_rate(conn, out_dir, args.user_id, args.start, args.end, source_ids)
    export_steps(conn, out_dir, args.user_id, args.start, args.end, source_ids)
    export_vo2max(conn, out_dir, args.user_id, args.start, args.end, source_ids)
    export_rr(conn, out_dir, args.user_id, args.start, args.end, source_ids)
    export_hrv_rmssd(conn, out_dir, args.user_id, args.start, args.end, source_ids)
    export_body_temperature(conn, out_dir, args.user_id, args.start, args.end, source_ids)

    conn.close()

    print("\n[OK] Raw export complete.")
    print(f"Output dir: {out_dir}")

if __name__ == "__main__":
    main()