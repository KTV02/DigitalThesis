#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
apple_export_raw.py

Read Apple Health export XML (export.xml) and write raw CSVs matching the
Google raw exporter schema, with no interpolation and local wall-clock times:

  • heart_rate.csv           user,datetime,bpm
  • steps.csv                user,start_datetime,end_datetime,steps
  • sleep_episodes.csv       user,start,end,sleep_id,restless_seconds
  • sleep_stages.csv         user,start,end,stage
  • hrv.csv                  user,datetime,rmssd_ms        (*Apple provides SDNN; exported under rmssd_ms for schema compatibility*)
  • rr_intervals.csv         user,datetime,rr_ms           (if present)
  • vo2max.csv               user,datetime,vo2max
  • body_temperature.csv     user,datetime,temperature_c   (includes Sleeping Wrist Temperature)

Required args: --xml, --out-dir, --user-id, --start, --end
Optional: --source-name "Exact Source Name" to restrict to a specific device/app.

Times are parsed from Apple’s strings like "2025-09-01 13:35:00 +0200", kept in the
record’s local wall-clock, and written as naive "YYYY-MM-DD HH:MM:SS".
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import xml.etree.ElementTree as ET

import pandas as pd

# ------------------------ helpers ------------------------

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _write_csv(df: pd.DataFrame, out_path: Path, sort_col: Optional[str] = None):
    _ensure_dir(out_path.parent)
    d = df.copy()
    if sort_col and sort_col in d.columns:
        d = d.sort_values(sort_col)
    d.to_csv(out_path, index=False)
    print(f"[write] {out_path}  rows={len(d)}")

def _parse_apple_local_naive(s: str) -> Optional[datetime]:
    """
    Parse Apple datetime like '2025-09-01 13:35:00 +0200' into a naive local wall time.
    """
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce", utc=False)
    if pd.isna(dt):
        # try without offset (rare)
        dt = pd.to_datetime(s.split(" +")[0], errors="coerce", utc=False)
    if pd.isna(dt):
        return None
    return dt.tz_localize(None).to_pydatetime()

def _iter_records(xml_path: Path) -> Iterable[ET.Element]:
    """
    Stream the XML with iterparse; yield <Record> elements and clear them to save memory.
    """
    context = ET.iterparse(str(xml_path), events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag.endswith("Record"):
            yield elem
            elem.clear()
            root.clear()

def _source_matches(elem: ET.Element, wanted: Optional[str]) -> bool:
    if not wanted:
        return True
    return (elem.get("sourceName", "") or "").strip() == wanted.strip()

def _within_points_window(dt: datetime, start: Optional[datetime], end_exclusive: Optional[datetime]) -> bool:
    if start and dt < start:
        return False
    if end_exclusive and dt >= end_exclusive:
        return False
    return True

def _interval_overlaps_window(st: datetime, et: datetime, start: Optional[datetime], end_exclusive: Optional[datetime]) -> bool:
    # overlap with [start, end_exclusive)
    a0 = start if start else datetime.min
    a1 = end_exclusive if end_exclusive else datetime.max
    return (st < a1) and (et > a0)

# ------------------------ sleep ------------------------

# Map Apple SleepAnalysis 'value' → our stage labels
def _apple_sleep_value_to_stage(val: str) -> Optional[str]:
    v = (val or "").lower()
    if "awake" in v:
        return "wake"
    if "asleeprem" in v:
        return "rem"
    if "asleepdeep" in v:
        return "deep"
    if "asleepcore" in v:
        return "light"
    if "asleep" in v:
        return "asleep"  # generic asleep when unspecified
    if "inbed" in v:
        return None     
    return None

def _build_sleep_episodes_and_stages(xml_path: Path,
                                     start: Optional[datetime],
                                     end_exclusive: Optional[datetime],
                                     user: str,
                                     source_name: Optional[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build sleep episodes from Apple SleepAnalysis 'Asleep*' blocks (union/merge).
    restless_seconds is computed from overlap with 'Awake' blocks.
    Stages are emitted per block, clipped to the episode boundary, mapped to {wake/light/deep/rem/asleep}.
    """
    asleep_segments: List[Tuple[datetime, datetime]] = []
    awake_segments: List[Tuple[datetime, datetime]] = []
    staged_segments: List[Tuple[datetime, datetime, str]] = []

    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue

        rtype = rec.get("type", "") or ""
        if rtype != "HKCategoryTypeIdentifierSleepAnalysis":
            continue

        v_raw = rec.get("value", "") or ""
        st = _parse_apple_local_naive(rec.get("startDate", ""))
        et = _parse_apple_local_naive(rec.get("endDate", ""))
        if st is None or et is None or et <= st:
            continue

        if not _interval_overlaps_window(st, et, start, end_exclusive):
            continue

        v_low = v_raw.lower()

        # Collect for episodes & stages
        if "awake" in v_low:
            awake_segments.append((st, et))
            staged_segments.append((st, et, "wake"))
        elif "asleep" in v_low:
            asleep_segments.append((st, et))
            stage = _apple_sleep_value_to_stage(v_raw) or "asleep"
            staged_segments.append((st, et, stage))
        else:
            # InBed or others: ignore for episodes; do not add stage
            pass

    # Merge asleep segments into episodes
    if not asleep_segments:
        episodes = []
    else:
        asleep_segments.sort(key=lambda x: x[0])
        episodes = []
        cs, ce = asleep_segments[0]
        for s, e in asleep_segments[1:]:
            if s <= ce:
                ce = max(ce, e)
            else:
                episodes.append((cs, ce))
                cs, ce = s, e
        episodes.append((cs, ce))

    # restless_seconds per episode = overlap with awake segments
    ep_rows = []
    sid = 1
    for s, e in episodes:
        restless = 0.0
        for aw_s, aw_e in awake_segments:
            os = max(s, aw_s)
            oe = min(e, aw_e)
            if oe > os:
                restless += (oe - os).total_seconds()
        ep_rows.append({
            "user": user,
            "start": s.strftime("%Y-%m-%d %H:%M:%S"),
            "end":   e.strftime("%Y-%m-%d %H:%M:%S"),
            "sleep_id": sid,
            "restless_seconds": int(round(restless))
        })
        sid += 1
    episodes_df = pd.DataFrame(ep_rows, columns=["user","start","end","sleep_id","restless_seconds"])

    # Stages: clip to episodes; if no episodes, emit stages as-is
    stage_rows = []
    if episodes:
        for s_st, s_et, lab in staged_segments:
            for ep_s, ep_e in episodes:
                os = max(s_st, ep_s)
                oe = min(s_et, ep_e)
                if oe > os:
                    stage_rows.append({
                        "user": user,
                        "start": os.strftime("%Y-%m-%d %H:%M:%S"),
                        "end":   oe.strftime("%Y-%m-%d %H:%M:%S"),
                        "stage": lab
                    })
    else:
        for s_st, s_et, lab in staged_segments:
            stage_rows.append({
                "user": user,
                "start": s_st.strftime("%Y-%m-%d %H:%M:%S"),
                "end":   s_et.strftime("%Y-%m-%d %H:%M:%S"),
                "stage": lab
            })

    if episodes and not stage_rows:
        for s, e in episodes:
            stage_rows.append({"user": user, "start": s.strftime("%Y-%m-%d %H:%M:%S"),
                               "end": e.strftime("%Y-%m-%d %H:%M:%S"), "stage": "asleep"})

    stages_df = pd.DataFrame(stage_rows, columns=["user","start","end","stage"])
    return episodes_df, stages_df

# ------------------------ metrics exporters ------------------------

def export_heart_rate(xml_path: Path, out_dir: Path, user: str,
                      start: Optional[datetime], end_exclusive: Optional[datetime],
                      source_name: Optional[str]) -> pd.DataFrame:
    rows = []
    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue
        if rec.get("type", "") != "HKQuantityTypeIdentifierHeartRate":
            continue
        dt = _parse_apple_local_naive(rec.get("startDate", ""))
        if dt is None or not _within_points_window(dt, start, end_exclusive):
            continue
        try:
            bpm = float(rec.get("value", "nan"))
        except Exception:
            continue
        rows.append((dt, bpm))

    if not rows:
        out = pd.DataFrame(columns=["user","datetime","bpm"])
        _write_csv(out, out_dir / "heart_rate.csv")
        return out

    df = pd.DataFrame(rows, columns=["datetime","bpm"]).sort_values("datetime")
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "user", user)
    _write_csv(df[["user","datetime","bpm"]], out_dir / "heart_rate.csv")
    return df

def export_steps(xml_path: Path, out_dir: Path, user: str,
                 start: Optional[datetime], end_exclusive: Optional[datetime],
                 source_name: Optional[str]) -> pd.DataFrame:
    """
    Raw step intervals from HKQuantityTypeIdentifierStepCount.
    """
    rows = []
    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue
        if rec.get("type", "") != "HKQuantityTypeIdentifierStepCount":
            continue
        st = _parse_apple_local_naive(rec.get("startDate", ""))
        et = _parse_apple_local_naive(rec.get("endDate", ""))
        if st is None or et is None or et <= st:
            continue
        if not _interval_overlaps_window(st, et, start, end_exclusive):
            continue
        try:
            steps = int(float(rec.get("value", "0")))
        except Exception:
            continue
        rows.append((st, et, steps))

    if not rows:
        out = pd.DataFrame(columns=["user","start_datetime","end_datetime","steps"])
        _write_csv(out, out_dir / "steps.csv")
        return out

    df = pd.DataFrame(rows, columns=["start_datetime","end_datetime","steps"]).sort_values("start_datetime")
    if start:
        df = df[df["end_datetime"] >= start]
    if end_exclusive:
        df = df[df["start_datetime"] < end_exclusive]

    df["start_datetime"] = df["start_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["end_datetime"]   = df["end_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "user", user)
    _write_csv(df[["user","start_datetime","end_datetime","steps"]], out_dir / "steps.csv")
    return df

def export_vo2max(xml_path: Path, out_dir: Path, user: str,
                  start: Optional[datetime], end_exclusive: Optional[datetime],
                  source_name: Optional[str]) -> pd.DataFrame:
    rows = []
    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue
        if rec.get("type", "") != "HKQuantityTypeIdentifierVO2Max":
            continue
        dt = _parse_apple_local_naive(rec.get("startDate", ""))
        if dt is None or not _within_points_window(dt, start, end_exclusive):
            continue
        try:
            v = float(rec.get("value", "nan"))
        except Exception:
            continue
        rows.append((dt, v))
    if not rows:
        out = pd.DataFrame(columns=["user","datetime","vo2max"])
        _write_csv(out, out_dir / "vo2max.csv")
        return out
    df = pd.DataFrame(rows, columns=["datetime","vo2max"]).sort_values("datetime")
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "user", user)
    _write_csv(df[["user","datetime","vo2max"]], out_dir / "vo2max.csv")
    return df

def export_rr(xml_path: Path, out_dir: Path, user: str,
              start: Optional[datetime], end_exclusive: Optional[datetime],
              source_name: Optional[str]) -> pd.DataFrame:
    """
    Try common RR series identifiers (vendors differ). Values often seconds; convert to ms if <10.
    """
    SUBSTRS = ("BeatToBeat", "HeartBeatSeries", "RRInterval")
    rows = []
    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue
        rtype = rec.get("type", "") or ""
        if not any(s in rtype for s in SUBSTRS):
            continue
        dt = _parse_apple_local_naive(rec.get("startDate", ""))
        if dt is None or not _within_points_window(dt, start, end_exclusive):
            continue
        try:
            v = float(rec.get("value", "nan"))
        except Exception:
            continue
        rr_ms = v * 1000.0 if v < 10.0 else v
        rows.append((dt, rr_ms))
    if not rows:
        out = pd.DataFrame(columns=["user","datetime","rr_ms"])
        _write_csv(out, out_dir / "rr_intervals.csv")
        return out
    df = pd.DataFrame(rows, columns=["datetime","rr_ms"]).sort_values("datetime")
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "user", user)
    _write_csv(df[["user","datetime","rr_ms"]], out_dir / "rr_intervals.csv")
    return df

def export_hrv(xml_path: Path, out_dir: Path, user: str,
               start: Optional[datetime], end_exclusive: Optional[datetime],
               source_name: Optional[str]) -> pd.DataFrame:
    """
    Apple exposes HRV as SDNN (HKQuantityTypeIdentifierHeartRateVariabilitySDNN) in ms.
    For schema compatibility with Google raw, we emit it as 'rmssd_ms' in hrv.csv.
    """
    rows = []
    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue
        if rec.get("type", "") != "HKQuantityTypeIdentifierHeartRateVariabilitySDNN":
            continue
        dt = _parse_apple_local_naive(rec.get("startDate", ""))
        if dt is None or not _within_points_window(dt, start, end_exclusive):
            continue
        try:
            ms = float(rec.get("value", "nan"))
        except Exception:
            continue
        rows.append((dt, ms))

    if not rows:
        out = pd.DataFrame(columns=["user","datetime","rmssd_ms"])
        _write_csv(out, out_dir / "hrv.csv")
        return out

    df = pd.DataFrame(rows, columns=["datetime","rmssd_ms"]).sort_values("datetime")
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "user", user)
    _write_csv(df[["user","datetime","rmssd_ms"]], out_dir / "hrv.csv")
    return df

def export_body_temperature(xml_path: Path, out_dir: Path, user: str,
                            start: Optional[datetime], end_exclusive: Optional[datetime],
                            source_name: Optional[str]) -> pd.DataFrame:
    """
    Body temperature (°C). We look for:
      - HKQuantityTypeIdentifierBodyTemperature
      - HKQuantityTypeIdentifierBasalBodyTemperature
      - HKQuantityTypeIdentifierAppleSleepingWristTemperature  <-- added
    """
    TYPES = {
        "HKQuantityTypeIdentifierBodyTemperature",
        "HKQuantityTypeIdentifierBasalBodyTemperature",
        "HKQuantityTypeIdentifierAppleSleepingWristTemperature",
    }
    rows = []
    for rec in _iter_records(xml_path):
        if not _source_matches(rec, source_name):
            continue
        rtype = rec.get("type", "")
        if rtype not in TYPES:
            continue
        dt = _parse_apple_local_naive(rec.get("startDate", ""))
        if dt is None or not _within_points_window(dt, start, end_exclusive):
            continue
        try:
            temp_c = float(rec.get("value", "nan"))
        except Exception:
            continue
        rows.append((dt, temp_c))
    if not rows:
        out = pd.DataFrame(columns=["user","datetime","temperature_c"])
        _write_csv(out, out_dir / "body_temperature.csv")
        return out
    df = pd.DataFrame(rows, columns=["datetime","temperature_c"]).sort_values("datetime")
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "user", user)
    _write_csv(df[["user","datetime","temperature_c"]], out_dir / "body_temperature.csv")
    return df

# ------------------------ CLI ------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Apple Health export.xml → raw CSVs (schema aligned with Google raw)")
    ap.add_argument("--xml", required=True, help="Path to Apple Health export.xml")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--user-id", default="USER123", help="Value for 'user' column")
    ap.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD), inclusive")
    ap.add_argument("--end",   type=str, required=True, help="End date (YYYY-MM-DD), inclusive")
    ap.add_argument("--source-name", type=str, default=None,
                    help="If provided, only use records whose sourceName exactly matches this string.")
    return ap.parse_args()

def main():
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    _ensure_dir(out_dir)

    xml_path = Path(args.xml).expanduser().resolve()
    if not xml_path.exists():
        raise FileNotFoundError(f"XML not found: {xml_path}")

    # Window: [start 00:00, end+1 00:00)
    start_dt = datetime.fromisoformat(args.start)
    end_exclusive = datetime.fromisoformat(args.end) + timedelta(days=1)

    # Sleep first (episodes + stages)
    episodes, stages = _build_sleep_episodes_and_stages(
        xml_path, start_dt, end_exclusive, args.user_id, args.source_name
    )
    _write_csv(episodes, out_dir / "sleep_episodes.csv", sort_col="start")
    _write_csv(stages,   out_dir / "sleep_stages.csv",   sort_col="start")

    # Point/interval metrics
    export_heart_rate(xml_path, out_dir, args.user_id, start_dt, end_exclusive, args.source_name)
    export_steps(xml_path, out_dir, args.user_id, start_dt, end_exclusive, args.source_name)
    export_vo2max(xml_path, out_dir, args.user_id, start_dt, end_exclusive, args.source_name)
    export_rr(xml_path, out_dir, args.user_id, start_dt, end_exclusive, args.source_name)
    export_hrv(xml_path, out_dir, args.user_id, start_dt, end_exclusive, args.source_name)
    export_body_temperature(xml_path, out_dir, args.user_id, start_dt, end_exclusive, args.source_name)

    print("\n[OK] Raw Apple export complete.")
    print(f"Output dir: {out_dir}")

if __name__ == "__main__":
    main()