#!/usr/bin/env python3
"""
coverage_from_csvs.py

Compute coverage/quality metrics from RAW (non-interpolated) wearable CSV exports.

Metrics per modality/file:
- study_start / study_end (user-supplied window, or fallback to observed span)
- data_start / data_end (actual observed span within the study window)
- study duration (hours/days)
- total rows in window
- unique active hours (% of hours with >=1 observation)
- active days
- longest INTERNAL inter-measurement gap (hours, between consecutive measurements only)
- leading gap (hours): study_start -> first measurement
- trailing gap (hours): last measurement -> study_end
- sampling stats:
    - median/mean sampling interval (seconds)
    - derived median sampling frequency (Hz, per-minute, per-hour)

Outputs (in --out-dir):
- coverage_metrics.json
- coverage_metrics.csv
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd


TIME_COL_CANDIDATES = [
    "timestamp", "time", "datetime", "date", "minute",
    "start", "start_time", "start_datetime",
    "end", "end_time", "end_datetime",
]

EXTRA_TIME_CANDIDATES = [
    "local_time", "record_time", "recordTime",
    "measured_at", "measuredAt",
]


def _pick_time_col(df: pd.DataFrame) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for c in TIME_COL_CANDIDATES + EXTRA_TIME_CANDIDATES:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _coerce_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=False, infer_datetime_format=True)


def _to_hour_floor(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("H")


def _iso(ts: pd.Timestamp | None) -> Optional[str]:
    if ts is None or pd.isna(ts):
        return None
    return ts.isoformat()


@dataclass
class GapInfoHours:
    hours: float = 0.0
    start: Optional[str] = None
    end: Optional[str] = None


@dataclass
class ModalityMetrics:
    modality: str
    path: str
    rows_in_window: int

    study_start: Optional[str]
    study_end: Optional[str]
    study_duration_hours: float
    study_duration_days: float

    data_start: Optional[str]
    data_end: Optional[str]

    active_hours: int
    total_hours: int
    coverage_hours_pct: float

    active_days: int
    total_days: int
    coverage_days_pct: float

    longest_internal_intermeasurement_gap: GapInfoHours
    leading_gap: GapInfoHours
    trailing_gap: GapInfoHours

    median_sampling_seconds: Optional[float]
    mean_sampling_seconds: Optional[float]
    median_sampling_hz: Optional[float]
    median_sampling_per_minute: Optional[float]
    median_sampling_per_hour: Optional[float]


def _sampling_stats(ts: pd.Series) -> Tuple[Optional[float], Optional[float]]:
    if len(ts) < 2:
        return None, None
    diffs = ts.diff().dt.total_seconds().dropna()
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return None, None
    return float(diffs.median()), float(diffs.mean())


def _freq_from_seconds(sec: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if sec is None or sec <= 0:
        return None, None, None
    return (1 / sec, 60 / sec, 3600 / sec)


def _internal_longest_gap_hours(ts: pd.Series) -> GapInfoHours:
    if len(ts) < 2:
        return GapInfoHours()

    diffs = ts.diff().dt.total_seconds().dropna()
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return GapInfoHours()

    idx = diffs.idxmax()
    gap_hours = diffs.loc[idx] / 3600.0

    pos = ts.index.get_loc(idx)
    start = ts.iloc[pos - 1]
    end = ts.iloc[pos]

    return GapInfoHours(
        hours=float(gap_hours),
        start=_iso(start),
        end=_iso(end),
    )


def compute_metrics_for_csv(
    path: str,
    modality: str,
    window_start: Optional[pd.Timestamp],
    window_end: Optional[pd.Timestamp],
) -> ModalityMetrics:

    df = pd.read_csv(path)
    time_col = _pick_time_col(df)

    if time_col is None or df.empty:
        ts_all = pd.Series([], dtype="datetime64[ns]")
    else:
        ts_all = _coerce_datetime(df[time_col]).dropna().sort_values()

    if ts_all.empty:
        data_start_full = data_end_full = None
    else:
        data_start_full = ts_all.iloc[0]
        data_end_full = ts_all.iloc[-1]

    study_start = window_start if window_start is not None else data_start_full
    study_end = window_end if window_end is not None else data_end_full

    if study_start is None or study_end is None or study_end < study_start:
        return ModalityMetrics(
            modality, path, 0,
            _iso(study_start), _iso(study_end),
            0.0, 0.0,
            None, None,
            0, 0, 0.0,
            0, 0, 0.0,
            GapInfoHours(), GapInfoHours(), GapInfoHours(),
            None, None, None, None, None,
        )

    ts_win = ts_all[(ts_all >= study_start) & (ts_all <= study_end)]
    rows_in_window = len(ts_win)

    study_seconds = (study_end - study_start).total_seconds()
    study_hours = study_seconds / 3600.0
    study_days = study_seconds / 86400.0

    total_hours = int(((study_end.floor("H") - study_start.floor("H")).total_seconds() / 3600) + 1)
    total_days = int(((study_end.normalize() - study_start.normalize()).days) + 1)

    if ts_win.empty:
        return ModalityMetrics(
            modality, path, 0,
            _iso(study_start), _iso(study_end),
            study_hours, study_days,
            None, None,
            0, total_hours, 0.0,
            0, total_days, 0.0,
            GapInfoHours(),
            GapInfoHours(hours=study_hours, start=_iso(study_start), end=_iso(study_end)),
            GapInfoHours(),
            None, None, None, None, None,
        )

    data_start = ts_win.iloc[0]
    data_end = ts_win.iloc[-1]

    hours = _to_hour_floor(ts_win)
    active_hours = hours.nunique()
    coverage_hours_pct = (active_hours / total_hours * 100.0) if total_hours > 0 else 0.0

    active_days = ts_win.dt.date.nunique()
    coverage_days_pct = (active_days / total_days * 100.0) if total_days > 0 else 0.0

    internal_gap = _internal_longest_gap_hours(ts_win)

    leading_gap_hours = max(0.0, (data_start - study_start).total_seconds() / 3600.0)
    trailing_gap_hours = max(0.0, (study_end - data_end).total_seconds() / 3600.0)

    leading_gap = GapInfoHours(
        hours=leading_gap_hours,
        start=_iso(study_start),
        end=_iso(data_start),
    ) if leading_gap_hours > 0 else GapInfoHours()

    trailing_gap = GapInfoHours(
        hours=trailing_gap_hours,
        start=_iso(data_end),
        end=_iso(study_end),
    ) if trailing_gap_hours > 0 else GapInfoHours()

    med_s, mean_s = _sampling_stats(ts_win)
    hz, per_min, per_hour = _freq_from_seconds(med_s)

    return ModalityMetrics(
        modality, path, rows_in_window,
        _iso(study_start), _iso(study_end),
        study_hours, study_days,
        _iso(data_start), _iso(data_end),
        active_hours, total_hours, coverage_hours_pct,
        active_days, total_days, coverage_days_pct,
        internal_gap, leading_gap, trailing_gap,
        med_s, mean_s, hz, per_min, per_hour,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--participant", default=None)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)

    ap.add_argument("--heart-rate-csv")
    ap.add_argument("--hrv-csv")
    ap.add_argument("--spo2-csv")
    ap.add_argument("--temp-csv")
    ap.add_argument("--steps-csv")
    ap.add_argument("--sleep-csv")
    ap.add_argument("--resting-hr-csv")
    ap.add_argument("--vo2max-csv")

    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    window_start = pd.to_datetime(args.start, errors="coerce") if args.start else None
    window_end = pd.to_datetime(args.end, errors="coerce") if args.end else None

    inputs = [
        ("heart_rate", args.heart_rate_csv),
        ("hrv", args.hrv_csv),
        ("spo2", args.spo2_csv),
        ("temp", args.temp_csv),
        ("steps", args.steps_csv),
        ("sleep", args.sleep_csv),
        ("resting_hr", args.resting_hr_csv),
        ("vo2max", args.vo2max_csv),
    ]

    per_modality = [
        compute_metrics_for_csv(p, m, window_start, window_end)
        for m, p in inputs if p
    ]

    payload = {
        "participant": args.participant,
        "generated_at": datetime.now().isoformat(),
        "per_modality": [asdict(m) for m in per_modality],
    }

    json_path = os.path.join(args.out_dir, "coverage_metrics.json")
    with open(json_path, "w") as f:
        json.dump(payload, f, indent=2)

    rows = []
    for m in per_modality:
        d = asdict(m)

        for key in ("longest_internal_intermeasurement_gap", "leading_gap", "trailing_gap"):
            gap = d.pop(key)
            d[f"{key}_hours"] = gap["hours"]
            d[f"{key}_start"] = gap["start"]
            d[f"{key}_end"] = gap["end"]

        rows.append(d)

    csv_path = os.path.join(args.out_dir, "coverage_metrics.csv")
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    print(json_path)
    print(csv_path)


if __name__ == "__main__":
    main()