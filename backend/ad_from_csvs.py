#!/usr/bin/env python3
# ad_from_csvs.py — format your minute CSVs for AnomalyDetect and (optionally) run the offline detectors.

from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import subprocess

# ---------- small utils ----------

def run(cmd, cwd: Optional[str] = None, env: Optional[dict] = None, check: bool = True) -> subprocess.CompletedProcess:
    print(f"\n[RUN] {' '.join(cmd)}")
    print(f"  CWD: {cwd or os.getcwd()}")
    proc = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.rstrip())
        if check:
            raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc

def ensure_exists(p: Path, label: str) -> Path:
    if not p.exists():
        raise FileNotFoundError(f"{label} not found: {p}")
    return p

def infer_bounds_from_hr(hr_csv: Path) -> Tuple[Optional[str], Optional[str]]:
    """Infer YYYY-MM-DD bounds from hr.csv."""
    try:
        df = pd.read_csv(hr_csv)
        if "datetime" not in df.columns:
            return (None, None)
        dt = pd.to_datetime(df["datetime"], errors="coerce")
        dt = dt.dropna()
        if dt.empty:
            return (None, None)
        return (dt.min().date().isoformat(), dt.max().date().isoformat())
    except Exception:
        return (None, None)

# ---------- formatters ----------

def _fmt_dt_mdy_minute(ts) -> str:
    # 'M/D/YY H:MM'  (no leading zeros for month/day/hour)
    # NOTE: strftime can't drop leading zeros portably; build manually.
    y2 = ts.year % 100
    return f"{ts.month}/{ts.day}/{y2} {ts.hour}:{ts.minute:02d}"

def _fmt_dt_iso_minute(ts) -> str:
    return ts.strftime("%Y-%m-%d %H:%M")

def _fmt_dt_iso_seconds(ts) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")

# ---------- coercion helpers (DROP 'user' col!) ----------

def make_hr_for_detectors(hr_src: Path, out_csv: Path, hr_datetime_format: str) -> int:
    """
    Read your minute HR file (either user,minute,bpm OR datetime,heartrate/bpm) and
    write detector-ready hr.csv with ONLY columns: datetime,heartrate
    """
    df = pd.read_csv(hr_src)
    cols = {c.lower(): c for c in df.columns}

    # Map to (datetime, heartrate)
    if "datetime" in cols and ("heartrate" in cols or "bpm" in cols or "hr" in cols):
        dt_col = cols["datetime"]
        hr_col = cols.get("heartrate") or cols.get("bpm") or cols.get("hr")
        df = df[[dt_col, hr_col]].rename(columns={dt_col: "datetime", hr_col: "heartrate"})
    else:
        # expect user, minute, bpm
        minute_col = cols.get("minute")
        bpm_col = cols.get("bpm") or cols.get("heartrate") or cols.get("hr")
        if not minute_col or not bpm_col:
            raise SystemExit(f"[HR] {hr_src} missing required columns. Have: {list(df.columns)}")
        df = df[[minute_col, bpm_col]].rename(columns={minute_col: "datetime", bpm_col: "heartrate"})

    # Parse and clean
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["heartrate"] = pd.to_numeric(df["heartrate"], errors="coerce")
    df = df.dropna(subset=["datetime", "heartrate"])
    if df.empty:
        # still write empty detector schema
        pd.DataFrame(columns=["datetime", "heartrate"]).to_csv(out_csv, index=False)
        print(f"[OK] Wrote {out_csv}  rows=0")
        return 0

    # Sort + unique minutes (if dup minutes, keep mean)
    df = df.sort_values("datetime")
    df["minute"] = df["datetime"].dt.floor("T")
    df = df.groupby("minute", as_index=False)["heartrate"].mean()
    df["datetime"] = df["minute"]
    df = df.drop(columns=["minute"])

    # Format datetime text
    if hr_datetime_format == "mdy_minute":
        df["datetime"] = df["datetime"].apply(_fmt_dt_mdy_minute)
    else:
        df["datetime"] = df["datetime"].apply(_fmt_dt_iso_minute)

    # Cast heartrate to int (legacy detectors expect ints)
    df["heartrate"] = df["heartrate"].round().astype(int)

    df[["datetime", "heartrate"]].to_csv(out_csv, index=False)
    print(f"[OK] Wrote {out_csv}  rows={len(df):,}")
    return len(df)

def make_steps_for_detectors(steps_src: Path, out_csv: Path) -> int:
    """
    Read your minute Steps file (either user,minute,steps OR datetime,steps) and
    write detector-ready steps.csv with ONLY columns: datetime,steps (ISO seconds)
    """
    df = pd.read_csv(steps_src)
    cols = {c.lower(): c for c in df.columns}

    if "datetime" in cols and "steps" in cols:
        dt_col = cols["datetime"]
        st_col = cols["steps"]
        df = df[[dt_col, st_col]].rename(columns={dt_col: "datetime", st_col: "steps"})
    else:
        minute_col = cols.get("minute")
        steps_col = cols.get("steps")
        if not minute_col or not steps_col:
            raise SystemExit(f"[STEPS] {steps_src} missing required columns. Have: {list(df.columns)}")
        df = df[[minute_col, steps_col]].rename(columns={minute_col: "datetime", steps_col: "steps"})

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["steps"] = pd.to_numeric(df["steps"], errors="coerce")
    df = df.dropna(subset=["datetime", "steps"])
    if df.empty:
        pd.DataFrame(columns=["datetime", "steps"]).to_csv(out_csv, index=False)
        print(f"[OK] Wrote {out_csv}  rows=0")
        return 0

    # consolidate per minute
    df = df.sort_values("datetime")
    df["minute"] = df["datetime"].dt.floor("T")
    df = df.groupby("minute", as_index=False)["steps"].sum()
    df["datetime"] = df["minute"]
    df = df.drop(columns=["minute"])

    # format with seconds for detectors
    df["datetime"] = df["datetime"].apply(_fmt_dt_iso_seconds)
    df["steps"] = df["steps"].round().astype(int)

    df[["datetime", "steps"]].to_csv(out_csv, index=False)
    print(f"[OK] Wrote {out_csv}  rows={len(df):,}")
    return len(df)

# ---------- detector runners ----------

def run_rhrad(python_bin: str, anomalydetect_dir: Path, out_dir: Path,
              user_id: str, outliers_fraction: float, random_seed: int,
              symptom_date: Optional[str], diagnosis_date: Optional[str]) -> None:
    script = ensure_exists(anomalydetect_dir / "rhrad_offline.py", "rhrad_offline.py")
    hr_csv = ensure_exists(out_dir / "hr.csv", "hr.csv")
    steps_csv = ensure_exists(out_dir / "steps.csv", "steps.csv")

    if not symptom_date or not diagnosis_date:
        s, e = infer_bounds_from_hr(hr_csv)
        symptom_date = symptom_date or s
        diagnosis_date = diagnosis_date or e

    anomalies = out_dir / f"{user_id}_rhrad_anomalies.csv"
    figure = out_dir / f"{user_id}_rhrad_anomalies.pdf"

    cmd = [
        python_bin, str(script),
        "--heart_rate", str(hr_csv),
        "--steps", str(steps_csv),
        "--myphd_id", user_id,
        "--figure", str(figure),
        "--anomalies", str(anomalies),
        "--outliers_fraction", str(outliers_fraction),
        "--random_seed", str(random_seed),
    ]
    if symptom_date: cmd += ["--symptom_date", symptom_date]
    if diagnosis_date: cmd += ["--diagnosis_date", diagnosis_date]
    run(cmd, cwd=str(out_dir))

def run_hrosad(python_bin: str, anomalydetect_dir: Path, out_dir: Path,
               user_id: str, outliers_fraction: float, random_seed: int,
               symptom_date: Optional[str], diagnosis_date: Optional[str]) -> None:
    script = ensure_exists(anomalydetect_dir / "hrosad_offline.py", "hrosad_offline.py")
    hr_csv = ensure_exists(out_dir / "hr.csv", "hr.csv")
    steps_csv = ensure_exists(out_dir / "steps.csv", "steps.csv")

    if not symptom_date or not diagnosis_date:
        s, e = infer_bounds_from_hr(hr_csv)
        symptom_date = symptom_date or s
        diagnosis_date = diagnosis_date or e

    anomalies = out_dir / f"{user_id}_hrosad_anomalies.csv"
    figure = out_dir / f"{user_id}_hrosad_anomalies.pdf"

    cmd = [
        python_bin, str(script),
        "--heart_rate", str(hr_csv),
        "--steps", str(steps_csv),
        "--myphd_id", user_id,
        "--figure", str(figure),
        "--anomalies", str(anomalies),
        "--outliers_fraction", str(outliers_fraction),
        "--random_seed", str(random_seed),
    ]
    if symptom_date: cmd += ["--symptom_date", symptom_date]
    if diagnosis_date: cmd += ["--diagnosis_date", diagnosis_date]
    run(cmd, cwd=str(out_dir))

# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Format CSVs for AnomalyDetect and run rhrad/hrosad.")
    ap.add_argument("--hr-csv", required=True, help="Your minute HR CSV (user,minute,bpm OR datetime,heartrate/bpm/hr)")
    ap.add_argument("--steps-csv", required=True, help="Your minute Steps CSV (user,minute,steps OR datetime,steps)")
    ap.add_argument("--out-dir", required=True, help="Folder to write detector-ready hr.csv/steps.csv and results")
    ap.add_argument("--user-id", default="USER123", help="ID passed to detectors as --myphd_id")
    ap.add_argument("--hr-datetime-format", choices=["mdy_minute", "iso_minute"], default="mdy_minute",
                    help="Output format for hr.csv datetime")
    ap.add_argument("--anomalydetect-dir", required=True,
                    help="Directory containing rhrad_offline.py and hrosad_offline.py")
    ap.add_argument("--detectors-python", default=sys.executable,
                    help="Python interpreter to run detectors (default: current env)")
    ap.add_argument("--outliers", type=float, default=0.1, help="Outliers fraction for both detectors")
    ap.add_argument("--rhrad-random-seed", type=int, default=10)
    ap.add_argument("--hrosad-random-seed", type=int, default=10)
    ap.add_argument("--symptom-date", type=str, default=None)
    ap.add_argument("--diagnosis-date", type=str, default=None)
    ap.add_argument("--format-only", action="store_true", help="Only write hr.csv/steps.csv, do not run detectors")
    return ap.parse_args()

def main():
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print(json.dumps({
        "hr_csv": str(Path(args.hr_csv).resolve()),
        "steps_csv": str(Path(args.steps_csv).resolve()),
        "out_dir": str(out_dir),
        "user_id": args.user_id,
        "hr_datetime_format": args.hr_datetime_format,
        "anomalydetect_dir": str(Path(args.anomalydetect_dir).resolve()),
        "detectors_python": args.detectors_python,
        "outliers": args.outliers,
        "symptom_date": args.symptom_date,
        "diagnosis_date": args.diagnosis_date,
        "format_only": args.format_only,
    }, indent=2))

    # Write detector-ready CSVs (NO 'user' column)
    hr_rows = make_hr_for_detectors(Path(args.hr_csv), out_dir / "hr.csv", args.hr_datetime_format)
    st_rows = make_steps_for_detectors(Path(args.steps_csv), out_dir / "steps.csv")

    # If either is empty, still exit cleanly (detectors will likely fail). Warn instead.
    if hr_rows == 0:
        print("[WARN] hr.csv has 0 rows after cleaning; detectors may fail.")
    if st_rows == 0:
        print("[WARN] steps.csv has 0 rows after cleaning; detectors may fail.")

    if args.format_only:
        print("\n[DONE] Wrote detector-ready hr.csv and steps.csv (format-only).")
        return

    repo = Path(args.anomalydetect_dir).expanduser().resolve()
    ensure_exists(repo, "Anomalydetect dir")
    python_bin = args.detectors_python or sys.executable

    try:
        run_rhrad(python_bin, repo, out_dir, args.user_id,
                  args.outliers, args.rhrad_random_seed,
                  args.symptom_date, args.diagnosis_date)
    except Exception as e:
        print(f"[WARN] rhrad_offline.py failed: {e}")

    try:
        run_hrosad(python_bin, repo, out_dir, args.user_id,
                   args.outliers, args.hrosad_random_seed,
                   args.symptom_date, args.diagnosis_date)
    except Exception as e:
        print(f"[WARN] hrosad_offline.py failed: {e}")

    print("\n[DONE]")
    print(f"Outputs are in: {out_dir}")
    print(" - hr.csv")
    print(" - steps.csv")
    print(f" - {args.user_id}_rhrad_anomalies.csv (and PDF)")
    print(f" - {args.user_id}_hrosad_anomalies.csv (and PDF)")

if __name__ == "__main__":
    main()