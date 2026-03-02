#!/usr/bin/env python3
# laad_from_csvs.py — wrapper to launch LAAD from processed minute CSVs w/ robust preflight

from __future__ import annotations
import argparse, sys, subprocess, tempfile
from pathlib import Path
import pandas as pd
from typing import Tuple

def parse_args():
    ap = argparse.ArgumentParser(description="Run LAAD from processed minute CSVs (with preflight checks).")
    ap.add_argument("--hr", required=True, help="Minute HR CSV (columns: user,minute,bpm OR datetime,heartrate)")
    ap.add_argument("--steps", required=True, help="Minute steps CSV (columns: user,minute,steps OR datetime,steps)")
    ap.add_argument("--symptom-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--user-id", default="USER123", help="myphd_id to pass to LAAD")
    ap.add_argument("--laad-script", default="laad_covid19.py",
                    help="Path to the original LAAD script (default: laad_RHR_keras_v4.py)")
    ap.add_argument("--python-bin", default=sys.executable, help="Python to run LAAD (default: current)")
    ap.add_argument("--strict", action="store_true",
                    help="If set, missing/insufficient inputs cause a non-zero exit. Default: warn and exit 0.")
    ap.add_argument("--synthesize-steps-zeros", action="store_true",
                    help="If set and steps are missing/empty, synthesize steps=0 for each HR minute.")
    ap.add_argument("--output-dir", required=True,
                    help="Directory to write LAAD outputs (CSV summaries, plots, metrics).")
    return ap.parse_args()

def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def _norm_dt_col(df: pd.DataFrame, names: Tuple[str, ...]) -> str | None:
    """Return the first present column name (case-insensitive) from names."""
    if df.empty:
        return None
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n in lower:
            return lower[n]
    return None

def to_laad_hr_schema(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return df
    dt = _norm_dt_col(df, ("datetime", "minute"))
    hr = _norm_dt_col(df, ("heartrate", "bpm", "hr"))
    if not dt or not hr:
        return pd.DataFrame()
    out = df.rename(columns={dt: "datetime", hr: "heartrate"})[["datetime", "heartrate"]].copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out["heartrate"] = pd.to_numeric(out["heartrate"], errors="coerce")
    out = out.dropna(subset=["datetime", "heartrate"])
    if out.empty:
        return out
    # normalize to minute string expected by LAAD script
    out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M")
    return out

def to_laad_steps_schema(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return df
    dt = _norm_dt_col(df, ("datetime", "minute"))
    st = _norm_dt_col(df, ("steps",))
    if not dt or not st:
        return pd.DataFrame()
    out = df.rename(columns={dt: "datetime", st: "steps"})[["datetime", "steps"]].copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out["steps"] = pd.to_numeric(out["steps"], errors="coerce")
    out = out.dropna(subset=["datetime", "steps"])
    if out.empty:
        return out
    out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M")
    return out

def _preflight(hr_df: pd.DataFrame,
               steps_df: pd.DataFrame,
               symptom_date: str) -> Tuple[bool, list[str]]:
    """
    Return (ok, warnings). Minimal requirements for LAAD:
      - HR must exist and have at least some rows.
      - Steps should exist and have at least some rows (LAAD uses a 12-min idle window);
        otherwise the RHR prefilter collapses to empty.
      -  also warn if train coverage before (symptom_date - 20d) looks missing.
    """
    warns: list[str] = []
    ok = True

    if hr_df is None or hr_df.empty:
        warns.append("[WARN] Missing/empty HR minutes: cannot run LAAD.")
        return False, warns

    # Parse hr datetime back for simple coverage checks
    hr_dt = pd.to_datetime(hr_df["datetime"], errors="coerce")
    if hr_dt.notna().sum() == 0:
        warns.append("[WARN] HR datetime parsing failed: cannot run LAAD.")
        return False, warns

    # Steps presence
    if steps_df is None or steps_df.empty:
        warns.append("[WARN] Steps minutes are missing/empty. LAAD’s resting-HR gate will produce an empty set.")
        ok = False

    else:
        st_dt = pd.to_datetime(steps_df["datetime"], errors="coerce")
        if st_dt.notna().sum() == 0:
            warns.append("[WARN] Steps datetime parsing failed. LAAD cannot filter for resting periods.")
            ok = False

    # Coverage before symptom date
    try:
        sd = pd.to_datetime(symptom_date)
        train_cut = sd - pd.Timedelta(days=20)
        have_train = (hr_dt.min() <= train_cut) and (hr_dt.max() >= train_cut)
        if not have_train:
            warns.append("[WARN] HR seems to lack coverage ≥20 days before symptom date; LAAD may fail or be unreliable.")
            # Not fatal; just warn.
    except Exception:
        warns.append(f"[WARN] Could not parse --symptom-date={symptom_date!r} for coverage check.")

    return ok, warns

def _synthesize_steps_from_hr(hr_df: pd.DataFrame) -> pd.DataFrame:
    """Build a steps dataframe (steps=0) for each HR minute."""
    s = hr_df[["datetime"]].copy()
    s["steps"] = 0
    return s[["datetime", "steps"]]

def main():
    args = parse_args()
    hr_in = Path(args.hr)
    steps_in = Path(args.steps)
    laad_py = Path(args.laad_script)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Writing LAAD outputs to {output_dir}")

    if not laad_py.exists():
        msg = f"[ERROR] LAAD script not found: {laad_py}"
        print(msg, file=sys.stderr)
        sys.exit(2 if args.strict else 0)

    # Convert to LAAD schemas (or empty df on failure)
    hr_df = to_laad_hr_schema(hr_in)
    steps_df = to_laad_steps_schema(steps_in)

    ok, warns = _preflight(hr_df, steps_df, args.symptom_date)
    for w in warns:
        print(w)

    # Handle missing steps according to flags
    if not ok:
        if (steps_df is None or steps_df.empty) and args.synthesize_steps_zeros and hr_df is not None and not hr_df.empty:
            print("[INFO] --synthesize-steps-zeros enabled → creating steps=0 for each HR minute.")
            steps_df = _synthesize_steps_from_hr(hr_df)
            ok = True  # allow run with synthesized steps
        else:
            msg = "[SKIP] Crucial inputs missing; not invoking LAAD."
            print(msg)
            # exit code 0 by default (soft skip); non-zero if strict
            sys.exit(2 if args.strict else 0)

    # Final guard: ensure both dfs non-empty now
    if hr_df is None or hr_df.empty:
        print("[SKIP] HR data unavailable after normalization.")
        sys.exit(2 if args.strict else 0)
    if steps_df is None or steps_df.empty:
        print("[SKIP] Steps data unavailable after normalization.")
        sys.exit(2 if args.strict else 0)

    # Fire LAAD
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        hr_tmp = td / "hr_for_laad.csv"
        st_tmp = td / "steps_for_laad.csv"
        hr_df.to_csv(hr_tmp, index=False)
        steps_df.to_csv(st_tmp, index=False)

        cmd = [
            args.python_bin, str(laad_py),
            "--heart_rate", str(hr_tmp),
            "--steps", str(st_tmp),
            "--myphd_id", args.user_id,
            "--symptom_date", args.symptom_date,
            "--output_dir", str(output_dir),
        ]
        print(f"[RUN] {' '.join(cmd)}")
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
        if proc.returncode != 0:
            # bubble up as error only in strict mode; otherwise treat as soft skip
            msg = f"[SKIP] LAAD exited with {proc.returncode}"
            print(msg)
            sys.exit(proc.returncode if args.strict else 0)

if __name__ == "__main__":
    main()