#!/usr/bin/env python3
"""
fips_from_csv.py

Usage:
  python fips_from_csv.py \
    --sleep-csv path/to/sleep_episodes.csv \
    --out-dir out/plots \
    --user-id USER
"""

import argparse
import subprocess
from pathlib import Path
import os

def run(cmd):
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sleep-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--user-id", default="USER123")
    args = ap.parse_args()

    sleep_csv = Path(args.sleep_csv).resolve()
    out_dir   = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    r_script = Path(__file__).with_name("fips_run.R").resolve()
    if not r_script.exists():
        raise SystemExit(f"Cannot find fips_run.R at {r_script}")

    run([
        "Rscript", str(r_script),
        "--sleep_csv", str(sleep_csv),
        "--out_dir", str(out_dir),
        "--user_id", args.user_id
    ])

    print("\n[FIPS DONE]")
    print(f"Plots in: {out_dir}")

if __name__ == "__main__":
    main()