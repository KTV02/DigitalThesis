#!/usr/bin/env python3
"""
moodml_from_csvs.py

Make the mood_ml pipeline work from processed CSVs (no DB/XML step).

Inputs (choose ONE): (cares about sleep/wake times not detailed stages)
  --sleep-episodes  path/to/sleep_episodes_merged.csv
  --sleep-stages    path/to/sleep_stages_merged.csv
  
  
  # From project root 
python unified/moodml_from_csvs.py \
  --sleep-episodes ktvfull_interpolated/sleep_episodes_merged.csv \
  --output-dir moodml_out \
  --scripts-dir /path/to/moodml_assets \
  --user-id ktv \
  --longest-per-day \
  --matlab-cmd matlab
  

Both variants produce:  example.csv  with columns:
  date,start_datetime,end_datetime,minutes_sleep,user
  - date: YYYY-MM-DD (local day of sleep onset)
  - start_datetime/end_datetime: "YYYY-MM-DD HH:MM"
  - minutes_sleep: float minutes
  - user: optional label; defaults from --user-id

Then:
  1) Copies Index_calculation.m + mnsd.p into --output-dir (next to example.csv)
  2) Runs MATLAB in batch to produce test.csv
  3) Runs Python predictions to produce expected_outcome_*.csv

"""

from __future__ import annotations
import argparse, os, sys, shutil, time
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import numpy as np
import subprocess

# ---------- utilities ----------

def run(cmd, cwd=None, env=None, check=True):
    print(f"\n[RUN] {' '.join(cmd)}")
    print(f"  CWD: {cwd or os.getcwd()}")
    proc = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.strip())
        if check:
            raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc


def ensure_shared_permissions(base: Path) -> None:
    """Best-effort chmod so the MATLAB proxy user can see freshly created files."""
    try:
        if base.is_dir():
            base.chmod(0o775)
        else:
            base.chmod(0o664)
    except PermissionError:
        return

    if not base.is_dir():
        return

    for path in base.rglob("*"):
        try:
            if path.is_dir():
                path.chmod(0o775)
            else:
                path.chmod(0o664)
        except PermissionError:
            continue

def ensure_file(p: Path, label: str) -> Path:
    if not p.exists():
        raise FileNotFoundError(f"{label} not found: {p}")
    return p

def fmt_minute(ts) -> str:
    # ISO minute: 'YYYY-MM-DD HH:MM'
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M")

def day_of(ts) -> str:
    return pd.to_datetime(ts).date().isoformat()

def write_daysact_shim(out_dir: Path):
    # Avoid MATLAB Financial Toolbox dependency if missing
    shim = out_dir / "daysact.m"
    if shim.exists():
        return
    shim.write_text(
        """function d = daysact(startDate, endDate)
% Minimal DAYSAct replacement (Actual/Actual) used by mnsd.p when toolbox is missing.
    if nargin ~= 2, error('daysact:InvalidInput','Two inputs required.'); end
    if isdatetime(startDate) && isdatetime(endDate)
        d = days(endDate - startDate);
    else
        try
            d = datenum(endDate) - datenum(startDate);
        catch
            s = datetime(startDate); e = datetime(endDate);
            d = days(e - s);
        end
    end
    d = double(d);
end
""",
        encoding="utf-8",
    )

# ---------- example.csv builders ----------

def build_example_from_episodes(csv_path: Path, user: str, longest_per_day: bool) -> pd.DataFrame:
    """
    Accepts 'sleep_episodes_merged.csv'.
    Expected columns (flexible names handled):
        ['user','start','end','minutes_sleep']  (case-insensitive)
    """
    df = pd.read_csv(csv_path)
    cols = {c.lower(): c for c in df.columns}

    # required start/end
    start_col = cols.get("start") or cols.get("start_datetime") or cols.get("start_time")
    end_col   = cols.get("end")   or cols.get("end_datetime")   or cols.get("end_time")
    if not start_col or not end_col:
        raise SystemExit(f"{csv_path} must include start/end timestamps (found: {list(df.columns)})")

    # optional user
    user_col = cols.get("user")
    if user_col is None:
        df["user"] = user
        user_col = "user"

    # minutes
    mins_col = cols.get("minutes_sleep") or cols.get("duration_min") or cols.get("minutes")
    df["start_dt"] = pd.to_datetime(df[start_col], errors="coerce")
    df["end_dt"]   = pd.to_datetime(df[end_col], errors="coerce")
    df = df.dropna(subset=["start_dt","end_dt"]).copy()

    if mins_col:
        df["minutes_sleep"] = pd.to_numeric(df[mins_col], errors="coerce")
    else:
        df["minutes_sleep"] = (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 60.0

    # sanitize
    df = df[df["minutes_sleep"] > 0].copy()
    df["date"] = df["start_dt"].dt.date.astype(str)
    df["start_datetime"] = df["start_dt"].dt.strftime("%Y-%m-%d %H:%M")
    df["end_datetime"]   = df["end_dt"].dt.strftime("%Y-%m-%d %H:%M")

    # optional: longest per local day
    if longest_per_day:
        df["rank"] = df.groupby("date")["minutes_sleep"].rank(method="first", ascending=False)
        df = df[df["rank"] == 1].copy()
        df = df.drop(columns=["rank"])

    out = df[["date","start_datetime","end_datetime","minutes_sleep", user_col]].copy()
    out = out.rename(columns={user_col: "user"})
    out = out.sort_values(["date", "start_datetime"])

    # Provide legacy + MATLAB-friendly column names expected by mnsd.p
    midpoint = df["start_dt"] + (df["end_dt"] - df["start_dt"]) / 2
    time_in_bed_minutes = (
        (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 60.0
    )
    minutes_awake = time_in_bed_minutes - out["minutes_sleep"].astype(float)
    minutes_awake = minutes_awake.clip(lower=0).fillna(0.0)
    for name, values in (
        ("sleep_date", out["date"]),
        ("sleep_start", df["start_dt"].dt.strftime("%Y-%m-%d %H:%M")),
        ("sleep_end", df["end_dt"].dt.strftime("%Y-%m-%d %H:%M")),
        ("sleep_midpoint", midpoint.dt.strftime("%Y-%m-%d %H:%M")),
        ("sleep_duration", out["minutes_sleep"]),
        ("sleep_duration_minutes", out["minutes_sleep"]),
        ("sleep_user", out["user"]),
        ("time_in_bed", time_in_bed_minutes),
        ("time_in_bed_minutes", time_in_bed_minutes),
        ("time_in_bed_hours", time_in_bed_minutes / 60.0),
        ("minutes_awake", minutes_awake),
    ):
        out[name] = values

    return out

def _collapse_stages_to_episodes(df_st: pd.DataFrame,
                                 asleep_labels: List[str]) -> pd.DataFrame:
    """
    Collapse contiguous 'asleep' stages into episodes.
    Expects columns: start, end, stage (case-insensitive; flexible names).
    """
    cols = {c.lower(): c for c in df_st.columns}
    s_col = cols.get("start") or cols.get("start_datetime") or cols.get("start_time")
    e_col = cols.get("end")   or cols.get("end_datetime")   or cols.get("end_time")
    stage_col = cols.get("stage") or cols.get("state") or cols.get("label") or cols.get("type")
    if not s_col or not e_col or not stage_col:
        raise SystemExit("sleep_stages CSV needs start/end/stage columns.")

    df = df_st.rename(columns={s_col:"start", e_col:"end", stage_col:"stage"}).copy()
    df["start_dt"] = pd.to_datetime(df["start"], errors="coerce")
    df["end_dt"]   = pd.to_datetime(df["end"],   errors="coerce")
    df = df.dropna(subset=["start_dt","end_dt"])
    df = df.sort_values("start_dt")

    # normalize stage labels to lowercase for comparison
    df["stage_lc"] = df["stage"].astype(str).str.lower()

    mask = df["stage_lc"].isin([s.lower() for s in asleep_labels])
    df_aslp = df[mask].copy()
    if df_aslp.empty:
        return pd.DataFrame(columns=["start_dt","end_dt","minutes_sleep"])

    # collapse contiguous
    episodes = []
    cur_s = None
    cur_e = None
    last_e = None

    for _, r in df_aslp.iterrows():
        s = r["start_dt"]; e = r["end_dt"]
        if cur_s is None:
            cur_s, cur_e = s, e
            last_e = e
            continue
        # contiguous or touching?
        if s <= last_e:
            # overlap or touch; extend
            cur_e = max(cur_e, e)
            last_e = e
        else:
            episodes.append((cur_s, cur_e))
            cur_s, cur_e = s, e
            last_e = e
    if cur_s is not None:
        episodes.append((cur_s, cur_e))

    ep = pd.DataFrame(episodes, columns=["start_dt","end_dt"])
    ep["minutes_sleep"] = (ep["end_dt"] - ep["start_dt"]).dt.total_seconds() / 60.0
    ep = ep[ep["minutes_sleep"] > 0]
    return ep

def build_example_from_stages(csv_path: Path, user: str, longest_per_day: bool) -> pd.DataFrame:
    """
    Accepts  'sleep_stages_merged.csv' and reconstructs episodes.
    """
    df_st = pd.read_csv(csv_path)
    asleep_labels = ["asleep", "sleep", "core", "deep", "rem", "light"]
    ep = _collapse_stages_to_episodes(df_st, asleep_labels)
    if ep.empty:
        print("[WARN] No asleep episodes reconstructed from stages.")
        return pd.DataFrame(columns=["date","start_datetime","end_datetime","minutes_sleep","user"])

    ep["date"] = ep["start_dt"].dt.date.astype(str)
    ep["start_datetime"] = ep["start_dt"].dt.strftime("%Y-%m-%d %H:%M")
    ep["end_datetime"]   = ep["end_dt"].dt.strftime("%Y-%m-%d %H:%M")
    ep["user"] = user

    if longest_per_day:
        ep["rank"] = ep.groupby("date")["minutes_sleep"].rank(method="first", ascending=False)
        ep = ep[ep["rank"] == 1].drop(columns=["rank"])

    out = ep[["date","start_datetime","end_datetime","minutes_sleep","user"]].copy()
    out = out.sort_values(["date", "start_datetime"])

    midpoint = ep["start_dt"] + (ep["end_dt"] - ep["start_dt"]) / 2
    time_in_bed_minutes = (
        (ep["end_dt"] - ep["start_dt"]).dt.total_seconds() / 60.0
    )
    minutes_awake = time_in_bed_minutes - out["minutes_sleep"].astype(float)
    minutes_awake = minutes_awake.clip(lower=0).fillna(0.0)
    for name, values in (
        ("sleep_date", out["date"]),
        ("sleep_start", ep["start_dt"].dt.strftime("%Y-%m-%d %H:%M")),
        ("sleep_end", ep["end_dt"].dt.strftime("%Y-%m-%d %H:%M")),
        ("sleep_midpoint", midpoint.dt.strftime("%Y-%m-%d %H:%M")),
        ("sleep_duration", out["minutes_sleep"]),
        ("sleep_duration_minutes", out["minutes_sleep"]),
        ("sleep_user", out["user"]),
        ("time_in_bed", time_in_bed_minutes),
        ("time_in_bed_minutes", time_in_bed_minutes),
        ("time_in_bed_hours", time_in_bed_minutes / 60.0),
        ("minutes_awake", minutes_awake),
    ):
        out[name] = values

    return out

# ---------- MATLAB + predictions ----------

def run_matlab_index(scripts_dir: Path, out_dir: Path, matlab_cmd: str):
    idx_m = ensure_file(scripts_dir / "Index_calculation.m", "Index_calculation.m")
    mnsd  = ensure_file(scripts_dir / "mnsd.p", "mnsd.p")
    shutil.copy2(idx_m, out_dir / idx_m.name)
    shutil.copy2(mnsd,  out_dir / mnsd.name)
    write_daysact_shim(out_dir)

    batch = (
        "try, "
        "Index_calculation, "
        "catch ME, disp(getReport(ME,'extended','hyperlinks','off')), exit(1), "
        "end, exit(0)"
    )
    run([matlab_cmd, "-batch", batch], cwd=str(out_dir))


def create_manual_matlab_script(out_dir: Path) -> Path:
    script_path = out_dir / "run_moodml_manual.m"
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    script_path.write_text(
        """%% MoodML manual execution helper
%% Generated automatically by moodml_from_csvs.py
%% Timestamp: {timestamp}
%%
%% How to use:
%%  1. Open this script in MATLAB (the working directory must match this folder).
%%  2. Run the script. It calls Index_calculation.m which reads example.csv and writes test.csv.
%%  3. Once complete, return to the Unified Workbench and run the MoodML finalization step.

here = fileparts(mfilename('fullpath'));
if ~isempty(here)
    cd(here);
end
addpath(here);
clear functions;
rehash;
rehash toolboxcache;
contents = dir(here);
disp('MoodML working directory contents:');
for k = 1:numel(contents)
    disp(['  ', contents(k).name]);
end
disp('Running MoodML Index_calculation.m ...');
Index_calculation;
disp('MoodML MATLAB stage complete. test.csv should now be present in this folder.');
""".format(timestamp=timestamp),
        encoding="utf-8",
    )
    return script_path


def write_manual_instructions(out_dir: Path, script_path: Path) -> Path:
    instructions = out_dir / "MOODML_MANUAL_INSTRUCTIONS.txt"
    text = f"""MoodML manual MATLAB instructions
====================================

This folder contains everything required for the MATLAB portion of the MoodML pipeline.

Required steps:
  1) Open MATLAB and change the working directory to:
     {out_dir}
     (In the MATLAB Proxy UI this folder is visible under the "UnifiedWorkspaces" shortcut.)
  2) Run the generated script:
     {script_path.name}
     This script executes Index_calculation.m which reads example.csv and writes test.csv.
  3) After MATLAB finishes, return to the Unified Workbench UI and run the "Finalize MoodML"
     action to compute the prediction CSV files from test.csv.

Files of interest:
  - example.csv                  Input for MATLAB (already prepared).
  - {script_path.name}           Convenience script to execute in MATLAB.
  - Index_calculation.m          Helper script copied from the repository.
  - mnsd.p                       MATLAB compiled function required by Index_calculation.
  - test.csv                     Output generated by MATLAB (appears after manual execution).

If you regenerate the MoodML preparation step, a new folder may be created and you will need
to repeat the MATLAB execution before finalizing again.
"""
    instructions.write_text(text, encoding="utf-8")
    return instructions

def run_predictions(test_csv: Path, scripts_dir: Path):
    # reuse your earlier notebook-recreation logic, embedded here
    import pickle, numpy as np

    df = pd.read_csv(test_csv)
    if df.shape[1] < 2:
        raise RuntimeError("test.csv has insufficient columns.")
    date_col = df.columns[0]
    X = df.drop(columns=[date_col]).copy()

    # load models
    models = {}
    for tag in ("DE","ME","HME"):
        p = ensure_file(scripts_dir / f"XGBoost_{tag}.pkl", f"XGBoost_{tag}.pkl")
        with open(p, "rb") as f:
            models[tag] = pickle.load(f)

    # try to align features to model’s expected order
    def get_feature_order(m):
        try:
            names = m.get_booster().feature_names
            if names: return list(names)
        except Exception:
            pass
        names = getattr(m, "feature_names_in_", None)
        return list(names) if names is not None and len(names) else None

    exp = get_feature_order(models["DE"])
    if not exp:
        raise RuntimeError("Could not retrieve expected feature names from XGBoost_DE.pkl")
    # Coerce numeric + fill missing
    missing = [c for c in exp if c not in X.columns]
    for m in missing:
        X[m] = 0.0
    X = X[exp].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    def two_col(p):
        a = np.asarray(p)
        if a.ndim == 1:
            a = np.vstack([1.0 - a, a]).T
        return a

    preds = {tag: two_col(models[tag].predict_proba(X)) for tag in ("DE","ME","HME")}

    dates = pd.to_datetime(df[date_col], errors="coerce")
    if dates.isna().all():
        dstr = df[date_col].astype(str)
    else:
        dstr = dates.dt.strftime("%Y-%m-%d")

    out = test_csv.parent
    pd.DataFrame({"date": dstr, "prob_no_de": preds["DE"][:,0], "prob_de": preds["DE"][:,1]}).to_csv(out/"expected_outcome_de.csv", index=False)
    pd.DataFrame({"date": dstr, "prob_no_me": preds["ME"][:,0], "prob_me": preds["ME"][:,1]}).to_csv(out/"expected_outcome_me.csv", index=False)
    pd.DataFrame({"date": dstr, "prob_no_hme": preds["HME"][:,0], "prob_hme": preds["HME"][:,1]}).to_csv(out/"expected_outcome_hme.csv", index=False)

    print("[OK] Wrote expected_outcome_*.csv")

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Run mood_ml from your processed CSVs (no DB/XML).")
    ap.add_argument("--sleep-episodes", help="Path to sleep_episodes_merged.csv")
    ap.add_argument("--sleep-stages", help="Path to sleep_stages_merged.csv")

    ap.add_argument("--output-dir", required=True, help="Folder where example.csv/test.csv/results are written")
    ap.add_argument("--scripts-dir", required=True, help="Folder containing Index_calculation.m, mnsd.p, XGBoost_*.pkl")
    ap.add_argument("--user-id", default="USER123", help="User label to inject into example.csv if missing")
    ap.add_argument("--longest-per-day", action="store_true", help="Keep only the longest episode per local day")
    ap.add_argument("--matlab-cmd", default="matlab", help="MATLAB executable on PATH (used for stage=all)")
    ap.add_argument(
        "--stage",
        choices=["all", "prepare", "finalize"],
        default="all",
        help=(
            "Which portion of the pipeline to run: "
            "'prepare' builds example.csv and creates manual MATLAB helpers, "
            "'finalize' runs predictions assuming test.csv exists, "
            "'all' performs the legacy end-to-end workflow."
        ),
    )
    args = ap.parse_args()

    out_dir = Path(args.output_dir).expanduser().resolve()
    scr_dir = Path(args.scripts_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    stage = args.stage

    if stage in {"prepare", "all"}:
        if not (args.sleep_episodes or args.sleep_stages):
            raise SystemExit("--sleep-episodes or --sleep-stages is required for stage=prepare/all")

        # 1) Build example.csv from your CSVs
        if args.sleep_episodes:
            ex = build_example_from_episodes(Path(args.sleep_episodes), args.user_id, args.longest_per_day)
        else:
            ex = build_example_from_stages(Path(args.sleep_stages), args.user_id, args.longest_per_day)

        if ex.empty:
            raise SystemExit("No sleep episodes found to build example.csv (check inputs).")

        example_csv = out_dir / "example.csv"
        desired_columns = [
            "date",
            "start_datetime",
            "end_datetime",
            "minutes_sleep",
            "user",
            "sleep_date",
            "sleep_start",
            "sleep_end",
            "sleep_midpoint",
            "sleep_duration",
            "sleep_duration_minutes",
            "sleep_user",
            "time_in_bed",
            "time_in_bed_minutes",
            "time_in_bed_hours",
            "minutes_awake",
        ]
        columns = [c for c in desired_columns if c in ex.columns]
        ex = ex[columns]
        ex.to_csv(example_csv, index=False)
        print(f"[OK] Wrote {example_csv}  rows={len(ex):,}")

        # Provide manual MATLAB helpers regardless of stage
        idx_m = ensure_file(scr_dir / "Index_calculation.m", "Index_calculation.m")
        mnsd = ensure_file(scr_dir / "mnsd.p", "mnsd.p")
        shutil.copy2(idx_m, out_dir / idx_m.name)
        shutil.copy2(mnsd, out_dir / mnsd.name)
        write_daysact_shim(out_dir)
        manual_script = create_manual_matlab_script(out_dir)
        instructions = write_manual_instructions(out_dir, manual_script)
        print(f"[OK] Prepared manual MATLAB script: {manual_script}")
        print(f"[OK] Instructions: {instructions}")
        ensure_shared_permissions(out_dir)

        if stage == "prepare":
            print("\n[MANUAL STEP] Open MATLAB, run the generated script, then execute stage=finalize to complete predictions.")
            print("Output directory:", out_dir)
            return

    if stage in {"all"}:
        # 2) MATLAB -> test.csv
        run_matlab_index(scr_dir, out_dir, args.matlab_cmd)
        test_csv = ensure_file(out_dir / "test.csv", "test.csv")
    else:
        test_csv = ensure_file(out_dir / "test.csv", "test.csv")

    # 3) Predictions (only stages all/finalize reach here)
    run_predictions(test_csv, scr_dir)
    ensure_shared_permissions(out_dir)
    print("\n[DONE] Outputs in:", out_dir)

if __name__ == "__main__":
    main()