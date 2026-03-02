# Unified Analytics Project Overview

## Mission and Scope
- Provide an end-to-end, reproducible pipeline for wearable and sleep analytics, spanning raw export ingestion, signal interpolation, daily metric computation, anomaly detection, R-based visualization, and MATLAB-backed modeling. 【F:README.md†L1-L104】
- Deliver the toolchain as a Dockerized suite with a Flask API facade, a MATLAB delegate, and a React frontend so non-technical users can orchestrate complex analytics from a browser. 【F:README.md†L1-L82】【F:docker-compose.yml†L1-L40】

## System Architecture
- **Containers**: `backend` (Python/R) exposes the API and executes all scripts except MoodML; `matlab` (MathWorks image) runs the MoodML bridge and MATLAB Proxy licensing; `frontend` hosts the Vite/React UI. Shared `/data` volume persists workspaces and job outputs. 【F:docker-compose.yml†L1-L40】【F:README.md†L17-L61】
- **Backend runtime**: Ubuntu base installs Python 3.10, dedicated Python 3.6 venv for anomaly detectors, R with FIPS dependencies, and exposes `app.py` via a lightweight entrypoint. 【F:Dockerfile†L1-L86】【F:Dockerfile†L92-L118】
- **MATLAB delegate**: Extends MathWorks' image, installs Python deps, launches MATLAB Proxy, ensures `/data` is accessible, and exposes a Flask bridge at port 9000 for MoodML execution and licensing checks. 【F:docker/matlab/Dockerfile†L1-L69】【F:docker/matlab/service.py†L1-L200】
- **Frontend**: React app orchestrates uploads, task submission, and result exploration. It initializes workspaces, uploads raw exports, runs interpolation and downstream projects, manages job state, and surfaces MATLAB licensing UX. 【F:frontend/src/App.tsx†L1-L200】【F:frontend/src/components/ProjectConfigurator.tsx†L1-L160】

## Data Management Model
- Workspaces live under `/data/<workspace>` (configurable via `UNIFIED_DATA_ROOT`). Uploads, jobs, and generated outputs are segregated per workspace to isolate analyses. 【F:app.py†L21-L152】【F:app.py†L316-L368】
- Jobs execute within `jobs/<job_id>` folders; metadata (`job.json`), stdout/stderr logs, and output directories are persisted for retrieval. Output directory defaults (`outputs/<task>` subdirs) are auto-created when not provided. 【F:app.py†L368-L488】【F:app.py†L488-L560】
- The API enforces path safety (`_resolve_path`, `_is_subpath`) ensuring scripts can only interact with workspace or repo files. 【F:app.py†L78-L132】

## HTTP API Facade (`app.py`)
- Registers each CLI analytics script via `ScriptSpec`, mapping HTTP tasks to Python/R/Matlab executables with path/output parameter metadata and defaults. 【F:app.py†L44-L252】
- Exposes routes to list tasks, manage workspaces, upload/download files, track jobs, and execute tasks. All requests normalize CLI flags, merge defaults, and materialize output directories before invoking the script. 【F:app.py†L252-L432】【F:app.py†L488-L560】
- For local tasks, runs the script via `subprocess.run` using the repo’s Python interpreter, capturing stdout/stderr. For MoodML, delegates to the MATLAB service after verifying proxy authentication. 【F:app.py†L488-L560】【F:app.py†L196-L344】
- Collects output inventories by scanning requested output paths and attaches logs to the job metadata returned to clients. 【F:app.py†L512-L560】

## MATLAB Delegation Workflow
- MATLAB bridge enforces licensing readiness (`/status`), ensures MATLAB Proxy is running, and surfaces email/expiry metadata to the backend/front-end. 【F:docker/matlab/service.py†L96-L200】
- `/run/moodml` executes `moodml_from_csvs.py` inside the shared repo using Python, passing normalized parameters and capturing exit status/logs; timeouts produce 504 responses. 【F:docker/matlab/service.py†L200-L240】
- Backend’s MoodML task auto-wires defaults (`scripts-dir`, `stage`, `user-id`) and ensures the MATLAB container is authenticated before dispatching. 【F:app.py†L216-L304】【F:app.py†L344-L432】

## Core Analytics Pipelines
1. **Raw Data Exporters**
   - `apple_export_raw.py`: Streams HealthKit `export.xml`, filters by source/device, constructs sleep episodes/stages, and writes schema-aligned CSVs (HR, steps, HRV, RR intervals, VO₂, body temperature) with naive local timestamps. Handles restless time aggregation and stage merging logic. 【F:apple_export_raw.py†L1-L200】【F:apple_export_raw.py†L52-L200】
   - `google_export_raw.py`: Reads Health Connect SQLite, normalizes epoch units, optionally filters by app ID, resolves sleep sessions/stages relationships, and emits CSVs parallel to Apple exporter. 【F:google_export_raw.py†L1-L200】

2. **Interpolation & Merging**
   - `interpolate_metrics.py`: Harmonizes raw CSVs into minute-level series, merges adjacent sleep episodes/stages, interpolates HR/HRV/body temperature via configurable strategies, handles step interval overlap resolution, and produces daily VO₂ estimates. 【F:interpolate_metrics.py†L1-L160】

3. **Daily Metrics**
   - `metrics_from_csvs.py`: Loads individual CSV inputs, normalizes schemas, computes MVPA (steps or HR-based), resting HR, VO₂ (native or HR-derived), sleep efficiency, sunrise deviation, and HR cosinor metrics, writing CSV outputs plus a `summary.json`. 【F:metrics_from_csvs.py†L1-L160】

4. **Anomaly Detection**
   - `ad_from_csvs.py`: Preflights minute HR/steps data, rewrites them to detector schemas, optionally invokes RH-RAD and HRoSAD offline detectors using the bundled Python 3.6 environment, and aggregates their results. 【F:ad_from_csvs.py†L1-L160】
   - `rhrad_offline.py` & `hrosad_offline.py`: Legacy Stanford scripts that compute resting-HR anomalies using seasonal decomposition, Z-score standardization, and EllipticEnvelope models, outputting plots/CSV anomaly lists. 【F:rhrad_offline.py†L1-L40】【F:hrosad_offline.py†L1-L120】

5. **FIPS Visualization**
   - `fips_from_csv.py` + `fips_run.R`: Python wrapper validates inputs then launches R script, which installs dependencies on demand, rounds sleep episodes to 5-minute grids, generates FIPS barcodes and unified-model plots, and saves timeline CSVs. 【F:fips_from_csv.py†L1-L60】【F:fips_run.R†L1-L160】

6. **LAAD Wrapper**
   - `laad_from_csvs.py`: Validates HR/steps CSVs, reshapes them to LAAD schemas, warns on coverage gaps, optionally synthesizes zero steps, and executes the bundled `laad_covid19.py` script with user-configurable strictness. 【F:laad_from_csvs.py†L1-L120】

7. **MoodML Pipeline**
   - `moodml_from_csvs.py`: Accepts merged sleep episodes or stages, constructs MATLAB-ready example CSVs, copies MATLAB assets (`Index_calculation.m`, `mnsd.p`), injects compatibility shims (e.g., `daysact.m`), runs MATLAB batch jobs, and post-processes outputs into prediction CSVs. Ensures cross-container permissions. 【F:moodml_from_csvs.py†L1-L120】

## Frontend User Experience
- Guides users through three phases: uploading raw exports (`UploadCard`), running interpolation (`InterpolationForm`), and selecting downstream projects (`ProjectConfigurator`). Maintains job progress, surfaces errors, and refreshes workspace file listings. 【F:frontend/src/App.tsx†L1-L200】【F:frontend/src/components/ProjectConfigurator.tsx†L1-L160】
- Provides job logs (`JobLog`), data exploration tabs (`ResultsTabs`, `DataExplorer`), questionnaire placeholders, and MATLAB licensing helpers (fetches `/matlab/browser`, exposes call-to-action when manual MoodML stage requires browser authentication). 【F:frontend/src/App.tsx†L160-L320】

### Questionnaire validation (PSQI)
- The questionnaire panel requires operators to bracket a timeframe and upload a PSQI CSV; it persists normalized responses, uploads the artifact into `questionnaires/psqi/`, and immediately re-runs the evaluation loop so that comparisons reflect the freshly ingested answers. Status messaging communicates missing prerequisites, upload progress, and the outcome of the biometric audit. 【F:frontend/src/components/QuestionnairesSection.tsx†L998-L1106】【F:frontend/src/components/QuestionnairesSection.tsx†L1052-L1098】
- During evaluation the frontend scans the workspace for prerequisite datasets—raw and merged sleep episodes, metrics outputs, and FIPS visualizations—parses them, filters records to the requested window, and annotates availability cards so investigators know which biometric evidence underpins each comparison. 【F:frontend/src/components/QuestionnairesSection.tsx†L360-L493】【F:frontend/src/components/QuestionnairesSection.tsx†L542-L639】
- Context charts are generated directly from the metrics CSVs for the selected window: sunrise-aligned sleep timing deviations, resting heart rate, and sleep efficiency/restlessness. These datasets are plotted alongside explanatory copy referencing their precise CSV sources, giving reviewers an at-a-glance sense of longitudinal patterns during the PSQI interval. 【F:frontend/src/components/QuestionnairesSection.tsx†L600-L742】
- Structured comparisons reconcile each PSQI response with objective measures: median onset/offset of longest merged episodes vs. bedtime/wake time responses; nightly sleep minutes vs. reported duration; awakenings counted from raw episode gaps vs. self-reported wake-ups; and sleep efficiency contextualizing the ordinal quality score. Thresholds (±30 minutes for times, ±0.5 hours for duration, exact match for awakenings) classify each row as match/mismatch/unavailable so discrepancies surface immediately. 【F:frontend/src/components/QuestionnairesSection.tsx†L756-L899】
- Qualitative evidence about daytime alertness merges FIPS TMP/TPM images and timeline CSVs: the component correlates chart filenames, job IDs, and timeline date coverage to confirm alignment with the questionnaire window, generating narrative notes that explain why an image supports or fails to support the PSQI claim. 【F:frontend/src/components/QuestionnairesSection.tsx†L600-L639】【F:frontend/src/components/QuestionnairesSection.tsx†L901-L935】

## Configuration & Defaults
- Environment variables: `UNIFIED_DATA_ROOT`, `UNIFIED_ALLOWED_ORIGINS`, `UNIFIED_DEFAULT_USER_ID`, MATLAB proxy URLs, anomaly detector Python path override, etc. 【F:app.py†L21-L116】
- Backend auto-sanitizes filenames, enforces 1 GiB upload limit, and normalizes CLI flag keys (hyphen/underscore tolerant). 【F:app.py†L33-L132】【F:app.py†L252-L320】
- Default output directories and user IDs reduce required input; clients can override paths to reuse previous outputs. 【F:app.py†L200-L320】【F:app.py†L432-L488】

## Deployment & Operations
- Docker Compose builds images, optionally pins MATLAB release, configures license hints, and maps host `runtime/` to `/data` for persistence. 【F:README.md†L61-L140】【F:docker-compose.yml†L1-L40】
- Backend entrypoint exposes Flask on port 8000; MATLAB service listens on 9000 and proxies licensing UI on 9100. Frontend served via Vite dev server on 5173. 【F:docker-compose.yml†L1-L40】【F:docker/entrypoint.sh†L1-L20】

## Extensibility Notes
- New scripts can be surfaced by adding `ScriptSpec` entries; path resolution and output harvesting automatically integrate them into workspace/job tracking. 【F:app.py†L188-L320】【F:app.py†L432-L520】
- Frontend project configuration is data-driven; adding a new backend task requires corresponding UI toggles and parameter bindings. 【F:frontend/src/components/ProjectConfigurator.tsx†L1-L160】

## Key Assets & Dependencies
- Bundled analytical assets include the MATLAB driver `Index_calculation.m` for MoodML post-processing and the standalone LAAD neural anomaly detector implementation `laad_covid19.py`. 【F:Index_calculation.m†L1-L5】【F:laad_covid19.py†L1-L20】
- Python requirements cover scientific stack (pandas, numpy, scipy optional, TensorFlow CPU) and Flask; MATLAB delegate installs bridging libs defined in `docker/matlab/requirements.txt`. 【F:Dockerfile†L60-L110】【F:docker/matlab/Dockerfile†L40-L69】
- R dependencies (FIPS) bootstrapped at build time and revalidated inside script for reproducibility. 【F:Dockerfile†L104-L112】【F:fips_run.R†L1-L60】

