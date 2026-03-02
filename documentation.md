# Unified Analytics Backend (Python/R + MATLAB delegate)

This project provides a Dockerized backend for the wearable and sleep analytics scripts in this repository. The stack is split
into two cooperative containers:

- **`backend`** – Ubuntu-based image with Python 3, a dedicated Python 3.6 virtual environment for the anomaly detectors, and R.
  It exposes a Flask API for uploading data, launching jobs, and retrieving results for every script except the MATLAB-heavy
  MoodML pipeline.
- **`matlab`** – MathWorks' official MATLAB image with the same Python dependencies. It handles MoodML execution and owns the
  MATLAB licensing workflow via an HTTP bridge.

The services share a `/data` volume so intermediate CSVs and final outputs are available to both containers and to the host.

## Prerequisites

- Docker Engine 20.10+ (or Docker Desktop on macOS/Windows)
- Access to MathWorks' Docker registry (`docker login registry.mathworks.com`) in order to build the MATLAB delegate image
- A valid MATLAB license 

> **Apple Silicon (ARM64) hosts:** MathWorks currently publishes MATLAB images for `linux/amd64` only. Set
> `DOCKER_DEFAULT_PLATFORM=linux/amd64` when building or running the MATLAB container so Docker Desktop performs x86_64
> emulation automatically.

## Building the containers

```bash
# Optional: choose a MATLAB release for the delegate (defaults to r2023b)
export MATLAB_RELEASE=r2023b

# Build both images
DOCKER_DEFAULT_PLATFORM=${DOCKER_DEFAULT_PLATFORM:-linux/amd64} docker compose build
```

The backend image installs:

- Python 3.10 runtime with dependencies from `requirements.txt` (including the CPU-only TensorFlow build to keep the image
  size manageable)
- A separate Python 3.6 virtual environment at `/opt/py36` with the detector packages from `requirements-ad.txt`
- R plus the packages required by `fips_run.R` (including the FIPS package from
  [`humanfactors/FIPS`](https://github.com/humanfactors/FIPS))
- All repository scripts and the Flask service at `/app/app.py`

The MATLAB delegate image installs:

- MATLAB (from MathWorks' base image)
- The Python dependencies required by `moodml_from_csvs.py` and the Flask licensing bridge (pinned in `docker/matlab/requirements.txt`)

## Running with Docker Compose

```bash
mkdir -p runtime

cat <<ENV > .env
MWI_LICENSE_FILE=27000@licenseserver.example.com
# MATLAB_LICENSE_FILE=/licenses/license.lic
# MLM_LICENSE_FILE=/licenses/license.lic
ENV

docker compose up

# Apple Silicon users should continue to force x86_64 when starting the stack
# DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose up
```

Set `UNIFIED_ALLOWED_ORIGINS` (comma-separated list) if your frontend runs on a different origin and needs browser access to
the API. The default (`*`) enables CORS for any origin when developing locally.

Compose starts:

- `backend` on port `8000`
- `matlab` on port `9000` for the job bridge and `9100` for the MATLAB Proxy licensing UI

Both services mount `./runtime` as `/data`, so uploads, intermediate CSVs, job logs, and MATLAB outputs persist on the host.
The project directory is also bind-mounted into `/app` for iterative development.

The MATLAB delegate now boots the job bridge alongside MathWorks' MATLAB Proxy server. Use the proxy in a browser to complete the
online licensing handshake whenever MathWorks expires the cached state.

## MATLAB licensing in the browser

The backend refuses to run MoodML jobs until the MATLAB delegate reports an authenticated MATLAB Proxy session. The MATLAB
container exposes MathWorks' proxy at [http://localhost:9100](http://localhost:9100) by default (override the external URL by
setting `MATLAB_BROWSER_URL` for the backend service).

To license MATLAB:

1. Open the proxy in your browser – either click the *Open MATLAB authentication* button that appears in the frontend when you
   select the MoodML project, or visit [http://localhost:9100](http://localhost:9100) directly.
2. Sign in with your MathWorks Account and complete the standard licensing prompts. MATLAB Proxy persists its state under
   `./runtime/.matlab-proxy`, so the login normally survives container restarts.
3. Confirm the container is licensed:
   ```bash
   curl http://localhost:8000/matlab/status | jq
   ```
   Once MATLAB Proxy reports an active license the payload includes `"authenticated": true`.

When the proxy launches MATLAB it now switches the *Current Folder* to `/data` (also exposed as the `UnifiedWorkspaces`
shortcut in the file browser). Freshly generated job directories therefore appear immediately without rebuilding the
image; hitting the refresh button will rescan the shared volume if needed.


## API overview

The backend service (port 8000) exposes the following endpoints:

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/health` | Liveness probe |
| `GET` | `/` | Service metadata + available tasks |
| `GET` | `/tasks` | Detailed task catalog (script descriptions, default flags) |
| `GET` | `/matlab/status` | Report the MATLAB Proxy licensing status |
| `GET` | `/matlab/browser` | Return the URL the frontend should open for MATLAB Proxy |

The MATLAB endpoints return structured JSON errors; for example, submitting an expired OTP returns an HTTP `409` with the latest transcript so you can request a new code and retry.
| `POST` | `/workspaces/<name>/files` | Upload a file (`multipart/form-data` field `file`) to the workspace |
| `GET` | `/workspaces` | List workspace names |
| `GET` | `/workspaces/<name>/files?prefix=...` | List files inside a workspace (optionally under a prefix) |
| `GET` | `/workspaces/<name>/files/<path>` | Download a file (`?download=true` forces attachment) |
| `GET` | `/workspaces/<name>/jobs` | List jobs and their status |
| `GET` | `/workspaces/<name>/jobs/<job_id>` | Retrieve full metadata for a job |
| `POST` | `/workspaces/<name>/run/<task>` | Execute one of the bundled scripts |

### Available tasks

- `apple-export` → `apple_export_raw.py`
- `google-export` → `google_export_raw.py`
- `interpolate` → `interpolate_metrics.py`
- `moodml` → `moodml_from_csvs.py` (delegated to the MATLAB container; requires prior authentication)
- `fips` → `fips_from_csv.py`
- `metrics` → `metrics_from_csvs.py`
- `anomaly` → `ad_from_csvs.py` (automatically wires `--detectors-python` to the bundled Python 3.6 environment)
- `laad` → `laad_from_csvs.py`

All CLI flags are exposed via the `params` object in the JSON payload. Flags can be supplied with underscores or hyphens; the
service normalizes them for the underlying script. Tasks that accept a `user_id`/`user-id` flag default to `USER123` when it is
not provided. Output directories default to job-scoped subfolders when omitted.

## Sample end-to-end workflows

The examples below show how to exercise the full pipeline with HTTP requests alone. They assume the Compose stack is running on
`localhost`, the backend API is available on port `8000`, and your working directory contains the raw export you want to
process. Workspace directories are created automatically the first time you upload a file or run a job against them.

Set a helper variable for the workspace name:

```bash
export WS=demo
```

### Apple Health XML → analytics

1. **Upload the Apple Health export.** Unzip Apple’s export bundle locally and point the upload to `export.xml` :

   ```bash
   curl -X POST "http://localhost:8000/workspaces/$WS/files" \
     -F "path=inputs/apple/export.xml" \
     -F "file=@/absolute/path/to/export.xml"
   ```

2. **Convert the XML into raw CSVs.** Provide the workspace-relative path to the XML along with the desired output folder.
   Supplying a `user_id` is optional—if you omit it, the backend automatically injects `USER123`. The backend resolves paths
   within the workspace and creates the output directory if needed.

   ```bash
   curl -X POST "http://localhost:8000/workspaces/$WS/run/apple-export" \
     -H "Content-Type: application/json" \
     -d '{
           "params": {
             "xml": "inputs/apple/export.xml",
             "out_dir": "datasets/apple/raw",
             "user_id": "USER123",
             "start": "2025-01-01",
             "end": "2025-02-01"
           }
         }'
   ```

3. **Interpolate minute-level series.** Point the interpolator at the raw output directory and choose a destination for the
   processed CSVs.

   ```bash
   curl -X POST "http://localhost:8000/workspaces/$WS/run/interpolate" \
     -H "Content-Type: application/json" \
     -d '{
           "params": {
             "in_dir": "datasets/apple/raw",
             "out_dir": "datasets/apple/interpolated",
             "start": "2025-01-01",
             "end": "2025-02-01"
           }
         }'
   ```

4. **Launch downstream projects.** Every task accepts workspace-relative paths to its required CSVs. For example:

   ```bash
   # Daily metrics summary
  curl -X POST "http://localhost:8000/workspaces/$WS/run/metrics" \
    -H "Content-Type: application/json" \
    -d '{
          "params": {
            "sleep_csv": "datasets/apple/interpolated/sleep_episodes_merged.csv",
            "heart_rate_csv": "datasets/apple/interpolated/heart_rate_minute.csv",
            "steps_csv": "datasets/apple/interpolated/steps_minute.csv",
            "vo2max_csv": "datasets/apple/interpolated/vo2max_daily.csv",
            "resting_hr_csv": "datasets/apple/interpolated/heart_rate_minute.csv",
            "out_dir": "reports/apple/metrics",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "lat": 37.7749,
            "lon": -122.4194
          }
        }'

   # Anomaly detectors (Python 3.6 environment wired automatically)
   curl -X POST "http://localhost:8000/workspaces/$WS/run/anomaly" \
     -H "Content-Type: application/json" \
     -d '{
           "params": {
             "hr_csv": "datasets/apple/interpolated/heart_rate_minute.csv",
             "steps_csv": "datasets/apple/interpolated/steps_minute.csv",
             "out_dir": "reports/apple/anomaly"
           }
         }'

   # LAAD resting heart rate detector
   curl -X POST "http://localhost:8000/workspaces/$WS/run/laad" \
     -H "Content-Type: application/json" \
     -d '{
           "params": {
             "steps": "datasets/apple/interpolated/steps_minute.csv",
             "hr": "datasets/apple/interpolated/heart_rate_minute.csv",
             "symptom_date": "2025-09-25",
             "laad_script": "laad_covid19.py",
             "output_dir": "reports/apple/laad"
           }
         }'

   # FIPS plots from the merged sleep episodes
   curl -X POST "http://localhost:8000/workspaces/$WS/run/fips" \
     -H "Content-Type: application/json" \
     -d '{
           "params": {
             "sleep_csv": "datasets/apple/interpolated/sleep_episodes_merged.csv",
             "out_dir": "reports/apple/fips"
           }
         }'
   ```

   After you have manually licensed MATLAB (for example by logging into the delegate container and running the browser-based
   flow), you can trigger MoodML with:

   ```bash
   curl -X POST "http://localhost:8000/workspaces/$WS/run/moodml" \
     -H "Content-Type: application/json" \
     -d '{
           "params": {
             "sleep_episodes": "datasets/apple/interpolated/sleep_episodes_merged.csv",
             "output_dir": "reports/apple/moodml"
           }
         }'
   ```

5. **Inspect jobs and download artifacts.** Use the job APIs to retrieve logs and outputs or list/download files directly:

   ```bash
   # List jobs and locate the job_id of interest
   curl "http://localhost:8000/workspaces/$WS/jobs" | jq

   # Examine a specific job's metadata (paths, exit code, stdout/stderr)
   curl "http://localhost:8000/workspaces/$WS/jobs/<job_id>" | jq

   # Browse or download any file produced during the run
   curl "http://localhost:8000/workspaces/$WS/files?prefix=reports" | jq
   curl -L "http://localhost:8000/workspaces/$WS/files/reports/apple/metrics/metrics_summary.csv" -o metrics_summary.csv
   ```

### Google Health Connect DB → analytics

The Google exporter works the same way, but the upload targets the SQLite dump instead of an XML file:

```bash
# Upload the Health Connect database
curl -X POST "http://localhost:8000/workspaces/$WS/files" \
  -F "path=inputs/google/health_connect.db" \
  -F "file=@/absolute/path/to/health_connect.db"

# Convert to raw CSVs (the optional user_id defaults to USER123 when omitted)
curl -X POST "http://localhost:8000/workspaces/$WS/run/google-export" \
  -H "Content-Type: application/json" \
  -d '{
        "params": {
          "db": "inputs/google/health_connect.db",
          "out_dir": "datasets/google/raw",
          "user_id": "USER123",
          "start": "2025-01-01",
          "end": "2025-02-01"
        }
      }'

# Continue with interpolate/metrics/anomaly/etc. by pointing to the same
# workspace-relative directories used in the Apple example (replace
# datasets/apple/... with datasets/google/... as appropriate).
```

### Launching jobs

Example: run the interpolator, anomaly detectors, and MoodML end-to-end inside workspace `demo`.

```bash
# Upload CSVs first (repeat for each required file)
curl -X POST http://localhost:8000/workspaces/demo/files \
  -F "file=@data/sleep_episodes_merged.csv"

# Interpolate
curl -X POST http://localhost:8000/workspaces/demo/run/interpolate \
  -H 'Content-Type: application/json' \
  -d '{"params": {"in-dir": "files", "out-dir": "interpolated"}}'

# Anomaly detectors (uses the preconfigured Python 3.6 detectors environment)
curl -X POST http://localhost:8000/workspaces/demo/run/anomaly \
  -H 'Content-Type: application/json' \
  -d '{"params": {"hr-csv": "interpolated/hr.csv", "steps-csv": "interpolated/steps.csv", "anomalydetect-dir": "."}}'

# MoodML (authenticate once via http://localhost:9100 before running)
curl -X POST http://localhost:8000/workspaces/demo/run/moodml \
  -H 'Content-Type: application/json' \
  -d '{"params": {"sleep-episodes": "interpolated/sleep_episodes_merged.csv", "output-dir": "moodml", "scripts-dir": "."}}'
```

Each request returns job metadata, including the normalized command, stdout/stderr tails, exit code, and discovered outputs.
Logs and generated files can be downloaded from `/workspaces/<workspace>/files/...`.

## Storage layout and workspaces

Every workspace lives under `/data` (mapped to `./runtime` via Compose):

```
/data/
  demo/
    files/                 # user uploads
    jobs/
      <job-id>/
        job.json           # metadata
        stdout.log
        stderr.log
        outputs/
          ...
```

Workspaces isolate concurrent runs or team members. If no name is provided the backend falls back to `default`.

## Anomaly detector compatibility

The anomaly detection wrapper (`ad_from_csvs.py`) expects some legacy detectors that only support Python 3.6. The backend
keeps a dedicated virtual environment at `/opt/py36` with compatible versions of NumPy, pandas, SciPy, scikit-learn, and
matplotlib. The HTTP endpoint injects `--detectors-python /opt/py36/bin/python` automatically so callers do not have to manage
multiple interpreters manually.

## Extending the deployment

- Set `UNIFIED_DATA_ROOT` on either container if you want to mount a different host directory.
- Override `MATLAB_SERVICE_URL` on the backend if you expose the MATLAB container under a different hostname.
- Increase `MATLAB_AUTH_TIMEOUT` on the backend when slow MATLAB startups require more than the default 5 minutes to finish the online licensing prompts.
- Export `MATLAB_JOB_TIMEOUT` on the delegate to tweak the default 15 minute execution window for MoodML jobs.

With the split architecture you can scale or restart the backend and MATLAB services independently while sharing inputs and
outputs through the common `/data` volume.

### Web frontend (React)

The Compose stack includes a third container named `frontend` listening on port `5173`. It ships a React single-page app
that orchestrates the full workflow:

1. Split landing screen: upload Apple Health (`export.xml`) or Google Health Connect (`health_connect_export.db`) archives.
   Each half lets you enter the export window, user ID, and optional source filter before selecting the file. Uploading kicks off
   the matching exporter in the backend and streams status updates to the activity log.
2. Interpolation step: every CLI flag exposed by `interpolate_metrics.py` is configurable via dropdowns, number inputs, and
   checkboxes. Pick the export output directory, adjust deduplication/aggregation options, select interpolation methods per
   signal, and run the harmonizer.
3. Project stage: toggle the downstream pipelines (FIPS, Metrics, Anomaly, LAAD, MoodML). For each project you can choose which
   CSVs to feed in (raw vs interpolated), set thresholds (e.g., MVPA cadence, anomaly detector outliers, LAAD symptom date), and
   provide optional metadata like latitude/longitude. MoodML automatically prompts for MATLAB licensing if the delegate is not
   authenticated.
4. Monitoring + insights: the activity log lists every job with stdout/stderr tails and download links. A results explorer groups
   CSV outputs by task and previews them inline, while the data explorer overlays multiple metrics on a time-series chart with
   zoomable axes.

Open http://localhost:5173 after `docker compose up` to access the UI. The frontend automatically talks to the Flask API via the
internal service name (`http://backend:8000`). For standalone development you can run `npm install && npm run dev` inside
`frontend/` and point `VITE_BACKEND_URL` at a locally running backend instance.
