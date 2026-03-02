#!/usr/bin/env python3
"""HTTP facade for the unified analytics scripts."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid

import requests
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from flask import Flask, Response, abort, jsonify, request, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

REPO_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path(os.environ.get("UNIFIED_DATA_ROOT", "/data")).resolve()
DATA_ROOT.mkdir(parents=True, exist_ok=True)
MATLAB_SERVICE_URL = (os.environ.get("MATLAB_SERVICE_URL", "").strip() or None)
if MATLAB_SERVICE_URL:
    MATLAB_SERVICE_URL = MATLAB_SERVICE_URL.rstrip("/")
MATLAB_BROWSER_URL = os.environ.get("MATLAB_BROWSER_URL", "").strip() or MATLAB_SERVICE_URL
if MATLAB_BROWSER_URL:
    MATLAB_BROWSER_URL = MATLAB_BROWSER_URL.rstrip("/")
AD_PYTHON_BIN = os.environ.get("AD_DETECTORS_PYTHON", "/opt/py36/bin/python")
DEFAULT_USER_ID = os.environ.get("UNIFIED_DEFAULT_USER_ID", "USER123")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024  # 1 GiB uploads

_raw_allowed_origins = os.environ.get("UNIFIED_ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS = [origin.strip() for origin in _raw_allowed_origins.split(",") if origin.strip()]
if not ALLOWED_ORIGINS:
    ALLOWED_ORIGINS = ["*"]

if ALLOWED_ORIGINS == ["*"]:
    cors_resources = {r"/*": {"origins": "*"}}
else:
    cors_resources = {r"/*": {"origins": ALLOWED_ORIGINS}}

CORS(app, resources=cors_resources, expose_headers=["Content-Disposition"])


@dataclass
class ScriptSpec:
    """Metadata describing how to expose a CLI script via HTTP."""

    name: str
    description: str
    script: Path
    path_params: List[str] = field(default_factory=list)
    output_params: List[str] = field(default_factory=list)
    default_output_subdir: str = "outputs"
    defaults: Mapping[str, Any] = field(default_factory=dict)
    executor: str = "local"

    def to_public_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "cli": str(self.script.relative_to(REPO_ROOT)),
            "path_params": self.path_params,
            "output_params": self.output_params,
            "default_output_subdir": self.default_output_subdir,
            "defaults": self.defaults,
        }


def _canonical_key(name: str) -> str:
    return name.lstrip("-").replace("_", "-")


def _flag(name: str) -> str:
    return name if name.startswith("-") else f"--{name}"


def _is_subpath(candidate: Path, parent: Path) -> bool:
    try:
        candidate.relative_to(parent)
        return True
    except ValueError:
        return False


def _resolve_path(value: str | os.PathLike[str], workspace_dir: Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = (workspace_dir / path).resolve()
    else:
        path = path.resolve()
    if _is_subpath(path, workspace_dir) or _is_subpath(path, REPO_ROOT):
        return path
    raise ValueError(f"Path {path} is outside allowed roots ({workspace_dir}, {REPO_ROOT})")


def _normalize_params(raw: Mapping[str, Any] | None) -> MutableMapping[str, Any]:
    norm: MutableMapping[str, Any] = {}
    if not raw:
        return norm
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        norm[_canonical_key(key)] = value
    return norm


def _ensure_workspace(name: str) -> Path:
    workspace = Path(name.strip() or "default")
    safe_name = secure_filename(workspace.as_posix()) or "default"
    ws_dir = DATA_ROOT / safe_name
    ws_dir.mkdir(parents=True, exist_ok=True)
    (ws_dir / "jobs").mkdir(exist_ok=True)
    return ws_dir.resolve()


def _list_files(base: Path, workspace_dir: Path) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    if not base.exists():
        return files
    for path in sorted(base.rglob("*")):
        if path.is_file():
            rel = path.relative_to(workspace_dir)
            files.append(
                {
                    "path": rel.as_posix(),
                    "size": path.stat().st_size,
                    "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                    "download_url": f"/workspaces/{workspace_dir.name}/files/{rel.as_posix()}",
                }
            )
    return files


def _tail(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


class MatlabServiceError(RuntimeError):
    def __init__(self, message: str, status: Optional[int] = None, payload: Optional[Mapping[str, Any]] = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class MatlabServiceNotReady(MatlabServiceError):
    """Raised when the MATLAB backend is not yet authenticated."""


class MatlabClient:
    def __init__(self, base_url: Optional[str]):
        self.base_url = base_url.rstrip('/') if base_url else None

    def _request(self, method: str, path: str, *, timeout: float = 30, **kwargs: Any) -> requests.Response:
        if not self.base_url:
            raise MatlabServiceError("MATLAB service URL is not configured.")
        url = f"{self.base_url}{path}"
        payload: Optional[Mapping[str, Any]] = None
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            raise MatlabServiceError(f"Failed to reach MATLAB service: {exc}") from exc
        if response.status_code >= 400:
            message = (response.text or response.reason or "MATLAB service error").strip()
            content_type = response.headers.get("Content-Type", "")
            if content_type.split(";")[0] == "application/json":
                try:
                    payload = response.json()
                except ValueError:
                    payload = None
                else:
                    message = str(payload.get("error") or payload.get("message") or message)
            raise MatlabServiceError(message, status=response.status_code, payload=payload)
        return response

    def status(self) -> Dict[str, Any]:
        response = self._request("GET", "/status")
        return response.json()

    def ensure_authenticated(self) -> Dict[str, Any]:
        status = self.status()
        if not status.get("authenticated"):
            raise MatlabServiceNotReady("MATLAB container is not authenticated.", status=409)
        return status

    def run_moodml(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        timeout = float(payload.get("timeout", 900))
        response = self._request("POST", "/run/moodml", json=payload, timeout=timeout)
        return response.json()


MATLAB_CLIENT = MatlabClient(MATLAB_SERVICE_URL) if MATLAB_SERVICE_URL else None


def _json_error(status: int, message: str, extra: Optional[Mapping[str, Any]] = None) -> Response:
    payload: Dict[str, Any] = {"error": message}
    if extra:
        payload.update(extra)
    response = jsonify(payload)
    response.status_code = status
    return response


def _forward_matlab_error(exc: MatlabServiceError, context: str) -> Response:
    status_code = exc.status or 502
    if isinstance(exc.payload, Mapping):
        response = jsonify(exc.payload)
        response.status_code = status_code
        return response
    return _json_error(status_code, f"{context}: {exc}")


def _require_matlab_client() -> MatlabClient:
    if MATLAB_CLIENT is None:
        raise MatlabServiceError("MATLAB service URL is not configured.", status=503)
    return MATLAB_CLIENT

SCRIPT_REGISTRY: Dict[str, ScriptSpec] = {
    "apple-export": ScriptSpec(
        name="apple-export",
        description="Convert Apple Health export.xml into raw CSVs aligned with the Google schema.",
        script=REPO_ROOT / "apple_export_raw.py",
        path_params=["xml"],
        output_params=["out-dir"],
        default_output_subdir="apple",
        defaults={"user-id": DEFAULT_USER_ID},
    ),
    "google-export": ScriptSpec(
        name="google-export",
        description="Export raw CSVs from a Google Health Connect SQLite dump.",
        script=REPO_ROOT / "google_export_raw.py",
        path_params=["db"],
        output_params=["out-dir"],
        default_output_subdir="google",
        defaults={"user-id": DEFAULT_USER_ID},
    ),
    "interpolate": ScriptSpec(
        name="interpolate",
        description="Build minute-level series and merged sleep outputs from processed CSVs.",
        script=REPO_ROOT / "interpolate_metrics.py",
        path_params=["in-dir"],
        output_params=["out-dir"],
        default_output_subdir="interpolated",
    ),
    "moodml": ScriptSpec(
        name="moodml",
        description="Run the MoodML MATLAB + Python pipeline from processed sleep CSVs.",
        script=REPO_ROOT / "moodml_from_csvs.py",
        path_params=["sleep-episodes", "sleep-stages", "scripts-dir"],
        output_params=["output-dir"],
        default_output_subdir="moodml",
        defaults={"scripts-dir": str(REPO_ROOT), "user-id": DEFAULT_USER_ID, "stage": "prepare"},
        executor="local",
    ),
    "fips": ScriptSpec(
        name="fips",
        description="Render FIPS plots from sleep episodes CSVs using the bundled R script.",
        script=REPO_ROOT / "fips_from_csv.py",
        path_params=["sleep-csv"],
        output_params=["out-dir"],
        default_output_subdir="fips",
        defaults={"user-id": DEFAULT_USER_ID},
    ),
    "metrics": ScriptSpec(
        name="metrics",
        description="Compute daily metrics (MVPA, resting HR, VO2, sleep efficiency, etc.) from CSV inputs.",
        script=REPO_ROOT / "metrics_from_csvs.py",
        path_params=[
            "sleep-csv",
            "heart-rate-csv",
            "steps-csv",
            "vo2max-csv",
            "resting-hr-csv",
        ],
        output_params=["out-dir"],
        default_output_subdir="metrics",
    ),
    "coverage": ScriptSpec(
        name="coverage",
        description="Compute raw data coverage + sampling frequency metrics from CSV inputs.",
        script=REPO_ROOT / "coverage_from_csvs.py",
        path_params=[
            "heart-rate-csv",
            "hrv-csv",
            "spo2-csv",
            "temp-csv",
            "steps-csv",
            "sleep-csv",
            "resting-hr-csv",
            "vo2max-csv",
        ],
        output_params=["out-dir"],
        default_output_subdir="coverage",
        defaults={"participant": DEFAULT_USER_ID}, 
    ),
    "anomaly": ScriptSpec(
        name="anomaly",
        description="Prepare detector-ready HR/steps CSVs and run RH-RAD + HRoSAD anomaly detection.",
        script=REPO_ROOT / "ad_from_csvs.py",
        path_params=["hr-csv", "steps-csv", "anomalydetect-dir"],
        output_params=["out-dir"],
        default_output_subdir="anomaly",
        defaults={"anomalydetect-dir": str(REPO_ROOT), "user-id": DEFAULT_USER_ID},
    ),
    "laad": ScriptSpec(
        name="laad",
        description="Launch the LAAD resting-heart-rate detector with preflight validation.",
        script=REPO_ROOT / "laad_from_csvs.py",
        path_params=["hr", "steps", "laad-script"],
        output_params=["output-dir"],
        default_output_subdir="laad",
        defaults={"user-id": DEFAULT_USER_ID},
    ),
}


@app.get("/")
def index() -> Response:
    return jsonify(
        {
            "service": "unified-backend",
            "description": "HTTP facade for the unified analytics scripts",
            "data_root": DATA_ROOT.as_posix(),
            "tasks": [spec.to_public_dict() for spec in SCRIPT_REGISTRY.values()],
        }
    )


@app.get("/health")
def health() -> Response:
    return jsonify({"status": "ok"})


@app.get("/matlab/status")
def matlab_status() -> Response:
    try:
        client = _require_matlab_client()
        state = client.status()
    except MatlabServiceError as exc:
        return _forward_matlab_error(exc, "MATLAB status unavailable")
    if MATLAB_BROWSER_URL:
        state = dict(state)
        state.setdefault("browser_url", MATLAB_BROWSER_URL)
    return jsonify(state)


@app.get("/matlab/browser")
def matlab_browser() -> Response:
    if not MATLAB_BROWSER_URL:
        return _json_error(404, "MATLAB browser URL is not configured")
    try:
        client = _require_matlab_client()
        client.status()
    except MatlabServiceError as exc:
        return _forward_matlab_error(exc, "MATLAB browser unavailable")
    return jsonify({"url": MATLAB_BROWSER_URL})


@app.get("/tasks")
def list_tasks() -> Response:
    return jsonify({"tasks": [spec.to_public_dict() for spec in SCRIPT_REGISTRY.values()]})


@app.post("/workspaces/<workspace>/files")
def upload_file(workspace: str):
    workspace_dir = _ensure_workspace(workspace)
    if "file" not in request.files:
        abort(400, "Missing file upload part named 'file'.")
    file_storage = request.files["file"]
    if file_storage.filename is None or file_storage.filename == "":
        abort(400, "Uploaded file is missing a filename.")
    target_name = request.form.get("path") or file_storage.filename
    target_rel = Path(target_name)
    if target_rel.is_absolute():
        abort(400, "Upload path must be relative to the workspace.")
    secure_parts = [secure_filename(part) for part in target_rel.parts if part not in ("", ".")]
    if not secure_parts:
        secure_parts = [secure_filename(file_storage.filename)]
    dest = (workspace_dir / Path(*secure_parts)).resolve()
    if not _is_subpath(dest, workspace_dir):
        abort(400, "Destination outside workspace.")
    dest.parent.mkdir(parents=True, exist_ok=True)
    file_storage.save(dest)
    return (
        jsonify(
            {
                "workspace": workspace_dir.name,
                "path": dest.relative_to(workspace_dir).as_posix(),
                "size": dest.stat().st_size,
            }
        ),
        201,
    )


@app.get("/workspaces")
def list_workspaces() -> Response:
    workspaces = [d.name for d in DATA_ROOT.iterdir() if d.is_dir()]
    return jsonify({"workspaces": sorted(workspaces)})


@app.get("/workspaces/<workspace>/files")
def list_workspace_files(workspace: str):
    workspace_dir = _ensure_workspace(workspace)
    prefix = request.args.get("prefix")
    base = workspace_dir / prefix if prefix else workspace_dir
    base = base.resolve()
    if not _is_subpath(base, workspace_dir):
        abort(400, "Requested prefix escapes workspace root.")
    files = _list_files(base, workspace_dir)
    return jsonify({"workspace": workspace_dir.name, "files": files})


@app.get("/workspaces/<workspace>/files/<path:subpath>")
def download_workspace_file(workspace: str, subpath: str):
    workspace_dir = _ensure_workspace(workspace)
    file_path = (workspace_dir / subpath).resolve()
    if not _is_subpath(file_path, workspace_dir) or not file_path.exists():
        abort(404)
    as_attachment = request.args.get("download", "false").lower() == "true"
    return send_file(file_path, as_attachment=as_attachment, download_name=file_path.name)


@app.get("/workspaces/<workspace>/jobs")
def list_jobs(workspace: str):
    workspace_dir = _ensure_workspace(workspace)
    jobs_dir = workspace_dir / "jobs"
    jobs: List[Dict[str, Any]] = []
    for job_dir in sorted(jobs_dir.iterdir()) if jobs_dir.exists() else []:
        if not job_dir.is_dir():
            continue
        meta_path = job_dir / "job.json"
        if meta_path.exists():
            try:
                data = json.loads(meta_path.read_text(encoding="utf-8"))
                jobs.append({"job_id": data.get("job_id"), "task": data.get("task"), "status": data.get("status")})
            except Exception:
                continue
    return jsonify({"workspace": workspace_dir.name, "jobs": jobs})


@app.get("/workspaces/<workspace>/jobs/<job_id>")
def get_job(workspace: str, job_id: str):
    workspace_dir = _ensure_workspace(workspace)
    job_dir = workspace_dir / "jobs" / secure_filename(job_id)
    meta_path = job_dir / "job.json"
    if not meta_path.exists():
        abort(404)
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    return jsonify(data)


def _prepare_job_environment(workspace_dir: Path, job_id: str, spec: ScriptSpec) -> Path:
    job_dir = workspace_dir / "jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "outputs").mkdir(exist_ok=True)
    return job_dir


def _gather_output_paths(
    spec: ScriptSpec,
    params: Mapping[str, Any],
    workspace_dir: Path,
) -> Dict[str, str]:
    outputs: Dict[str, str] = {}
    for name in spec.output_params:
        value = params.get(name)
        if isinstance(value, list):
            paths = []
            for item in value:
                try:
                    p = Path(item)
                    if p.exists():
                        paths.append(p)
                except Exception:
                    continue
            if paths:
                outputs[name] = ",".join(str(p) for p in paths)
        elif isinstance(value, str):
            p = Path(value)
            if p.exists():
                outputs[name] = str(p)
    return outputs


def _expand_cli_arguments(name: str, value: Any) -> List[str]:
    flag = _flag(name)
    if value is None:
        return []
    if isinstance(value, bool):
        return [flag] if value else []
    if isinstance(value, (list, tuple)):
        args: List[str] = []
        for item in value:
            args.extend([flag, str(item)])
        return args
    return [flag, str(value)]


@app.post("/workspaces/<workspace>/run/<task>")
def run_task(workspace: str, task: str):
    if task not in SCRIPT_REGISTRY:
        abort(404, f"Unknown task '{task}'.")
    spec = SCRIPT_REGISTRY[task]
    workspace_dir = _ensure_workspace(workspace)
    payload = request.get_json(force=True, silent=True) or {}
    params = _normalize_params(payload.get("params"))

    # Merge defaults
    for key, value in spec.defaults.items():
        params.setdefault(_canonical_key(key), value)

    job_id = payload.get("job_id") or uuid.uuid4().hex
    job_id = secure_filename(str(job_id)) or uuid.uuid4().hex
    job_dir = _prepare_job_environment(workspace_dir, job_id, spec)

    # Provide default output directories when not supplied
    for out_name in spec.output_params:
        if out_name not in params:
            default_dir = job_dir / "outputs" / spec.default_output_subdir
            params[out_name] = default_dir.as_posix()

    resolved_params: MutableMapping[str, Any] = {}
    path_like = set(spec.path_params) | set(spec.output_params)

    for name, value in list(params.items()):
        if name in path_like:
            if isinstance(value, (list, tuple)):
                resolved_list = []
                for item in value:
                    resolved_list.append(_resolve_path(item, workspace_dir).as_posix())
                value = resolved_list
            else:
                resolved_path = _resolve_path(value, workspace_dir)
                if name in spec.output_params:
                    resolved_path.mkdir(parents=True, exist_ok=True)
                value = resolved_path.as_posix()
        resolved_params[name] = value

    if spec.name == "anomaly" and "detectors-python" not in resolved_params:
        resolved_params["detectors-python"] = AD_PYTHON_BIN

    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"

    stdout_content = ""
    stderr_content = ""
    exit_code = 1
    command: List[str] = []

    if spec.executor == "matlab":
        try:
            client = _require_matlab_client()
            client.ensure_authenticated()
        except MatlabServiceNotReady as exc:
            abort(exc.status or 409, str(exc))
        except MatlabServiceError as exc:
            abort(exc.status or 502, f"MATLAB status unavailable: {exc}")

        remote_payload = {
            "job_id": job_id,
            "workspace": workspace_dir.as_posix(),
            "job_dir": job_dir.as_posix(),
            "params": resolved_params,
            "script": str(spec.script.relative_to(REPO_ROOT)),
        }
        try:
            result = client.run_moodml(remote_payload)
        except MatlabServiceError as exc:
            abort(exc.status or 502, f"MATLAB execution failed: {exc}")
        stdout_content = result.get("stdout", "")
        stderr_content = result.get("stderr", "")
        command = result.get("command") or ["matlab-service", spec.name]
        if isinstance(command, str):
            command = [command]
        exit_code = int(result.get("exit_code", exit_code))
    else:
        command = [sys.executable, str(spec.script)]
        for name, value in resolved_params.items():
            command.extend(_expand_cli_arguments(name, value))

        proc = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        stdout_content = proc.stdout or ""
        stderr_content = proc.stderr or ""
        exit_code = proc.returncode

    stdout_path.write_text(stdout_content, encoding="utf-8")
    stderr_path.write_text(stderr_content, encoding="utf-8")

    outputs = []
    output_dirs = _gather_output_paths(spec, resolved_params, workspace_dir)
    for path_str in output_dirs.values():
        files = _list_files(Path(path_str), workspace_dir)
        outputs.extend(files)

    outputs.extend(
        [
            {
                "path": stdout_path.relative_to(workspace_dir).as_posix(),
                "size": stdout_path.stat().st_size,
                "modified": datetime.fromtimestamp(stdout_path.stat().st_mtime).isoformat(),
                "download_url": f"/workspaces/{workspace_dir.name}/files/{stdout_path.relative_to(workspace_dir).as_posix()}",
            },
            {
                "path": stderr_path.relative_to(workspace_dir).as_posix(),
                "size": stderr_path.stat().st_size,
                "modified": datetime.fromtimestamp(stderr_path.stat().st_mtime).isoformat(),
                "download_url": f"/workspaces/{workspace_dir.name}/files/{stderr_path.relative_to(workspace_dir).as_posix()}",
            },
        ]
    )

    status = "succeeded" if exit_code == 0 else "failed"

    job_metadata = {
        "job_id": job_id,
        "task": spec.name,
        "workspace": workspace_dir.name,
        "command": command,
        "params": resolved_params,
        "status": status,
        "exit_code": exit_code,
        "stdout_tail": _tail(stdout_content),
        "stderr_tail": _tail(stderr_content),
        "outputs": outputs,
        "output_directories": output_dirs,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    (job_dir / "job.json").write_text(json.dumps(job_metadata, indent=2), encoding="utf-8")

    http_status = 200 if exit_code == 0 else 422
    return jsonify(job_metadata), http_status


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
