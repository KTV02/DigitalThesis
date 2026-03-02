import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

import requests
from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("UNIFIED_DATA_ROOT", "/data")).resolve()
DATA_ROOT.mkdir(parents=True, exist_ok=True)

MATLAB_CMD = os.environ.get("MATLAB_CMD", "matlab")
DEFAULT_TIMEOUT = int(os.environ.get("MATLAB_JOB_TIMEOUT", "900"))

PROXY_APP = os.environ.get("MATLAB_PROXY_APP", "matlab-proxy-app")
PROXY_CONFIG = os.environ.get("MATLAB_PROXY_CONFIG", "default_configuration_matlab_proxy")
PROXY_INTERNAL_HOST = os.environ.get("MATLAB_PROXY_INTERNAL_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("MWI_APP_PORT", os.environ.get("MATLAB_PROXY_PORT", "9100")))
PROXY_BASE_URL = os.environ.get("MWI_BASE_URL", "/").strip() or "/"
PROXY_BASE_URL = PROXY_BASE_URL if PROXY_BASE_URL.startswith("/") else f"/{PROXY_BASE_URL}"
PROXY_BASE_URL = PROXY_BASE_URL.rstrip("/") or ""

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


@app.errorhandler(HTTPException)
def _handle_http_exception(exc: HTTPException):
    response = jsonify({"error": exc.description or exc.name})
    response.status_code = exc.code or 500
    return response


@app.errorhandler(Exception)
def _handle_unexpected_error(exc: Exception):
    response = jsonify({"error": str(exc)})
    response.status_code = 500
    return response


def _json_error(status: int, message: str):
    response = jsonify({"error": message})
    response.status_code = status
    return response


def _canonical_key(name: str) -> str:
    return name.lstrip("-").replace("_", "-")


def _flag(name: str) -> str:
    return name if name.startswith("-") else f"--{name}"


def _expand_cli_arguments(params: Mapping[str, Any]) -> List[str]:
    args: List[str] = []
    for name, value in params.items():
        flag = _flag(name)
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                args.append(flag)
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                args.extend([flag, str(item)])
            continue
        args.extend([flag, str(value)])
    return args


def _ensure_path(value: str) -> Path:
    path = Path(value).resolve()
    if DATA_ROOT not in path.parents and path != DATA_ROOT:
        raise ValueError(f"Path {path} is outside of shared data root {DATA_ROOT}")
    return path


_proxy_lock = threading.Lock()
_proxy_process: Optional[subprocess.Popen] = None
_proxy_error: Optional[str] = None


def _proxy_base() -> str:
    base = PROXY_BASE_URL
    return base if base else ""


def _ensure_proxy_running() -> None:
    global _proxy_process, _proxy_error
    with _proxy_lock:
        if _proxy_process is not None and _proxy_process.poll() is None:
            return
        _proxy_error = None
        env = os.environ.copy()
        env.setdefault("MWI_APP_PORT", str(PROXY_PORT))
        env.setdefault("MWI_APP_HOST", env.get("MWI_APP_HOST", "0.0.0.0"))
        env.setdefault("MWI_BASE_URL", PROXY_BASE_URL or "/")
        cmd = [PROXY_APP, "--config", PROXY_CONFIG]
        try:
            _proxy_process = subprocess.Popen(cmd, env=env)
        except FileNotFoundError as exc:
            _proxy_process = None
            _proxy_error = f"matlab-proxy executable not found: {exc}"
        except Exception as exc:  # pragma: no cover - defensive
            _proxy_process = None
            _proxy_error = f"Failed to start matlab-proxy: {exc}"


def _fetch_proxy_status() -> Dict[str, Any]:
    ensure_url = f"http://{PROXY_INTERNAL_HOST}:{PROXY_PORT}{_proxy_base()}/get_status"
    response = requests.get(ensure_url, timeout=5)
    response.raise_for_status()
    return response.json()


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/status")
def status():
    _ensure_proxy_running()
    if _proxy_error:
        return _json_error(503, _proxy_error)

    try:
        proxy_state = _fetch_proxy_status()
    except requests.RequestException as exc:
        return _json_error(503, f"MATLAB proxy unavailable: {exc}")

    licensing = proxy_state.get("licensing") or {}
    lic_status = str(licensing.get("status") or licensing.get("state") or "").lower()
    authenticated = bool(licensing) and lic_status not in {
        "", "unknown", "unlicensed", "unauthenticated", "needs_activation", "license_expired"
    }

    payload: Dict[str, Any] = {
        "authenticated": authenticated,
        "proxy": proxy_state,
    }

    email = licensing.get("emailAddress") or licensing.get("email")
    if email:
        payload["email"] = email
    expires = licensing.get("expiryDate") or licensing.get("expiry")
    if expires:
        payload["expires_at"] = expires

    return jsonify(payload)


@app.post("/run/moodml")
def run_moodml():
    payload = request.get_json(force=True, silent=True) or {}
    script_rel = payload.get("script")
    if not script_rel:
        return _json_error(400, "Missing script path")
    script_path = (REPO_ROOT / script_rel).resolve()
    if not script_path.exists():
        return _json_error(404, f"Script not found: {script_rel}")

    params = payload.get("params") or {}
    normalized: MutableMapping[str, Any] = {}
    for key, value in params.items():
        normalized[_canonical_key(key)] = value

    normalized.setdefault("matlab-cmd", MATLAB_CMD)

    job_dir_value = payload.get("job_dir")
    if not job_dir_value:
        return _json_error(400, "Missing job_dir")
    job_dir = _ensure_path(str(job_dir_value))
    job_dir.mkdir(parents=True, exist_ok=True)

    workspace = payload.get("workspace")
    if workspace:
        _ensure_path(str(workspace))

    command: List[str] = [sys.executable, str(script_path)]
    command.extend(_expand_cli_arguments(normalized))

    try:
        proc = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=int(payload.get("timeout", DEFAULT_TIMEOUT)),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        result = {
            "command": command,
            "exit_code": -1,
            "stdout": (exc.stdout or ""),
            "stderr": (exc.stderr or "") + "\nMATLAB job timed out",
            "status": "timeout",
        }
        return jsonify(result), 504

    result = {
        "command": command,
        "exit_code": proc.returncode,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
        "status": "succeeded" if proc.returncode == 0 else "failed",
    }
    return jsonify(result), (200 if proc.returncode == 0 else 422)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "9000"))
    app.run(host="0.0.0.0", port=port)
