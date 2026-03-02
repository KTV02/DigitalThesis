export const BACKEND_URL = (import.meta.env.VITE_BACKEND_URL || 'http://localhost:8000').replace(/\/$/, '');

export interface WorkspaceFile {
  path: string;
  size: number;
  modified: string;
  download_url: string;
}

export interface TaskMetadata {
  name: string;
  description: string;
  cli: string;
  path_params: string[];
  output_params: string[];
  default_output_subdir: string;
  defaults: Record<string, unknown>;
}

export interface JobResponse {
  job_id: string;
  task: string;
  workspace: string;
  command: string[];
  params: Record<string, unknown>;
  status: string;
  exit_code: number;
  stdout_tail: string;
  stderr_tail: string;
  outputs: WorkspaceFile[];
  output_directories: Record<string, string>;
  created_at: string;
}

function check(response: Response) {
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }
  return response;
}

export async function ensureWorkspace(name?: string): Promise<string> {
  const existing = name || window.localStorage.getItem('unified-workspace');
  if (existing) {
    window.localStorage.setItem('unified-workspace', existing);
    return existing;
  }
  const generated = `ws-${crypto.randomUUID()}`;
  window.localStorage.setItem('unified-workspace', generated);
  return generated;
}

export async function listTasks(): Promise<TaskMetadata[]> {
  const res = await fetch(`${BACKEND_URL}/tasks`);
  const payload = await check(res).json();
  return payload.tasks as TaskMetadata[];
}

export async function listWorkspaceFiles(workspace: string): Promise<WorkspaceFile[]> {
  const res = await fetch(`${BACKEND_URL}/workspaces/${encodeURIComponent(workspace)}/files`);
  const payload = await check(res).json();
  return payload.files as WorkspaceFile[];
}

export async function uploadFile(workspace: string, file: File, targetPath?: string): Promise<WorkspaceFile> {
  const form = new FormData();
  form.append('file', file);
  if (targetPath) {
    form.append('path', targetPath);
  }
  const res = await fetch(`${BACKEND_URL}/workspaces/${encodeURIComponent(workspace)}/files`, {
    method: 'POST',
    body: form,
  });
  const payload = await check(res).json();
  return payload as WorkspaceFile;
}

export interface RunTaskPayload {
  params?: Record<string, unknown>;
  job_id?: string;
}

export async function runTask(workspace: string, task: string, payload: RunTaskPayload): Promise<JobResponse> {
  const res = await fetch(`${BACKEND_URL}/workspaces/${encodeURIComponent(workspace)}/run/${task}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  if (!res.ok) {
    const message = (data && data.error) ? `${res.status} ${data.error}` : `Task failed (${res.status})`;
    throw new Error(message);
  }
  return data as JobResponse;
}

export interface MatlabStatus {
  authenticated: boolean;
  email?: string;
  expires_at?: string;
  [key: string]: unknown;
}

export async function getMatlabStatus(): Promise<MatlabStatus> {
  const res = await fetch(`${BACKEND_URL}/matlab/status`);
  if (res.status === 404 || res.status === 503) {
    throw new Error('MATLAB service unavailable');
  }
  return check(res).json();
}

export async function getMatlabBrowserUrl(): Promise<string | null> {
  const res = await fetch(`${BACKEND_URL}/matlab/browser`);
  if (res.status === 404) {
    return null;
  }
  const payload = await check(res).json();
  const url = typeof payload.url === 'string' ? payload.url : null;
  return url;
}

export async function downloadFile(workspace: string, path: string): Promise<Blob> {
  const res = await fetch(`${BACKEND_URL}/workspaces/${encodeURIComponent(workspace)}/files/${path}`);
  return check(res).blob();
}
