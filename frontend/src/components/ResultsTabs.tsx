import { useEffect, useMemo, useState } from 'react';
import Papa from 'papaparse';
import { JobEntry } from './JobLog';
import { WorkspaceFile, downloadFile } from '../api';

export interface ResultsTabsProps {
  workspace: string;
  jobs: JobEntry[];
}

interface PreviewData {
  headers: string[];
  rows: string[][];
}

export function ResultsTabs({ workspace, jobs }: ResultsTabsProps) {
  const [selectedTask, setSelectedTask] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<WorkspaceFile | null>(null);
  const [preview, setPreview] = useState<PreviewData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const taskGroups = useMemo(() => {
    const groups = new Map<string, WorkspaceFile[]>();
    for (const job of jobs) {
      if (job.status !== 'succeeded' || !job.job) continue;
      if (!groups.has(job.task)) {
        groups.set(job.task, []);
      }
      const list = groups.get(job.task)!;
      job.job.outputs.forEach((file) => {
        if (!list.find((item) => item.path === file.path)) {
          list.push(file);
        }
      });
    }
    return Array.from(groups.entries());
  }, [jobs]);

  useEffect(() => {
    if (taskGroups.length > 0 && !selectedTask) {
      setSelectedTask(taskGroups[0][0]);
    }
  }, [taskGroups, selectedTask]);

  useEffect(() => {
    if (!selectedTask) {
      setSelectedFile(null);
      setPreview(null);
    } else {
      const group = taskGroups.find(([task]) => task === selectedTask);
      if (group && group[1].length > 0) {
        setSelectedFile(group[1][0]);
      } else {
        setSelectedFile(null);
        setPreview(null);
      }
    }
  }, [selectedTask, taskGroups]);

  useEffect(() => {
    const loadPreview = async () => {
      if (!selectedFile) {
        setPreview(null);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const blob = await downloadFile(workspace, selectedFile.path);
        const text = await blob.text();
        const parsed = Papa.parse<string[]>(text, { skipEmptyLines: true });
        if (parsed.errors.length > 0) {
          throw new Error(parsed.errors[0].message);
        }
        const [headers, ...rows] = parsed.data;
        setPreview({
          headers: headers ?? [],
          rows: rows.slice(0, 50),
        });
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to preview file');
      } finally {
        setLoading(false);
      }
    };
    void loadPreview();
  }, [workspace, selectedFile]);

  if (taskGroups.length === 0) {
    return null;
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>4 · Results explorer</h3>
        <p className="small">Inspect CSV outputs per task, preview data, and verify pipelines.</p>
      </div>
      <div className="tabs">
        {taskGroups.map(([task]) => (
          <button
            key={task}
            type="button"
            className={`tab-button ${task === selectedTask ? 'active' : ''}`}
            onClick={() => setSelectedTask(task)}
          >
            {task}
          </button>
        ))}
      </div>
      {selectedTask && (
        <div className="results-content">
          <div className="field">
            <label htmlFor="results-file">Available outputs</label>
            <select
              id="results-file"
              value={selectedFile?.path ?? ''}
              onChange={(event) => {
                const group = taskGroups.find(([task]) => task === selectedTask);
                const file = group?.[1].find((item) => item.path === event.target.value) ?? null;
                setSelectedFile(file);
              }}
            >
              {taskGroups
                .find(([task]) => task === selectedTask)?.[1]
                .map((file) => (
                  <option key={file.path} value={file.path}>{file.path}</option>
                ))}
            </select>
          </div>
          {loading && <p className="status uploading">Loading preview…</p>}
          {error && <p className="status error">{error}</p>}
          {preview && (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    {preview.headers.map((header) => (
                      <th key={header}>{header}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.rows.map((row, index) => (
                    <tr key={index}>
                      {row.map((cell, cellIndex) => (
                        <td key={cellIndex}>{cell}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
