import { WorkspaceFile, JobResponse } from '../api';

export interface JobEntry {
  task: string;
  status: 'running' | 'succeeded' | 'failed';
  message: string;
  job?: JobResponse;
  error?: string;
}

export interface JobLogProps {
  jobs: JobEntry[];
  onDownload: (file: WorkspaceFile) => void;
}

export function JobLog({ jobs, onDownload }: JobLogProps) {
  if (jobs.length === 0) {
    return null;
  }
  return (
    <div className="panel">
      <div className="panel-header">
        <h3>Activity log</h3>
        <p className="small">Monitor uploads, exports, interpolation, and project pipelines.</p>
      </div>
      <div className="job-log">
        {jobs.map((job, index) => (
          <article key={`${job.task}-${index}`} className="job-entry">
            <div className="badge">{job.task}</div>
            <strong>Status:</strong> {job.status}
            {job.error && <p className="status error">{job.error}</p>}
            {job.message && <p className="small">{job.message}</p>}
            {job.job?.stdout_tail && (
              <details>
                <summary>Stdout tail</summary>
                <pre>{job.job.stdout_tail}</pre>
              </details>
            )}
            {job.job?.stderr_tail && (
              <details>
                <summary>Stderr tail</summary>
                <pre>{job.job.stderr_tail}</pre>
              </details>
            )}
            {job.job?.outputs && job.job.outputs.length > 0 && (
              <div>
                <strong>Outputs:</strong>
                <ul>
                  {job.job.outputs.map((file) => (
                    <li key={file.path}>
                      <button className="link-button" type="button" onClick={() => onDownload(file)}>
                        {file.path}
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </article>
        ))}
      </div>
    </div>
  );
}
