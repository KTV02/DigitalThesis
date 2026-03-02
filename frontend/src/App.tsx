import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './styles/app.css';
import './styles/index.css';
import { UploadCard, UploadFormState } from './components/UploadCard';
import { ensureWorkspace, listWorkspaceFiles, uploadFile, runTask, listTasks, WorkspaceFile, JobResponse, getMatlabBrowserUrl } from './api';
import { InterpolationForm, InterpolationParams } from './components/InterpolationForm';
import { ProjectConfigurator, ProjectKey, ProjectConfig } from './components/ProjectConfigurator';
import { JobEntry, JobLog } from './components/JobLog';
import { ResultsTabs } from './components/ResultsTabs';
import { DataExplorer } from './components/DataExplorer';
import { QuestionnairesSection } from './components/QuestionnairesSection';

const defaultInterpolation: InterpolationParams = {
  dedupRound: 'minute',
  dedupAgg: 'mean',
  hrInterp: 'linear',
  hrvInterp: 'linear',
  tempInterp: 'linear',
  polyDegree: 3,
  edgeFill: 'none',
  strictScipy: false,
  sleepMergeThresholdMins: 10,
  stepsDupeAgg: 'mean',
  stepsSleepAssisted: false,
};

const defaultProjectParams: Record<ProjectKey, ProjectConfig> = {
  metrics: { mvpaCadence: 100 },
  coverage: { participant: 'USER123' }, 
  fips: { userId: 'USER123' },
  anomaly: { userId: 'USER123', hrFormat: 'iso_minute', outliers: 0.1 },
  laad: { userId: 'USER123', strict: false, synthesizeStepsZeros: false },
  moodml: { userId: 'USER123', longestPerDay: false },
};

type ExportOutputs = Record<'apple' | 'google', { dir: string; job: JobResponse } | undefined>;

interface MoodmlManualState {
  stage: 'pending-finalize';
  outputDir: string;
  script?: WorkspaceFile;
  instructions?: WorkspaceFile;
  example?: WorkspaceFile;
}

export default function App() {
  const [workspace, setWorkspace] = useState<string>('');
  const [files, setFiles] = useState<WorkspaceFile[]>([]);
  const [jobs, setJobs] = useState<JobEntry[]>([]);
  const [interpolationParams, setInterpolationParams] = useState<InterpolationParams>(defaultInterpolation);
  const [exportOutputs, setExportOutputs] = useState<ExportOutputs>({ apple: undefined, google: undefined });
  const [interpolatedOutput, setInterpolatedOutput] = useState<JobResponse | null>(null);
  const [selectedProjects, setSelectedProjects] = useState<ProjectKey[]>([]);
  const [projectParams, setProjectParams] = useState<Record<ProjectKey, ProjectConfig>>(defaultProjectParams);
  const [projectProgress, setProjectProgress] = useState(0);
  const [projectRunning, setProjectRunning] = useState(false);
  const [moodmlManual, setMoodmlManual] = useState<MoodmlManualState | null>(null);
  const [matlabBrowserUrl, setMatlabBrowserUrl] = useState<string | null>(null);
  const [matlabBrowserError, setMatlabBrowserError] = useState<string | null>(null);
  const [showMoodmlInfo, setShowMoodmlInfo] = useState(false);

  useEffect(() => {
    const init = async () => {
      const ws = await ensureWorkspace();
      setWorkspace(ws);
    };
    void init();
  }, []);

  const loadMatlabBrowserUrl = useCallback(async (): Promise<string | null> => {
    try {
      const url = await getMatlabBrowserUrl();
      if (url) {
        setMatlabBrowserUrl(url);
        setMatlabBrowserError(null);
        return url;
      }
      const message = 'MATLAB browser authentication endpoint is unavailable. Ensure the MATLAB container is running.';
      setMatlabBrowserUrl(null);
      setMatlabBrowserError(message);
      return null;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to load MATLAB browser authentication URL.';
      setMatlabBrowserUrl(null);
      setMatlabBrowserError(message);
      return null;
    }
  }, []);

  useEffect(() => {
    if (moodmlManual && !matlabBrowserUrl) {
      void loadMatlabBrowserUrl();
    }
  }, [moodmlManual, matlabBrowserUrl, loadMatlabBrowserUrl]);

  const refreshFiles = useCallback(async () => {
    if (!workspace) return;
    const list = await listWorkspaceFiles(workspace);
    setFiles(list);
  }, [workspace]);

  useEffect(() => {
    if (!workspace) return;
    void refreshFiles();
    void listTasks().catch((err) => console.warn('Failed to load task metadata', err));
  }, [workspace, refreshFiles]);

  const trackJob = (task: string, message: string) => {
    const indexRef = { current: -1 };
    const entry: JobEntry = { task, status: 'running', message };
    setJobs((prev) => {
      const next = [...prev, entry];
      indexRef.current = next.length - 1;
      return next;
    });
    return (updates: Partial<JobEntry>) => {
      setJobs((prev) => prev.map((job, index) => (index === indexRef.current ? { ...job, ...updates } : job)));
    };
  };

  const handleUpload = useCallback(
    async (kind: 'apple' | 'google', file: File, form: UploadFormState) => {
      if (!workspace) throw new Error('Workspace not ready yet');
      const update = trackJob(`${kind}-export`, `Uploading ${file.name}`);
      try {
        const uploaded = await uploadFile(workspace, file);
        const params: Record<string, unknown> = {
          [kind === 'apple' ? 'xml' : 'db']: uploaded.path,
          start: form.start,
          end: form.end,
          'user-id': form.userId,
        };
        if (form.sourceName) {
          params['source-name'] = form.sourceName;
        }
        const job = await runTask(workspace, `${kind}-export`, { params });
        update({ status: 'succeeded', job, message: `Export complete (${job.outputs.length} files)` });
        setExportOutputs((prev) => ({ ...prev, [kind]: { dir: job.output_directories['out-dir'], job } }));
        setInterpolationParams((prev) => ({ ...prev, inDir: job.output_directories['out-dir'] }));
        await refreshFiles();
      } catch (err) {
        update({ status: 'failed', error: err instanceof Error ? err.message : 'Export failed' });
        throw err;
      }
    },
    [workspace, refreshFiles]
  );

  const availableInterpolationInputs = useMemo(() => {
    const items: { label: string; value: string }[] = [];
    (['apple', 'google'] as const).forEach((kind) => {
      const data = exportOutputs[kind];
      if (data?.dir) {
        items.push({ label: `${kind === 'apple' ? 'Apple Health' : 'Google Health'} export`, value: data.dir });
      }
    });
    return items;
  }, [exportOutputs]);

  const runInterpolation = useCallback(
    async (params: InterpolationParams) => {
      if (!workspace) return;
      if (!params.inDir) throw new Error('Please select an input directory');
      const update = trackJob('interpolate', 'Running interpolation pipeline');
      try {
        const payload: Record<string, unknown> = {
          'in-dir': params.inDir,
          ...(params.start ? { start: params.start } : {}),
          ...(params.end ? { end: params.end } : {}),
          'dedup-round': params.dedupRound,
          'dedup-agg': params.dedupAgg,
          'hr-interp': params.hrInterp,
          'hrv-interp': params.hrvInterp,
          'temp-interp': params.tempInterp,
          'poly-degree': params.polyDegree,
          'edge-fill': params.edgeFill,
          'strict-scipy': params.strictScipy,
          'sleep-merge-threshold-mins': params.sleepMergeThresholdMins,
          'steps-dupe-agg': params.stepsDupeAgg,
          'steps-sleep-assisted': params.stepsSleepAssisted,
        };
        if (params.stepsMaxPerMinute !== undefined) {
          payload['steps-max-per-minute'] = params.stepsMaxPerMinute;
        }
        const job = await runTask(workspace, 'interpolate', { params: payload });
        update({ status: 'succeeded', job, message: 'Interpolation completed' });
        setInterpolatedOutput(job);
        await refreshFiles();
      } catch (err) {
        update({ status: 'failed', error: err instanceof Error ? err.message : 'Interpolation failed' });
        throw err;
      }
    },
    [workspace, refreshFiles]
  );

  const csvOptions = useMemo(() => files.filter((file) => file.path.endsWith('.csv')).map((file) => ({ value: file.path, label: file.path })), [files]);
  const interpolatedOptions = useMemo(() => csvOptions.filter((opt) => opt.value.includes('interpolated')), [csvOptions]);

  const toggleProject = (project: ProjectKey) => {
    setSelectedProjects((prev) =>
      prev.includes(project) ? prev.filter((item) => item !== project) : [...prev, project]
    );
  };

  const updateProjectParams = (project: ProjectKey, updates: ProjectConfig) => {
    setProjectParams((prev) => ({ ...prev, [project]: updates }));
  };

  const moodmlSelected = selectedProjects.includes('moodml');
  const prevMoodmlSelectedRef = useRef(false);
  useEffect(() => {
    if (moodmlSelected && !prevMoodmlSelectedRef.current) {
      setShowMoodmlInfo(true);
    }
    prevMoodmlSelectedRef.current = moodmlSelected;
  }, [moodmlSelected]);

  useEffect(() => {
    if (moodmlSelected) {
      void loadMatlabBrowserUrl();
    }
  }, [moodmlSelected, loadMatlabBrowserUrl]);

  const openMatlabAuthentication = useCallback(async () => {
    let authWindow: Window | null = null;
    try {
      authWindow = window.open('', '_blank');
      const url = matlabBrowserUrl ?? (await loadMatlabBrowserUrl());
      if (!url) {
        throw new Error(matlabBrowserError || 'MATLAB browser authentication URL is unavailable.');
      }
      if (authWindow) {
        authWindow.location.href = url;
      } else {
        window.open(url, '_blank');
      }
    } catch (err) {
      if (authWindow) {
        authWindow.close();
      }
      const message = err instanceof Error ? err.message : 'Failed to open MATLAB authentication.';
      setMatlabBrowserError(message);
      console.error('Failed to open MATLAB authentication', err);
    }
  }, [loadMatlabBrowserUrl, matlabBrowserUrl, matlabBrowserError]);

  const moodmlFinalizePending = moodmlSelected && moodmlManual?.stage === 'pending-finalize';
  const moodmlOutputFolder = moodmlManual
    ? moodmlManual.script?.path
        ? moodmlManual.script.path.split('/').slice(0, -1).join('/')
        : moodmlManual.instructions?.path
          ? moodmlManual.instructions.path.split('/').slice(0, -1).join('/')
          : moodmlManual.outputDir
    : null;
  const moodmlOutputPath = moodmlOutputFolder ? moodmlOutputFolder.split('\\').join('/') : "unknown";

  const runProjects = useCallback(
    async (projects: ProjectKey[]) => {
      if (!workspace || projects.length === 0) {
        return;
      }
      setProjectRunning(true);
      setProjectProgress(0);
      for (let index = 0; index < projects.length; index += 1) {
        const project = projects[index];
        const update = trackJob(project, 'Executing project pipeline');
        try {
          const params = projectParams[project];
          const payload: Record<string, unknown> = {};
          let successMessage = 'Project completed';
          switch (project) {
            case 'metrics': {
              if (!params.sleepCsv) throw new Error('Sleep CSV is required for metrics');
              payload['sleep-csv'] = params.sleepCsv;
              if (params.heartRateCsv) payload['heart-rate-csv'] = params.heartRateCsv;
              if (params.stepsCsv) payload['steps-csv'] = params.stepsCsv;
              if (params.vo2Csv) payload['vo2max-csv'] = params.vo2Csv;
              if (params.restingHrCsv) payload['resting-hr-csv'] = params.restingHrCsv;
              if (params.start) payload['start'] = params.start;
              if (params.end) payload['end'] = params.end;
              if (params.lat !== undefined) payload['lat'] = params.lat;
              if (params.lon !== undefined) payload['lon'] = params.lon;
              if (params.mvpaCadence !== undefined) payload['mvpa-cadence-threshold'] = params.mvpaCadence;
              break;
            }
            case 'coverage': {
              const hasAny =
                params.heartRateCsv || params.hrvCsv || params.spo2Csv || params.tempCsv ||
                params.stepsCsv || params.sleepCsv || params.restingHrCsv || params.vo2Csv;

              if (!hasAny) throw new Error('Select at least one raw CSV for Coverage');

              if (params.heartRateCsv) payload['heart-rate-csv'] = params.heartRateCsv;
              if (params.hrvCsv) payload['hrv-csv'] = params.hrvCsv;
              if (params.spo2Csv) payload['spo2-csv'] = params.spo2Csv;
              if (params.tempCsv) payload['temp-csv'] = params.tempCsv;
              if (params.stepsCsv) payload['steps-csv'] = params.stepsCsv;
              if (params.sleepCsv) payload['sleep-csv'] = params.sleepCsv;
              if (params.restingHrCsv) payload['resting-hr-csv'] = params.restingHrCsv;
              if (params.vo2Csv) payload['vo2max-csv'] = params.vo2Csv;

              if (params.start) payload['start'] = params.start;
              if (params.end) payload['end'] = params.end;

              if (params.participant) payload['participant'] = params.participant;

              successMessage = 'Coverage metrics computed';
              break;
            }
            case 'fips': {
              if (!params.sleepCsv) throw new Error('Sleep CSV is required for FIPS');
              payload['sleep-csv'] = params.sleepCsv;
              if (params.userId) payload['user-id'] = params.userId;
              break;
            }
            case 'anomaly': {
              if (!params.hrCsv || !params.stepsCsv) throw new Error('HR and steps CSVs are required for anomaly detection');
              payload['hr-csv'] = params.hrCsv;
              payload['steps-csv'] = params.stepsCsv;
              if (params.userId) payload['user-id'] = params.userId;
              if (params.hrFormat) payload['hr-datetime-format'] = params.hrFormat;
              if (params.outliers !== undefined) payload['outliers'] = params.outliers;
              if (params.symptomDate) payload['symptom-date'] = params.symptomDate;
              if (params.diagnosisDate) payload['diagnosis-date'] = params.diagnosisDate;
              if (params.formatOnly) payload['format-only'] = true;
              break;
            }
            case 'laad': {
              if (!params.hr || !params.steps || !params.symptomDate) throw new Error('HR, steps, and symptom date are required for LAAD');
              payload['hr'] = params.hr;
              payload['steps'] = params.steps;
              payload['symptom-date'] = params.symptomDate;
              if (params.userId) payload['user-id'] = params.userId;
              if (params.strict) payload['strict'] = true;
              if (params.synthesizeStepsZeros) payload['synthesize-steps-zeros'] = true;
              break;
            }
            case 'moodml': {
              const stage = moodmlManual?.stage === 'pending-finalize' ? 'finalize' : 'prepare';
              payload['stage'] = stage;
              if (params.userId) payload['user-id'] = params.userId;
              if (stage === 'prepare') {
                if (!params.sleepEpisodes || !params.sleepStages) {
                  throw new Error('Sleep episodes and stages are required for MoodML preparation');
                }
                payload['sleep-episodes'] = params.sleepEpisodes;
                payload['sleep-stages'] = params.sleepStages;
                if (params.longestPerDay) payload['longest-per-day'] = true;
                successMessage = 'MoodML preparation complete. Run MATLAB script manually before finalizing.';
              } else {
                if (!moodmlManual?.outputDir) {
                  throw new Error('MoodML preparation output directory is unknown. Re-run preparation first.');
                }
                payload['output-dir'] = moodmlManual.outputDir;
                successMessage = 'MoodML finalization complete';
              }
              break;
            }
            default:
              break;
          }
          const job = await runTask(workspace, project, { params: payload });
          if (project === 'moodml') {
            const stage = String(payload['stage']);
            if (stage === 'prepare') {
              const outputDir = job.output_directories['output-dir'];
              const findFile = (filename: string) => job.outputs.find((file) => file.path.endsWith(filename));
              setMoodmlManual({
                stage: 'pending-finalize',
                outputDir,
                script: findFile('run_moodml_manual.m'),
                instructions: findFile('MOODML_MANUAL_INSTRUCTIONS.txt'),
                example: findFile('example.csv'),
              });
            } else {
              setMoodmlManual(null);
            }
          }
          update({ status: 'succeeded', job, message: successMessage });
          await refreshFiles();
        } catch (err) {
          update({ status: 'failed', error: err instanceof Error ? err.message : 'Project failed' });
          break;
        } finally {
          setProjectProgress((index + 1) / projects.length);
        }
      }
      setProjectRunning(false);
      setProjectProgress(1);
    },
    [workspace, projectParams, refreshFiles, moodmlManual]
  );

  const handleProcessProjects = useCallback(() => {
    if (selectedProjects.length === 0) return;
    const needsFinalize = moodmlSelected && moodmlManual?.stage === 'pending-finalize';
    const projectsToRun: ProjectKey[] = needsFinalize ? ['moodml'] : selectedProjects;
    void runProjects(projectsToRun);
  }, [selectedProjects, moodmlManual, runProjects, moodmlSelected]);

  const handleDownload = async (file: WorkspaceFile) => {
    const url = `${window.location.origin}${file.download_url}`;
    window.open(url, '_blank');
  };

  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>Unified Health Analytics Workbench</h1>
        <p>
          Upload Apple Health or Google Health Connect exports, harmonize your data, tune interpolation, and orchestrate project pipelines.
          Inspect results interactively and download outputs on demand.
        </p>
      </header>
      <main className="app-content">
        <section className="upload-grid">
          <UploadCard
            title="Apple Health"
            description="Upload export.xml and run the Apple Health raw exporter."
            accept=".xml"
            defaults={{ start: '', end: '', userId: 'USER123' }}
            onUpload={(file, form) => handleUpload('apple', file, form)}
          />
          <UploadCard
            title="Google Health Connect"
            description="Upload the SQLite dump to launch the Google raw exporter."
            accept=".db"
            accent="sky"
            defaults={{ start: '', end: '', userId: 'USER123' }}
            onUpload={(file, form) => handleUpload('google', file, form)}
          />
        </section>

        <InterpolationForm
          params={interpolationParams}
          onChange={setInterpolationParams}
          onSubmit={runInterpolation}
          availableInputs={availableInterpolationInputs}
        />

        <div className="panel">
          <div className="panel-header">
            <h3>3 · Project selection</h3>
            <p className="small">Choose which downstream pipelines to execute and customize their parameters.</p>
          </div>
          <ProjectConfigurator
            selected={selectedProjects}
            onToggle={toggleProject}
            params={projectParams}
            onParamsChange={updateProjectParams}
            csvOptions={csvOptions}
            interpolatedOptions={interpolatedOptions}
          />
          {moodmlSelected && showMoodmlInfo && (
            <div className="notice notice-moodml">
              <div className="notice-heading">
                <h4>Manual MATLAB step required for MoodML</h4>
                <button
                  className="link-button"
                  type="button"
                  onClick={() => setShowMoodmlInfo(false)}
                >
                  Dismiss
                </button>
              </div>
              <p>
                The MoodML pipeline runs in two phases. First we generate the prepared data and a MATLAB script
                named <code>run_moodml_manual.m</code> that must be executed manually. Afterwards you can return here to
                finalize predictions just like other projects.
              </p>
              <ol>
                <li>
                  Run the project once to generate <code>run_moodml_manual.m</code> and <code>MOODML_MANUAL_INSTRUCTIONS.txt</code>
                  in your workspace.
                </li>
                <li>
                  Open MATLAB, switch the working directory to the generated folder (/data/your_workspace/jobs/see_path_below), and execute{' '}
                  <code>run_moodml_manual.m</code>. It will call <code>Index_calculation.m</code> using the paths we prepared.
                </li>
                <li>
                  After MATLAB finishes creating <code>test.csv</code>, return here and click <strong>Finalize MoodML</strong> to
                  produce the model outputs.
                </li>
              </ol>
              <div className="notice-actions">
                <button className="button button-ghost" type="button" onClick={openMatlabAuthentication}>
                  {matlabBrowserUrl ? 'Open MATLAB authentication' : 'Start MATLAB authentication'}
                </button>
                {moodmlManual?.instructions && (
                  <button
                    className="button button-ghost"
                    type="button"
                    onClick={() => handleDownload(moodmlManual.instructions!)}
                  >
                    Download latest instructions
                  </button>
                )}
              </div>
              {matlabBrowserError && <p className="notice-error">{matlabBrowserError}</p>}
            </div>
          )}
          <button className="button" type="button" onClick={handleProcessProjects} disabled={projectRunning || selectedProjects.length === 0}>
            {projectRunning ? 'Processing…' : moodmlFinalizePending ? 'Finalize MoodML' : 'Run selected projects'}
          </button>
          {projectRunning && (
            <div className="progress-bar">
              <div className="progress-bar-fill" style={{ width: `${Math.round(projectProgress * 100)}%` }} />
            </div>
          )}
        </div>

        {moodmlManual && (
          <div className="panel">
            <div className="panel-header">
              <h3>MoodML manual MATLAB step pending</h3>
              <p className="small">
                MATLAB must be run manually in the prepared output directory before finalizing the MoodML predictions.
              </p>
            </div>
            <div className="notice">
              <p>
                Workspace output folder:{' '}
                <span className="code-inline">{moodmlOutputPath}</span>
              </p>
              <ol>
                <li>
                  Download the generated MATLAB script{' '}
                  {moodmlManual.script ? (
                    <button className="link-button" type="button" onClick={() => handleDownload(moodmlManual.script!)}>
                      {moodmlManual.script.path.split('/').pop()}
                    </button>
                  ) : (
                    <span><code>run_moodml_manual.m</code></span>
                  )}{' '}
                  and open it inside MATLAB.
                </li>
                <li>
                  Change MATLAB&apos;s working directory to the folder above and run the script. It calls <code>Index_calculation.m</code>
                  {' '}to create <code>test.csv</code>.
                </li>
                <li>
                  After MATLAB finishes, return here and press <strong>Finalize MoodML</strong> to generate the prediction CSV files.
                </li>
              </ol>
              {moodmlManual.instructions && (
                <p>
                  Detailed instructions:{' '}
                  <button className="link-button" type="button" onClick={() => handleDownload(moodmlManual.instructions!)}>
                    {moodmlManual.instructions.path.split('/').pop()}
                  </button>
                </p>
              )}
              <p>
                Need to authenticate MATLAB?{' '}
                <button className="link-button" type="button" onClick={openMatlabAuthentication}>
                  {matlabBrowserUrl ? 'Open authentication page' : 'Start authentication'}
                </button>
              </p>
              {matlabBrowserError && <p className="notice-error">{matlabBrowserError}</p>}
            </div>
          </div>
        )}

        <JobLog jobs={jobs} onDownload={handleDownload} />
        <ResultsTabs workspace={workspace} jobs={jobs} />
        <DataExplorer workspace={workspace} files={files} />
        <QuestionnairesSection workspace={workspace} files={files} onFilesUpdated={refreshFiles} />
      </main>
    </div>
  );
}
