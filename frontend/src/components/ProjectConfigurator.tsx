import { ChangeEvent } from 'react';

export type ProjectKey = 'metrics' | 'coverage' | 'fips' | 'anomaly' | 'laad' | 'moodml';

export interface OptionItem {
  value: string;
  label: string;
}

export interface ProjectConfig {
  [key: string]: unknown;
}

export interface ProjectConfiguratorProps {
  selected: ProjectKey[];
  onToggle: (project: ProjectKey) => void;
  params: Record<ProjectKey, ProjectConfig>;
  onParamsChange: (project: ProjectKey, updates: ProjectConfig) => void;
  csvOptions: OptionItem[];
  interpolatedOptions: OptionItem[];
}

const PROJECT_LABELS: Record<ProjectKey, string> = {
  metrics: 'Metrics (daily summaries, MVPA, VO₂, sleep metrics)',
  coverage: 'Coverage (raw data coverage + sampling frequency)',
  fips: 'FIPS (sleep episode visualizations)',
  anomaly: 'Anomaly detection (RH-RAD + HRoSAD)',
  laad: 'LAAD (resting HR monitoring)',
  moodml: 'MoodML (MATLAB pipeline)'
};

function renderSelect(
  id: string,
  label: string,
  value: unknown,
  onChange: (event: ChangeEvent<HTMLSelectElement>) => void,
  options: OptionItem[],
  placeholder = 'Select CSV…',
) {
  return (
    <div className="field">
      <label htmlFor={id}>{label}</label>
      <select id={id} value={(value as string) ?? ''} onChange={onChange}>
        <option value="">{placeholder}</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>{option.label}</option>
        ))}
      </select>
    </div>
  );
}

export function ProjectConfigurator({ selected, onToggle, params, onParamsChange, csvOptions, interpolatedOptions }: ProjectConfiguratorProps) {
  const handleField = (project: ProjectKey, key: string, value: unknown) => {
    onParamsChange(project, { ...params[project], [key]: value });
  };

  const selectedSet = new Set(selected);
    // RAW = everything that is NOT in the interpolated folder
  const rawCsvOptions = csvOptions.filter((opt) => !opt.value.includes('interpolated'));

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>3 · Run project pipelines</h3>
        <p className="small">Choose downstream analytics to execute. Configure inputs per project.</p>
      </div>
      <div className="checkbox-list">
        {(Object.keys(PROJECT_LABELS) as ProjectKey[]).map((project) => (
          <label key={project}>
            <input
              type="checkbox"
              checked={selectedSet.has(project)}
              onChange={() => onToggle(project)}
            />
            <span>{PROJECT_LABELS[project]}</span>
          </label>
        ))}
      </div>

      {selectedSet.has('metrics') && (
        <section className="project-section">
          <div className="badge">Metrics</div>
          <div className="project-grid">
            {renderSelect(
              'metrics-sleep',
              'Sleep CSV',
              params.metrics.sleepCsv,
              (event) => handleField('metrics', 'sleepCsv', event.target.value || undefined),
              csvOptions.filter((opt) => opt.value.includes('sleep'))
            )}
            {renderSelect(
              'metrics-hr',
              'Heart rate CSV',
              params.metrics.heartRateCsv,
              (event) => handleField('metrics', 'heartRateCsv', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('heart_rate'))
            )}
            {renderSelect(
              'metrics-steps',
              'Steps CSV',
              params.metrics.stepsCsv,
              (event) => handleField('metrics', 'stepsCsv', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('steps'))
            )}
            {renderSelect(
              'metrics-vo2',
              'VO₂ max CSV',
              params.metrics.vo2Csv,
              (event) => handleField('metrics', 'vo2Csv', event.target.value || undefined),
              csvOptions.filter((opt) => opt.value.includes('vo2'))
            )}
            {renderSelect(
              'metrics-resting',
              'Resting HR CSV',
              params.metrics.restingHrCsv,
              (event) => handleField('metrics', 'restingHrCsv', event.target.value || undefined),
              csvOptions.filter((opt) => opt.value.includes('resting'))
            )}
            <div className="field">
              <label htmlFor="metrics-start">Start date</label>
              <input
                id="metrics-start"
                type="date"
                value={(params.metrics.start as string) ?? ''}
                onChange={(event) => handleField('metrics', 'start', event.target.value || undefined)}
              />
            </div>
            <div className="field">
              <label htmlFor="metrics-end">End date</label>
              <input
                id="metrics-end"
                type="date"
                value={(params.metrics.end as string) ?? ''}
                onChange={(event) => handleField('metrics', 'end', event.target.value || undefined)}
              />
            </div>
            <div className="field">
              <label htmlFor="metrics-lat">Latitude</label>
              <input
                id="metrics-lat"
                type="number"
                value={(params.metrics.lat as number | undefined) ?? ''}
                onChange={(event) => handleField('metrics', 'lat', event.target.value === '' ? undefined : Number(event.target.value))}
              />
            </div>
            <div className="field">
              <label htmlFor="metrics-lon">Longitude</label>
              <input
                id="metrics-lon"
                type="number"
                value={(params.metrics.lon as number | undefined) ?? ''}
                onChange={(event) => handleField('metrics', 'lon', event.target.value === '' ? undefined : Number(event.target.value))}
              />
            </div>
            <div className="field">
              <label htmlFor="metrics-mvpa">MVPA cadence threshold</label>
              <input
                id="metrics-mvpa"
                type="number"
                value={(params.metrics.mvpaCadence as number | undefined) ?? 100}
                onChange={(event) => handleField('metrics', 'mvpaCadence', Number(event.target.value))}
              />
            </div>
          </div>
        </section>
      )}


      {selectedSet.has('coverage') && (
        <section className="project-section">
          <div className="badge">Coverage</div>
          <div className="project-grid">

            <div className="field">
              <label htmlFor="coverage-start">Start date</label>
              <input
                id="coverage-start"
                type="date"
                value={(params.coverage.start as string) ?? ''}
                onChange={(event) => handleField('coverage', 'start', event.target.value || undefined)}
              />
            </div>

            <div className="field">
              <label htmlFor="coverage-end">End date</label>
              <input
                id="coverage-end"
                type="date"
                value={(params.coverage.end as string) ?? ''}
                onChange={(event) => handleField('coverage', 'end', event.target.value || undefined)}
              />
            </div>
            {renderSelect(
              'coverage-hr',
              'Heart rate CSV (raw)',
              params.coverage.heartRateCsv,
              (event) => handleField('coverage', 'heartRateCsv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('heart_rate'))
            )}
            {renderSelect(
              'coverage-hrv',
              'HRV CSV (raw)',
              params.coverage.hrvCsv,
              (event) => handleField('coverage', 'hrvCsv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('hrv'))
            )}
            {renderSelect(
              'coverage-spo2',
              'SpO₂ CSV (raw)',
              params.coverage.spo2Csv,
              (event) => handleField('coverage', 'spo2Csv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('spo2'))
            )}
            {renderSelect(
              'coverage-temp',
              'Temperature CSV (raw)',
              params.coverage.tempCsv,
              (event) => handleField('coverage', 'tempCsv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('temp'))
            )}
            {renderSelect(
              'coverage-steps',
              'Steps CSV (raw)',
              params.coverage.stepsCsv,
              (event) => handleField('coverage', 'stepsCsv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('steps'))
            )}
            {renderSelect(
              'coverage-sleep',
              'Sleep CSV (raw)',
              params.coverage.sleepCsv,
              (event) => handleField('coverage', 'sleepCsv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('sleep'))
            )}
            {renderSelect(
              'coverage-resting',
              'Resting HR CSV (raw)',
              params.coverage.restingHrCsv,
              (event) => handleField('coverage', 'restingHrCsv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('resting'))
            )}
            {renderSelect(
              'coverage-vo2',
              'VO₂ max CSV (raw)',
              params.coverage.vo2Csv,
              (event) => handleField('coverage', 'vo2Csv', event.target.value || undefined),
              rawCsvOptions.filter((opt) => opt.value.includes('vo2'))
            )}
            <div className="field">
              <label htmlFor="coverage-participant">Participant label (optional)</label>
              <input
                id="coverage-participant"
                type="text"
                value={(params.coverage.participant as string | undefined) ?? 'USER123'}
                onChange={(event) => handleField('coverage', 'participant', event.target.value)}
              />
            </div>
          </div>
          <p className="small">
            Computes coverage (% active hours), active days, longest gaps, and median sampling frequency per modality.
          </p>
        </section>
      )}

      {selectedSet.has('fips') && (
        <section className="project-section">
          <div className="badge">FIPS</div>
          <div className="project-grid">
            {renderSelect(
              'fips-sleep',
              'Sleep CSV',
              params.fips.sleepCsv,
              (event) => handleField('fips', 'sleepCsv', event.target.value || undefined),
              csvOptions.filter((opt) => opt.value.includes('sleep_episodes'))
            )}
            <div className="field">
              <label htmlFor="fips-user">User ID</label>
              <input
                id="fips-user"
                type="text"
                value={(params.fips.userId as string | undefined) ?? 'USER123'}
                onChange={(event) => handleField('fips', 'userId', event.target.value)}
              />
            </div>
          </div>
        </section>
      )}

      {selectedSet.has('anomaly') && (
        <section className="project-section">
          <div className="badge">Anomaly</div>
          <div className="project-grid">
            {renderSelect(
              'anomaly-hr',
              'Minute HR CSV',
              params.anomaly.hrCsv,
              (event) => handleField('anomaly', 'hrCsv', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('heart_rate'))
            )}
            {renderSelect(
              'anomaly-steps',
              'Minute steps CSV',
              params.anomaly.stepsCsv,
              (event) => handleField('anomaly', 'stepsCsv', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('steps'))
            )}
            <div className="field">
              <label htmlFor="anomaly-user">User ID</label>
              <input
                id="anomaly-user"
                type="text"
                value={(params.anomaly.userId as string | undefined) ?? 'USER123'}
                onChange={(event) => handleField('anomaly', 'userId', event.target.value)}
              />
            </div>
            <div className="field">
              <label htmlFor="anomaly-hrfmt">HR datetime format</label>
              <select
                id="anomaly-hrfmt"
                value={(params.anomaly.hrFormat as string | undefined) ?? 'iso_minute'}
                onChange={(event) => handleField('anomaly', 'hrFormat', event.target.value)}
              >
                <option value="mdy_minute">mdy_minute</option>
                <option value="iso_minute">iso_minute</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="anomaly-outliers">Outliers fraction</label>
              <input
                id="anomaly-outliers"
                type="number"
                step="0.01"
                min={0}
                max={0.5}
                value={(params.anomaly.outliers as number | undefined) ?? 0.1}
                onChange={(event) => handleField('anomaly', 'outliers', Number(event.target.value))}
              />
            </div>
            <div className="field">
              <label htmlFor="anomaly-symptom">Symptom date</label>
              <input
                id="anomaly-symptom"
                type="date"
                value={(params.anomaly.symptomDate as string | undefined) ?? ''}
                onChange={(event) => handleField('anomaly', 'symptomDate', event.target.value || undefined)}
              />
            </div>
            <div className="field">
              <label htmlFor="anomaly-diagnosis">Diagnosis date</label>
              <input
                id="anomaly-diagnosis"
                type="date"
                value={(params.anomaly.diagnosisDate as string | undefined) ?? ''}
                onChange={(event) => handleField('anomaly', 'diagnosisDate', event.target.value || undefined)}
              />
            </div>
            <div className="field">
              <label className="checkbox-list">
                <input
                  type="checkbox"
                  checked={Boolean(params.anomaly.formatOnly)}
                  onChange={(event) => handleField('anomaly', 'formatOnly', event.target.checked)}
                />
                Format only (skip detectors)
              </label>
            </div>
          </div>
        </section>
      )}

      {selectedSet.has('laad') && (
        <section className="project-section">
          <div className="badge">LAAD</div>
          <div className="project-grid">
            {renderSelect(
              'laad-hr',
              'Minute HR CSV',
              params.laad.hr,
              (event) => handleField('laad', 'hr', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('heart_rate'))
            )}
            {renderSelect(
              'laad-steps',
              'Minute steps CSV',
              params.laad.steps,
              (event) => handleField('laad', 'steps', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('steps'))
            )}
            <div className="field">
              <label htmlFor="laad-symptom">Symptom date</label>
              <input
                id="laad-symptom"
                type="date"
                value={(params.laad.symptomDate as string | undefined) ?? ''}
                onChange={(event) => handleField('laad', 'symptomDate', event.target.value || undefined)}
              />
            </div>
            <div className="field">
              <label htmlFor="laad-user">User ID</label>
              <input
                id="laad-user"
                type="text"
                value={(params.laad.userId as string | undefined) ?? 'USER123'}
                onChange={(event) => handleField('laad', 'userId', event.target.value)}
              />
            </div>
            <div className="field">
              <label className="checkbox-list">
                <input
                  type="checkbox"
                  checked={Boolean(params.laad.strict)}
                  onChange={(event) => handleField('laad', 'strict', event.target.checked)}
                />
                Strict validation
              </label>
            </div>
            <div className="field">
              <label className="checkbox-list">
                <input
                  type="checkbox"
                  checked={Boolean(params.laad.synthesizeStepsZeros)}
                  onChange={(event) => handleField('laad', 'synthesizeStepsZeros', event.target.checked)}
                />
                Synthesize missing steps (zeros)
              </label>
            </div>
          </div>
        </section>
      )}

      {selectedSet.has('moodml') && (
        <section className="project-section">
          <div className="badge">MoodML</div>
          <div className="project-grid">
            {renderSelect(
              'moodml-episodes',
              'Sleep episodes (merged)',
              params.moodml.sleepEpisodes,
              (event) => handleField('moodml', 'sleepEpisodes', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('sleep_episodes'))
            )}
            {renderSelect(
              'moodml-stages',
              'Sleep stages (merged)',
              params.moodml.sleepStages,
              (event) => handleField('moodml', 'sleepStages', event.target.value || undefined),
              interpolatedOptions.filter((opt) => opt.value.includes('sleep_stages'))
            )}
            <div className="field">
              <label htmlFor="moodml-user">User ID</label>
              <input
                id="moodml-user"
                type="text"
                value={(params.moodml.userId as string | undefined) ?? 'USER123'}
                onChange={(event) => handleField('moodml', 'userId', event.target.value)}
              />
            </div>
            <div className="field">
              <label className="checkbox-list">
                <input
                  type="checkbox"
                  checked={Boolean(params.moodml.longestPerDay)}
                  onChange={(event) => handleField('moodml', 'longestPerDay', event.target.checked)}
                />
                Keep only longest episode per day
              </label>
            </div>
          </div>
        </section>
      )}
    </div>
  );
}
