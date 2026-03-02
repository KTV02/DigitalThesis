import { FormEvent } from 'react';
import { UploadFormState } from './UploadCard';

export interface InterpolationParams {
  inDir?: string;
  start?: string;
  end?: string;
  dedupRound: 'none' | 'second' | 'minute';
  dedupAgg: 'mean' | 'median' | 'max' | 'min';
  hrInterp: 'linear' | 'polynomial' | 'newtons' | 'neighbor' | 'pchip' | 'cubic';
  hrvInterp: 'linear' | 'polynomial' | 'newtons' | 'neighbor' | 'pchip' | 'cubic';
  tempInterp: 'linear' | 'polynomial' | 'newtons' | 'neighbor' | 'pchip' | 'cubic';
  polyDegree: number;
  edgeFill: 'none' | 'ffill' | 'bfill' | 'both';
  strictScipy: boolean;
  sleepMergeThresholdMins: number;
  stepsMaxPerMinute?: number | '';
  stepsDupeAgg: 'mean' | 'max' | 'min';
  stepsSleepAssisted: boolean;
}

export interface InterpolationFormProps {
  params: InterpolationParams;
  onChange: (params: InterpolationParams) => void;
  onSubmit: (params: InterpolationParams) => Promise<void>;
  disabled?: boolean;
  availableInputs: Array<{ label: string; value: string }>;
}

const INTERP_OPTIONS: InterpolationParams["hrInterp"][] = ['linear', 'polynomial', 'newtons', 'neighbor', 'pchip', 'cubic'];
const ROUND_OPTIONS: InterpolationParams['dedupRound'][] = ['none', 'second', 'minute'];
const AGG_OPTIONS: InterpolationParams['dedupAgg'][] = ['mean', 'median', 'max', 'min'];
const STEPS_AGG_OPTIONS: InterpolationParams['stepsDupeAgg'][] = ['mean', 'max', 'min'];
const EDGE_OPTIONS: InterpolationParams['edgeFill'][] = ['none', 'ffill', 'bfill', 'both'];

export function InterpolationForm({ params, onChange, onSubmit, disabled, availableInputs }: InterpolationFormProps) {
  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    await onSubmit(params);
  };

  return (
    <form className="panel" onSubmit={handleSubmit}>
      <div className="panel-header">
        <h3>2 · Interpolate + harmonize data</h3>
        <p className="small">Configure interpolation parameters and run the harmonizer.</p>
      </div>
      <div className="interpolation-grid">
        <div className="field">
          <label htmlFor="interpolate-input">Input directory</label>
          <select
            id="interpolate-input"
            value={params.inDir ?? ''}
            onChange={(event) => onChange({ ...params, inDir: event.target.value || undefined })}
            required
          >
            <option value="">Select exported CSV folder…</option>
            {availableInputs.map((item) => (
              <option key={item.value} value={item.value}>{item.label}</option>
            ))}
          </select>
        </div>
        <div className="field">
          <label htmlFor="interpolate-start">Start date override</label>
          <input
            id="interpolate-start"
            type="date"
            value={params.start ?? ''}
            onChange={(event) => onChange({ ...params, start: event.target.value || undefined })}
          />
        </div>
        <div className="field">
          <label htmlFor="interpolate-end">End date override</label>
          <input
            id="interpolate-end"
            type="date"
            value={params.end ?? ''}
            onChange={(event) => onChange({ ...params, end: event.target.value || undefined })}
          />
        </div>
        <div className="field">
          <label htmlFor="dedup-round">Deduplication bucket</label>
          <select
            id="dedup-round"
            value={params.dedupRound}
            onChange={(event) => onChange({ ...params, dedupRound: event.target.value as InterpolationParams['dedupRound'] })}
          >
            {ROUND_OPTIONS.map((option) => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </div>
        <div className="field">
          <label htmlFor="dedup-agg">Duplicate aggregation</label>
          <select
            id="dedup-agg"
            value={params.dedupAgg}
            onChange={(event) => onChange({ ...params, dedupAgg: event.target.value as InterpolationParams['dedupAgg'] })}
          >
            {AGG_OPTIONS.map((option) => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </div>
        <div className="field">
          <label>Interpolation methods</label>
          <div className="interpolation-grid">
            <div className="field">
              <label htmlFor="hr-interp">Heart rate</label>
              <select
                id="hr-interp"
                value={params.hrInterp}
                onChange={(event) => onChange({ ...params, hrInterp: event.target.value as InterpolationParams['hrInterp'] })}
              >
                {INTERP_OPTIONS.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </div>
            <div className="field">
              <label htmlFor="hrv-interp">HRV</label>
              <select
                id="hrv-interp"
                value={params.hrvInterp}
                onChange={(event) => onChange({ ...params, hrvInterp: event.target.value as InterpolationParams['hrvInterp'] })}
              >
                {INTERP_OPTIONS.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </div>
            <div className="field">
              <label htmlFor="temp-interp">Temperature</label>
              <select
                id="temp-interp"
                value={params.tempInterp}
                onChange={(event) => onChange({ ...params, tempInterp: event.target.value as InterpolationParams['tempInterp'] })}
              >
                {INTERP_OPTIONS.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </div>
          </div>
        </div>
        <div className="field">
          <label htmlFor="poly-degree">Polynomial degree</label>
          <input
            id="poly-degree"
            type="number"
            min={1}
            max={10}
            value={params.polyDegree}
            onChange={(event) => onChange({ ...params, polyDegree: Number(event.target.value) })}
          />
        </div>
        <div className="field">
          <label htmlFor="edge-fill">Edge filling</label>
          <select
            id="edge-fill"
            value={params.edgeFill}
            onChange={(event) => onChange({ ...params, edgeFill: event.target.value as InterpolationParams['edgeFill'] })}
          >
            {EDGE_OPTIONS.map((option) => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </div>
        <div className="field">
          <label className="checkbox-list">
            <input
              type="checkbox"
              checked={params.strictScipy}
              onChange={(event) => onChange({ ...params, strictScipy: event.target.checked })}
            />
            Require SciPy for PCHIP/cubic
          </label>
        </div>
        <div className="field">
          <label htmlFor="sleep-merge">Sleep merge threshold (minutes)</label>
          <input
            id="sleep-merge"
            type="number"
            min={0}
            value={params.sleepMergeThresholdMins}
            onChange={(event) => onChange({ ...params, sleepMergeThresholdMins: Number(event.target.value) })}
          />
        </div>
        <div className="field">
          <label htmlFor="steps-max">Max steps per minute</label>
          <input
            id="steps-max"
            type="number"
            min={0}
            value={params.stepsMaxPerMinute === undefined ? '' : params.stepsMaxPerMinute}
            onChange={(event) => {
              const value = event.target.value;
              onChange({ ...params, stepsMaxPerMinute: value === '' ? undefined : Number(value) });
            }}
          />
        </div>
        <div className="field">
          <label htmlFor="steps-dupe">Steps duplicate aggregation</label>
          <select
            id="steps-dupe"
            value={params.stepsDupeAgg}
            onChange={(event) => onChange({ ...params, stepsDupeAgg: event.target.value as InterpolationParams['stepsDupeAgg'] })}
          >
            {STEPS_AGG_OPTIONS.map((option) => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </div>
        <div className="field">
          <label className="checkbox-list">
            <input
              type="checkbox"
              checked={params.stepsSleepAssisted}
              onChange={(event) => onChange({ ...params, stepsSleepAssisted: event.target.checked })}
            />
            Sleep-assisted steps distribution
          </label>
        </div>
      </div>
      <button className="button" type="submit" disabled={disabled || !params.inDir}>
        Run interpolation
      </button>
    </form>
  );
}
