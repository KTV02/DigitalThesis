import { useCallback, useEffect, useMemo, useState } from 'react';
import Papa from 'papaparse';
import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  TimeScale,
  LinearScale,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import 'chartjs-adapter-date-fns';
import { BACKEND_URL, downloadFile, uploadFile, WorkspaceFile } from '../api';

ChartJS.register(LineElement, PointElement, TimeScale, LinearScale, Tooltip, Legend, Filler);

interface QuestionnairesSectionProps {
  workspace: string;
  files: WorkspaceFile[];
  onFilesUpdated?: () => void | Promise<void>;
}

interface DateRange {
  start: Date;
  end: Date;
  labelStart: string;
  labelEnd: string;
}

type QuestionnaireKind = 'psqi' | 'mfi' | 'pss10';
type QuestionnaireResponses = Record<string, string>;

type DataAvailabilityStatus = 'missing' | 'present' | 'empty' | 'error';

interface DataAvailability {
  status: DataAvailabilityStatus;
  path?: string;
  message: string;
}

interface ComparisonItem {
  field: string;
  response: string;
  computed?: string;
  difference?: string;
  status: 'match' | 'mismatch' | 'info' | 'unavailable';
  reason: string;
}

interface ChartColumn {
  label: string;
  data: { x: number; y: number }[];
  color: string;
}

interface ChartBundle {
  filePath: string;
  title: string;
  columns: ChartColumn[];
  reason: string;
}

interface EvaluationResult {
  availability: Record<string, DataAvailability>;
  comparisons: ComparisonItem[] | null;
  charts: ChartBundle[];
  alertnessImages: WorkspaceFile[];
  contextImages: WorkspaceFile[];
}

interface SleepEpisode {
  start: Date;
  end: Date;
  durationMinutes: number;
}

interface FipsTimelineCoverage {
  path: string;
  jobId: string | null;
  matches: boolean;
  startAligned: boolean;
  endAligned: boolean;
  earliest?: Date;
  latest?: Date;
  error?: string;
}

const DAILY_COVERAGE_THRESHOLD = 0.7;


type Pss10Instance = {
  participantId: string;
  scheduled: Date;          // parsed Session Scheduled Time
  scheduledLabel: string;   // original string
  raw: Record<string, string>;
};

type Pss10Responses = Pss10Instance[];

const REQUIRED_DATASETS_PSS10 = {
  hrv: 'HRV minute series (hrv_minute.csv or hrv.csv)',
  moodml: 'MoodML depression risk (expected_outcome_de.csv)',
  restingHr: 'Resting heart rate (resting_hr.csv)',
  sleepEfficiency: 'Sleep duration (sleep_efficiency.csv)',
  mergedSleep: 'Merged sleep episodes (sleep_episodes_merged.csv)',
};


const REQUIRED_DATASETS_PSQI = {
  rawSleep: 'Raw sleep episodes',
  mergedSleep: 'Merged sleep episodes',
  metrics: 'Metrics project outputs',
  fips: 'FIPS project outputs',
};

const REQUIRED_DATASETS_MFI = {
  steps: 'Daily steps',
  hrCosinor: 'HR cosinor metrics (hr_cosinor.csv)',
  sleepEfficiency: 'Sleep duration & efficiency (sleep_efficiency.csv)',
  fipsFatigue: 'FIPS TMP fatigue projections',
};

const DATASET_LABELS: Record<QuestionnaireKind, Record<string, string>> = {
  psqi: REQUIRED_DATASETS_PSQI,
  mfi: REQUIRED_DATASETS_MFI,
  pss10: REQUIRED_DATASETS_PSS10,

};

const QUESTIONNAIRE_SHORT_LABEL: Record<QuestionnaireKind, string> = {
  psqi: 'PSQI',
  mfi: 'MFI-20',
  pss10: 'PSS-10',

};

const QUESTIONNAIRE_RESPONSE_LABEL: Record<QuestionnaireKind, string> = {
  psqi: 'PSQI responses',
  mfi: 'MFI-20 responses',
  pss10: 'PSS-10 responses',

};

function buildInitialAvailability(kind: QuestionnaireKind): Record<string, DataAvailability> {
  const labels = DATASET_LABELS[kind];
  const result: Record<string, DataAvailability> = {};
  Object.entries(labels).forEach(([key, label]) => {
    result[key] = {
      status: 'missing',
      message: buildAvailabilityMessage('missing', label),
    };
  });
  return result;
}

const COLORS = ['#60a5fa', '#34d399', '#f472b6', '#fbbf24', '#38bdf8', '#c084fc', '#f97316'];

function paletteForLabel(label: string) {
  let hash = 0;
  for (const char of label) {
    hash = (hash * 31 + char.charCodeAt(0)) % COLORS.length;
  }
  return COLORS[Math.abs(hash) % COLORS.length];
}

async function parseCsv(path: string, workspace: string) {
  const blob = await downloadFile(workspace, path);
  const text = await blob.text();
  const parsed = Papa.parse<Record<string, string>>(text, { header: true, skipEmptyLines: true });
  if (parsed.errors.length > 0) {
    throw new Error(parsed.errors[0].message);
  }
  return parsed.data.filter((row) => Object.values(row).some((value) => value && value.trim().length > 0));
}




type StepsDailyAgg = {
  points: { x: number; y: number }[];
  totals: number[];
  byDay: Map<string, number>;
};

function extractStepsValue(row: Record<string, any>): number | null {
  const candidates = [
    row.steps,
    row.Steps,
    row.step_count,
    row.Step_count,
    row.stepCount,
    row.count,
    row.Count,
    row.value,
    row.Value,
  ];
  for (const v of candidates) {
    const n = Number(v);
    if (!Number.isNaN(n)) return n;
  }
  return null;
}



function normalizeTimestampKey(dt: Date): string {
  // If your steps_minute really is minute-resolution, normalize to minute key:
  const y = dt.getFullYear();
  const m = `${dt.getMonth() + 1}`.padStart(2, '0');
  const d = `${dt.getDate()}`.padStart(2, '0');
  const hh = `${dt.getHours()}`.padStart(2, '0');
  const mm = `${dt.getMinutes()}`.padStart(2, '0');
  return `${y}-${m}-${d}T${hh}:${mm}`;
}

function preprocessStepsRowsDedupe(rows: Record<string, any>[]): Record<string, any>[] {
  const groups = new Map<string, { row: Record<string, any>; steps: number }[]>();

  for (const row of rows) {
    const steps = extractStepsValue(row);
    if (steps === null) continue;

    const { dt } = extractDayOrDateTime(row);
    const { start, end } = extractInterval(row);

    let key: string | null = null;

    if (dt) {
      key = `DT:${normalizeTimestampKey(dt)}`;
    } else if (start && end) {
      // ✅ include both bounds for interval rows
      key = `INT:${normalizeTimestampKey(start)}->${normalizeTimestampKey(end)}`;
    } else if (start) {
      key = `START:${normalizeTimestampKey(start)}`;
    }

    if (!key) continue;

    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)!.push({ row, steps });
  }

  const out: Record<string, any>[] = [];

  for (const items of groups.values()) {
    const values = items.map((x) => x.steps);
    const first = values[0];
    const allSame = values.every((v) => v === first);

    if (allSame) {
      out.push(items[0].row);
    } else {
      const sum = values.reduce((a, b) => a + b, 0);
      const merged = { ...items[0].row, steps: sum };
      out.push(merged);
    }
  }

  return out;
}

function extractDayOrDateTime(row: Record<string, any>): { day: Date | null; dt: Date | null } {
  const day =
    parseDateFromDay(row.date || row.Date || row.day || row.Day || row.local_date || row.Local_date) ?? null;

  const dt =
    parseDateTime(
      row.minute ||
      row.datetime ||
      row.DateTime ||
      row.timestamp ||
      row.Timestamp ||
      row.time ||
      row.Time
    ) ?? null;

  return { day, dt };
}

function extractInterval(row: Record<string, any>): { start: Date | null; end: Date | null } {
  const start =
    parseDateTime(row.start_datetime || row.start || row.Start || row.begin || row.Begin) ?? null;
  const end =
    parseDateTime(row.end_datetime || row.end || row.End || row.stop || row.Stop) ?? null;
  return { start, end };
}

function addToDay(byDay: Map<string, number>, day: Date, steps: number) {
  const key = dayKey(day);
  byDay.set(key, (byDay.get(key) ?? 0) + steps);
}

/**
 * Aggregate arbitrary steps rows to daily totals.
 * - If row has an explicit day: treat it as daily (add to that day).
 * - Else if row has a single datetime: add to that datetime’s day.
 * - Else if row has start+end: split across days proportionally by overlap duration.
 */
function aggregateStepsToDaily(rows: Record<string, any>[]): StepsDailyAgg {
  rows = preprocessStepsRowsDedupe(rows); 
  // collect raw samples per day (when we have a datetime/day)
  const samplesByDay = new Map<string, { t: number; v: number }[]>();
  const intervalByDay = new Map<string, number>(); // for start/end interval splitting

  const pushSample = (day: Date, dt: Date, v: number) => {
    const key = dayKey(day);
    if (!samplesByDay.has(key)) samplesByDay.set(key, []);
    samplesByDay.get(key)!.push({ t: dt.getTime(), v });
  };

  const addIntervalPortion = (day: Date, steps: number) => {
    const key = dayKey(day);
    intervalByDay.set(key, (intervalByDay.get(key) ?? 0) + steps);
  };

  for (const row of rows) {
    const steps = extractStepsValue(row);
    if (steps === null) continue;

    const { day, dt } = extractDayOrDateTime(row);

    // case 1: explicit day totals
    if (day && !dt) {
      // treat as a single sample on that day
      pushSample(day, day, steps);
      continue;
    }

    // case 2: timestamped samples (minute/event-level)
    if (dt) {
      const d = startOfDay(dt);
      pushSample(d, dt, steps);
      continue;
    }

    // case 3: intervals (start/end)
    const { start, end } = extractInterval(row);
    if (!start || !end) continue;

    const totalMs = end.getTime() - start.getTime();
    if (totalMs <= 0) continue;

    // split across day boundaries proportionally
    let cursor = new Date(start.getTime());
    while (cursor < end) {
      const dayStart = startOfDay(cursor);
      const dayEnd = endOfDay(cursor);

      const segStart = cursor;
      const segEnd = new Date(Math.min(end.getTime(), dayEnd.getTime()));
      const segMs = segEnd.getTime() - segStart.getTime();

      if (segMs > 0) {
        const portion = segMs / totalMs;
        addIntervalPortion(dayStart, steps * portion);
      }

      cursor = new Date(dayStart.getTime());
      cursor.setDate(cursor.getDate() + 1);
      cursor.setHours(0, 0, 0, 0);
    }
  }

  // finalize per-day totals
  const byDay = new Map<string, number>();

  for (const [key, samples] of samplesByDay.entries()) {
    if (samples.length === 0) continue;
    samples.sort((a, b) => a.t - b.t);

    const values = samples.map((s) => s.v).filter((v) => Number.isFinite(v) && v >= 0);
    if (values.length === 0) continue;

    const sum = values.reduce((acc, v) => acc + v, 0);
    const min = Math.min(...values);
    const max = Math.max(...values);

    // heuristic: detect cumulative
    // - many points
    // - mostly non-decreasing
    // - sum much larger than max (summing cumulative blows up)
    // ✅ but ONLY if the magnitude looks like a cumulative counter (hundreds+)
    let nonDecreasing = 0;
    for (let i = 1; i < values.length; i += 1) {
      if (values[i] >= values[i - 1]) nonDecreasing += 1;
    }
    const nonDecreasingRate = values.length > 1 ? nonDecreasing / (values.length - 1) : 1;

    const range = max - min;

    // ✅ NEW: gate cumulative detection on plausible scale
    // Minute-level step counts usually have max ~0–10; cumulative totals are typically hundreds/thousands.
    const cumulativeScaleLikely = max >= 200 || range >= 200;

    const looksCumulative =
      cumulativeScaleLikely &&
      values.length >= 10 &&
      nonDecreasingRate >= 0.8 &&
      max > 0 &&
      sum / max > 3; // key signal: summing cumulative >> max

    const dailyTotal = looksCumulative ? Math.max(0, range) : sum;

    byDay.set(key, dailyTotal);
  }

  // add interval-based contributions (if any)
  for (const [key, v] of intervalByDay.entries()) {
    byDay.set(key, (byDay.get(key) ?? 0) + v);
  }

  const points = Array.from(byDay.entries())
    .map(([key, total]) => ({ x: parseDateFromDay(key)!.getTime(), y: total }))
    .sort((a, b) => a.x - b.x);

  const totals = points.map((p) => p.y);
  return { points, totals, byDay };
}


function toDateRange(start: string, end: string): DateRange | null {
  if (!start || !end) return null;
  const startDate = new Date(`${start}T00:00:00`);
  const endDate = new Date(`${end}T23:59:59`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return null;
  }
  if (startDate > endDate) {
    return null;
  }
  return { start: startDate, end: endDate, labelStart: start, labelEnd: end };
}

function parseDate(value: string | undefined): Date | null {
  if (!value) return null;
  const normalized = value.trim().replace(' ', 'T');
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? null : date;
}

function parseDateFromDay(value: string | undefined): Date | null {
  if (!value) return null;
  const s = value.trim();

  // If datetime, parse + take start-of-day
  if (s.includes('T') || s.includes(' ')) {
    const dt = parseDateTime(s);
    return dt ? startOfDay(dt) : null;
  }

  // Supports "9/22/25" or "09/22/2025"
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/.exec(s);
  if (m) {
    const month = Number(m[1]);
    const day = Number(m[2]);
    let year = Number(m[3]);
    if (year < 100) year += 2000;
    const d = new Date(year, month - 1, day, 0, 0, 0, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  // ISO "YYYY-MM-DD"
  const d = new Date(`${s}T00:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function parseDateTime(value: string | undefined): Date | null {
  if (!value) return null;
  const s = value.trim();

  // MM/DD/YYYY HH:mm[:ss]
  const m1 = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(s);
  if (m1) {
    const month = Number(m1[1]);
    const day = Number(m1[2]);
    let year = Number(m1[3]);
    const hour = Number(m1[4]);
    const minute = Number(m1[5]);
    const second = Number(m1[6] ?? '0');

    if (year < 100) year += 2000;
    const d = new Date(year, month - 1, day, hour, minute, second, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  // YYYY-MM-DD HH:mm[:ss] or YYYY-MM-DDTHH:mm[:ss]
  const m2 = /^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})(?::(\d{2}))?$/.exec(s);
  if (m2) {
    const year = Number(m2[1]);
    const month = Number(m2[2]);
    const day = Number(m2[3]);
    const hour = Number(m2[4]);
    const minute = Number(m2[5]);
    const second = Number(m2[6] ?? '0');

    const d = new Date(year, month - 1, day, hour, minute, second, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  return null;
}

function overlapsRange(start: Date, end: Date, range: DateRange) {
  return !(end < range.start || start > range.end);
}

function withinRangeStart(start: Date, range: DateRange) {
  return start >= range.start && start <= range.end;
}

function addDays(date: Date, days: number) {
  const clone = new Date(date.getTime());
  clone.setDate(clone.getDate() + days);
  return clone;
}

function startOfDay(date: Date) {
  const clone = new Date(date.getTime());
  clone.setHours(0, 0, 0, 0);
  return clone;
}

function endOfDay(date: Date) {
  const clone = new Date(date.getTime());
  clone.setHours(23, 59, 59, 999);
  return clone;
}

function dayKey(date: Date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function quantile(values: number[], q: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (sorted[base + 1] === undefined) return sorted[base];
  return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
}

function iqr(values: number[]): number | null {
  const q1 = quantile(values, 0.25);
  const q3 = quantile(values, 0.75);
  if (q1 === null || q3 === null) return null;
  return q3 - q1;
}

function expectedDaysInRange(range: DateRange): number {
  const start = startOfDay(range.start).getTime();
  const end = startOfDay(range.end).getTime();
  const days = Math.floor((end - start) / 86400000) + 1;
  return Math.max(0, days);
}

function expectedMinutesInRange(range: DateRange): number {
  // inclusive day span * 1440
  return expectedDaysInRange(range) * 1440;
}

function formatPct(p: number | null) {
  if (p === null) return '—';
  return `${(p * 100).toFixed(0)}%`;
}
type QualityFlag = 'ok' | 'low' | 'unknown';

function qualityFlagFromCoverage(coverage: number | null, threshold: number): QualityFlag {
  if (coverage === null) return 'unknown';
  return coverage >= threshold ? 'ok' : 'low';
}

function formatQualityFlag(flag: QualityFlag) {
  if (flag === 'low') return 'LOW QUALITY';
  if (flag === 'ok') return 'OK';
  return 'UNKNOWN';
}

function formatIqr(value: number | null, unit: string) {
  if (value === null) return '—';
  return `${value.toFixed(1)} ${unit}`;
}

function minutesSinceMidnight(date: Date) {
  return date.getHours() * 60 + date.getMinutes() + date.getSeconds() / 60;
}

function formatMinutes(minutes: number | null) {
  if (minutes === null) return '—';
  const rounded = Math.round(minutes);
  const hrs = Math.floor(rounded / 60);
  const mins = Math.abs(rounded % 60);
  return `${`${hrs}`.padStart(2, '0')}:${`${mins}`.padStart(2, '0')}`;
}

function formatDifferenceMinutes(delta: number | null) {
  if (delta === null) return undefined;
  const sign = delta > 0 ? '+' : '';
  return `${sign}${Math.round(delta)} min`;
}

function formatDifferenceHours(delta: number | null) {
  if (delta === null) return undefined;
  const sign = delta > 0 ? '+' : '';
  return `${sign}${delta.toFixed(2)} h`;
}

function formatDifferenceCount(delta: number | null, unit: string) {
  if (delta === null) return undefined;
  const sign = delta > 0 ? '+' : '';
  const value = Math.round(delta);
  return `${sign}${value} ${unit}${Math.abs(value) === 1 ? '' : 's'}`;
}

function parseTimeResponse(value: string | undefined) {
  if (!value) return null;
  const trimmed = value.trim();
  const match = /^(\d{1,2})(:(\d{2})(:(\d{2}))?)?$/.exec(trimmed);
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[3] ?? '0');
  if (Number.isNaN(hours) || Number.isNaN(minutes)) return null;
  if (hours > 23 || minutes > 59) return null;
  return hours * 60 + minutes;
}

function minimalCircularDifference(reference: number | null, comparison: number | null, period = 24 * 60) {
  if (reference === null || comparison === null) return null;
  const raw = reference - comparison;
  const wrapped = ((raw % period) + period) % period;
  return wrapped > period / 2 ? wrapped - period : wrapped;
}

function parseHoursResponse(value: string | undefined) {
  if (!value) return null;
  const trimmed = value.trim().replace(',', '.');
  if (trimmed.includes(':')) {
    const parts = trimmed.split(':');
    const hours = Number(parts[0]);
    const minutes = Number(parts[1] ?? '0');
    if (Number.isNaN(hours) || Number.isNaN(minutes)) return null;
    return hours + minutes / 60;
  }
  const numeric = Number(trimmed);
  if (Number.isNaN(numeric)) return null;
  return numeric;
}

function parseNumericResponse(value: string | undefined) {
  if (!value) return null;
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return null;
  return numeric;
}

function buildAvailabilityMessage(status: DataAvailabilityStatus, base: string) {
  switch (status) {
    case 'present':
      return `${base} available for the selected timeframe.`;
    case 'empty':
      return `${base} found, but no records overlap with the selected timeframe.`;
    case 'error':
      return `${base} could not be parsed.`;
    default:
      return `${base} not found in this workspace.`;
  }
}

function findFile(files: WorkspaceFile[], matcher: (file: WorkspaceFile) => boolean) {
  const prioritized = files.slice().sort((a, b) => a.path.localeCompare(b.path));
  return prioritized.find(matcher);
}

function filterFiles(files: WorkspaceFile[], matcher: (file: WorkspaceFile) => boolean) {
  return files.filter(matcher);
}

function resolveDownloadUrl(downloadUrl: string) {
  try {
    return new URL(downloadUrl, `${BACKEND_URL}/`).toString();
  } catch (err) {
    return downloadUrl;
  }
}

function matchesRangeInPath(path: string, range: DateRange) {
  const startIso = range.labelStart;
  const endIso = range.labelEnd;
  const startCompact = startIso.replace(/-/g, '');
  const endCompact = endIso.replace(/-/g, '');

  return (
    path.includes(startIso) ||
    path.includes(endIso) ||
    // allow YYYYMMDD only if it appears as an actual substring in the path
    path.includes(startCompact) ||
    path.includes(endCompact)
  );
}

function extractJobId(path: string) {
  const match = /jobs\/(.*?)\//i.exec(path);
  return match ? match[1] : null;
}

async function evaluatePSQI(
  workspace: string,
  files: WorkspaceFile[],
  range: DateRange,
  responses: QuestionnaireResponses | null,
): Promise<EvaluationResult> {
  const availability: Record<string, DataAvailability> = {
    rawSleep: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSQI.rawSleep) },
    mergedSleep: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSQI.mergedSleep) },
    metrics: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSQI.metrics) },
    fips: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSQI.fips) },
  };

  const rawSleepFile = findFile(
    files,
    (file) => file.path.toLowerCase().endsWith('sleep_episodes.csv') && !file.path.toLowerCase().includes('merged'),
  );
  const mergedSleepFile = findFile(
    files,
    (file) => file.path.toLowerCase().endsWith('sleep_episodes_merged.csv'),
  );
  const sleepEfficiencyFile = findFile(
    files,
    (file) => file.path.toLowerCase().includes('sleep_efficiency.csv'),
  );
  const sleepDeviationFile = findFile(
    files,
    (file) => file.path.toLowerCase().includes('sleep_deviation_vs_sun.csv'),
  );
  const restingHrFile = findFile(files, (file) => file.path.toLowerCase().includes('resting_hr.csv'));

  const fipsTmpImages = filterFiles(
    files,
    (file) => /(tmp|tpm)_dates_/i.test(file.path) && file.path.toLowerCase().endsWith('.png'),
  );
  const fipsTimingImages = filterFiles(files, (file) => /sleep_timing_and_duration_/i.test(file.path) && file.path.toLowerCase().endsWith('.png'));
  const fipsMidpointImages = filterFiles(files, (file) => /sleep_midpoint_/i.test(file.path) && file.path.toLowerCase().endsWith('.png'));
  const fipsTimelineFiles = filterFiles(
    files,
    (file) => /fips_timeline_/i.test(file.path) && file.path.toLowerCase().endsWith('.csv'),
  );

  const [rawSleepRows, mergedSleepRows, sleepEfficiencyRows, sleepDeviationRows, restingHrRows] = await Promise.all([
    (async () => {
      if (!rawSleepFile) return null;
      try {
        const rows = await parseCsv(rawSleepFile.path, workspace);
        const filtered = rows.filter((row) => {
          const start = parseDate(row.start || row.Start || row.begin);
          const end = parseDate(row.end || row.End || row.stop);
          if (!start || !end) return false;
          return overlapsRange(start, end, range);
        });
        availability.rawSleep = {
          status: filtered.length > 0 ? 'present' : 'empty',
          path: rawSleepFile.path,
          message: buildAvailabilityMessage(filtered.length > 0 ? 'present' : 'empty', REQUIRED_DATASETS_PSQI.rawSleep),
        };
        return filtered;
      } catch (err) {
        availability.rawSleep = {
          status: 'error',
          path: rawSleepFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSQI.rawSleep),
        };
        return null;
      }
    })(),
    (async () => {
      if (!mergedSleepFile) return null;
      try {
        const rows = await parseCsv(mergedSleepFile.path, workspace);
        const filtered = rows.filter((row) => {
          const start = parseDate(row.start || row.Start || row.begin);
          if (!start) return false;
          return withinRangeStart(start, range);
        });
        availability.mergedSleep = {
          status: filtered.length > 0 ? 'present' : 'empty',
          path: mergedSleepFile.path,
          message: buildAvailabilityMessage(filtered.length > 0 ? 'present' : 'empty', REQUIRED_DATASETS_PSQI.mergedSleep),
        };
        return filtered;
      } catch (err) {
        availability.mergedSleep = {
          status: 'error',
          path: mergedSleepFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSQI.mergedSleep),
        };
        return null;
      }
    })(),
    (async () => {
      if (!sleepEfficiencyFile) return null;
      try {
        const rows = await parseCsv(sleepEfficiencyFile.path, workspace);
        const filtered = rows.filter((row) => {
          const day = parseDateFromDay(row.date || row.Date);
          if (!day) return false;
          return withinRangeStart(day, range);
        });
        const metricsStatus = filtered.length > 0 ? 'present' : 'empty';
        availability.metrics = {
          status: metricsStatus,
          path: sleepEfficiencyFile.path,
          message: buildAvailabilityMessage(metricsStatus, REQUIRED_DATASETS_PSQI.metrics),
        };
        return filtered;
      } catch (err) {
        availability.metrics = {
          status: 'error',
          path: sleepEfficiencyFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSQI.metrics),
        };
        return null;
      }
    })(),
    (async () => {
      if (!sleepDeviationFile) return null;
      try {
        const rows = await parseCsv(sleepDeviationFile.path, workspace);
        const filtered = rows.filter((row) => {
          const day = parseDateFromDay(row.date || row.Date);
          if (!day) return false;
          return withinRangeStart(day, range);
        });
        if (filtered.length > 0) {
          const current = availability.metrics;
          if (current.status !== 'error') {
            availability.metrics = {
              status: 'present',
              path: [current.path, sleepDeviationFile.path].filter(Boolean).join(', '),
              message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSQI.metrics),
            };
          }
        }
        return filtered;
      } catch (err) {
        availability.metrics = {
          status: 'error',
          path: sleepDeviationFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSQI.metrics),
        };
        return null;
      }
    })(),
    (async () => {
      if (!restingHrFile) return null;
      try {
        const rows = await parseCsv(restingHrFile.path, workspace);
        const filtered = rows.filter((row) => {
          const day = parseDateFromDay(row.date || row.Date);
          if (!day) return false;
          return withinRangeStart(day, range);
        });
        if (filtered.length > 0) {
          const current = availability.metrics;
          if (current.status !== 'error') {
            availability.metrics = {
              status: 'present',
              path: [current.path, restingHrFile.path].filter(Boolean).join(', '),
              message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSQI.metrics),
            };
          }
        }
        return filtered;
      } catch (err) {
        availability.metrics = {
          status: 'error',
          path: restingHrFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSQI.metrics),
        };
        return null;
      }
    })(),
  ]);

  const fipsTimelineCoverages: FipsTimelineCoverage[] = await Promise.all(
    fipsTimelineFiles.map(async (file) => {
      try {
        const rows = await parseCsv(file.path, workspace);
        const datetimes = rows
          .map((row) => parseDateTime(row.datetime ?? row.DateTime ?? row.date ?? row.Date ?? row.timestamp ?? row.Timestamp))
          .filter((value): value is Date => value !== null)
          .sort((a, b) => a.getTime() - b.getTime());
        if (datetimes.length === 0) {
          return {
            path: file.path,
            jobId: extractJobId(file.path),
            matches: false,
            startAligned: false,
            endAligned: false,
            error: 'No datetime entries present in FIPS timeline.',
          };
        }
        const startWindowStart = startOfDay(addDays(range.start, -1));
        const startWindowEnd = endOfDay(addDays(range.start, 1));
        const endWindowStart = startOfDay(addDays(range.end, -1));
        const endWindowEnd = endOfDay(addDays(range.end, 1));
        const startAligned = datetimes.some((value) => value >= startWindowStart && value <= startWindowEnd);
        const endAligned = datetimes.some((value) => value >= endWindowStart && value <= endWindowEnd);
        return {
          path: file.path,
          jobId: extractJobId(file.path),
          matches: startAligned && endAligned,
          startAligned,
          endAligned,
          earliest: datetimes[0],
          latest: datetimes[datetimes.length - 1],
        };
      } catch (err) {
        return {
          path: file.path,
          jobId: extractJobId(file.path),
          matches: false,
          startAligned: false,
          endAligned: false,
          error: err instanceof Error ? err.message : String(err),
        };
      }
    }),
  );

  const jobIdsWithCoverage = new Set<string>();
  if (sleepDeviationRows && sleepDeviationRows.length > 0 && sleepDeviationFile) {
    const jobId = extractJobId(sleepDeviationFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }
  if (restingHrRows && restingHrRows.length > 0 && restingHrFile) {
    const jobId = extractJobId(restingHrFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }
  if (sleepEfficiencyRows && sleepEfficiencyRows.length > 0 && sleepEfficiencyFile) {
    const jobId = extractJobId(sleepEfficiencyFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }
  if (mergedSleepRows && mergedSleepRows.length > 0 && mergedSleepFile) {
    const jobId = extractJobId(mergedSleepFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }
  if (rawSleepRows && rawSleepRows.length > 0 && rawSleepFile) {
    const jobId = extractJobId(rawSleepFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }

  const timelineInfoByJob = new Map<string, FipsTimelineCoverage>();
  const timelineKnownJobs = new Set<string>();
  const timelineMatchedJobs = new Set<string>();
  const timelineIssues: string[] = [];
  const timelineErrorsWithoutJob: string[] = [];

  for (const coverage of fipsTimelineCoverages) {
    const { jobId } = coverage;
    if (jobId) {
      timelineInfoByJob.set(jobId, coverage);
      timelineKnownJobs.add(jobId);
      if (coverage.matches) {
        timelineMatchedJobs.add(jobId);
      } else if (coverage.error) {
        timelineIssues.push(`${coverage.path} (${coverage.error})`);
      } else {
        const unmet: string[] = [];
        if (!coverage.startAligned) unmet.push('start ±1 day window not covered');
        if (!coverage.endAligned) unmet.push('end ±1 day window not covered');
        timelineIssues.push(`${coverage.path} (${unmet.join(', ') || 'no datetime overlap within tolerance'})`);
      }
    } else if (coverage.error) {
      timelineErrorsWithoutJob.push(`${coverage.path} (${coverage.error})`);
    }
  }

  const matchesTimeframe = (path: string) => {
    const jobId = extractJobId(path);
    if (jobId && timelineKnownJobs.has(jobId)) {
      return timelineMatchedJobs.has(jobId);
    }
    if (matchesRangeInPath(path, range)) {
      return true;
    }
    return jobId ? jobIdsWithCoverage.has(jobId) : false;
  };

  const alertnessImages = fipsTmpImages.filter((file) => matchesTimeframe(file.path));
  const contextImages = [...fipsTimingImages, ...fipsMidpointImages].filter((file) => matchesTimeframe(file.path));

  const referencedFiles = [...alertnessImages, ...contextImages];
  const referencedJobIds = new Set<string>();
  referencedFiles.forEach((file) => {
    const jobId = extractJobId(file.path);
    if (jobId) {
      referencedJobIds.add(jobId);
    }
  });

  const timelineSupportPaths = new Set<string>();
  const timelineSupportNotes: string[] = [];
  referencedJobIds.forEach((jobId) => {
    const coverage = timelineInfoByJob.get(jobId);
    if (!coverage) return;
    const windowSummary = `start window ${coverage.startAligned ? 'met' : 'missed'}, end window ${coverage.endAligned ? 'met' : 'missed'}`;
    if (coverage.matches) {
      timelineSupportPaths.add(coverage.path);
      timelineSupportNotes.push(
        `FIPS_timeline coverage (${coverage.path}) confirms datetime alignment with ${range.labelStart} and ${range.labelEnd} (${windowSummary}).`,
      );
    } else if (coverage.error) {
      timelineSupportNotes.push(`FIPS_timeline file ${coverage.path} could not be evaluated (${coverage.error}).`);
    } else {
      timelineSupportNotes.push(`FIPS_timeline coverage (${coverage.path}) with ${windowSummary}).`);
    }
  });

  if (alertnessImages.length > 0 || contextImages.length > 0) {
    const combinedPaths = new Set<string>(referencedFiles.map((file) => file.path));
    timelineSupportPaths.forEach((path) => combinedPaths.add(path));
    availability.fips = {
      status: 'present',
      path: Array.from(combinedPaths).join(', '),
      message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSQI.fips),
    };
  } else if (fipsTmpImages.length + fipsTimingImages.length + fipsMidpointImages.length > 0) {
    availability.fips = {
      status: 'empty',
      path: [...fipsTmpImages, ...fipsTimingImages, ...fipsMidpointImages].map((file) => file.path).join(', '),
      message: buildAvailabilityMessage('empty', REQUIRED_DATASETS_PSQI.fips),
    };
  }

  const charts: ChartBundle[] = [];
  if (sleepDeviationRows && sleepDeviationRows.length > 0) {
    const columns: ChartColumn[] = [];
    const onset = sleepDeviationRows
      .map((row) => {
        const day = parseDateFromDay(row.date || row.Date);
        const value = Number(row.onset_dev_min ?? row.Onset_dev_min ?? row.onset_dev_minute);
        if (!day || Number.isNaN(value)) return null;
        return { x: day.getTime(), y: value };
      })
      .filter((item): item is { x: number; y: number } => item !== null)
      .sort((a, b) => a.x - b.x);
    if (onset.length > 0) {
      columns.push({ label: 'Onset deviation (min)', data: onset, color: paletteForLabel('onset_dev_min') });
    }
    const offset = sleepDeviationRows
      .map((row) => {
        const day = parseDateFromDay(row.date || row.Date);
        const value = Number(row.offset_dev_min ?? row.Offset_dev_min ?? row.offset_dev_minute);
        if (!day || Number.isNaN(value)) return null;
        return { x: day.getTime(), y: value };
      })
      .filter((item): item is { x: number; y: number } => item !== null)
      .sort((a, b) => a.x - b.x);
    if (offset.length > 0) {
      columns.push({ label: 'Offset deviation (min)', data: offset, color: paletteForLabel('offset_dev_min') });
    }
    if (columns.length > 0 && sleepDeviationFile) {
      const expectedDays = expectedDaysInRange(range);
      const onsetValues = onset.map((p) => p.y);
      const offsetValues = offset.map((p) => p.y);
      const onsetCov = expectedDays > 0 ? onsetValues.length / expectedDays : null;
      const offsetCov = expectedDays > 0 ? offsetValues.length / expectedDays : null;
      const onsetIqr = iqr(onsetValues);
      const offsetIqr = iqr(offsetValues);
      const deviationQualityNote =
        ` Coverage onset ${formatPct(onsetCov)}, offset ${formatPct(offsetCov)}; ` +
        `IQR onset ${formatIqr(onsetIqr, 'min')}, offset ${formatIqr(offsetIqr, 'min')}.`;
      charts.push({
        filePath: sleepDeviationFile.path,
        title: 'Sleep deviation vs. sunrise',
        columns,
        reason:
          'Daily deviation of sleep onset/offset from local sunrise, sourced from metrics project sleep_deviation_vs_sun.csv and filtered to the questionnaire timeframe.' +
          ` ${deviationQualityNote}`,
      });
    }
  }

  if (restingHrRows && restingHrRows.length > 0 && restingHrFile) {
    const points = restingHrRows
      .map((row) => {
        const day = parseDateFromDay(row.date || row.Date);
        const value = Number(row.resting_bpm ?? row.Resting_bpm ?? row.resting_hr ?? row.bpm);
        if (!day || Number.isNaN(value)) return null;
        return { x: day.getTime(), y: value };
      })
      .filter((item): item is { x: number; y: number } => item !== null)
      .sort((a, b) => a.x - b.x);
    if (points.length > 0) {
      charts.push({
        filePath: restingHrFile.path,
        title: 'Resting heart rate',
        columns: [{ label: 'Resting BPM', data: points, color: paletteForLabel('resting_bpm') }],
        reason:
          'Daily resting heart rate values from metrics project resting_hr.csv limited to the questionnaire timeframe.',
      });
    }
  }

  if (sleepEfficiencyRows && sleepEfficiencyRows.length > 0 && sleepEfficiencyFile) {
    const createSeries = (column: string, label: string) =>
      sleepEfficiencyRows
        .map((row) => {
          const day = parseDateFromDay(row.date || row.Date);
          const value = Number(row[column as keyof typeof row]);
          if (!day || Number.isNaN(value)) return null;
          return { x: day.getTime(), y: value };
        })
        .filter((item): item is { x: number; y: number } => item !== null)
        .sort((a, b) => a.x - b.x);

    const efficiencySeries = createSeries('efficiency_0_100', 'Sleep efficiency (0-100)');
    const sleepMinutesSeries = createSeries('sleep_min', 'Sleep minutes');
    const restlessSeries = createSeries('restless_min', 'Restless minutes');

    const columns: ChartColumn[] = [];
    if (efficiencySeries.length > 0) {
      columns.push({ label: 'Sleep efficiency (%)', data: efficiencySeries, color: paletteForLabel('efficiency_0_100') });
    }
    if (sleepMinutesSeries.length > 0) {
      columns.push({ label: 'Sleep minutes', data: sleepMinutesSeries, color: paletteForLabel('sleep_min') });
    }
    if (restlessSeries.length > 0) {
      columns.push({ label: 'Restless minutes', data: restlessSeries, color: paletteForLabel('restless_min') });
    }

    if (columns.length > 0) {
      charts.push({
        filePath: sleepEfficiencyFile.path,
        title: 'Sleep efficiency summary',
        columns,
        reason:
          'Metrics project sleep_efficiency.csv values filtered to the timeframe help contextualize subjective sleep ratings.',
      });
    }
  }

  if (!responses) {
    return {
      availability,
      comparisons: null,
      charts,
      alertnessImages,
      contextImages,
    };
  }

  const comparisons: ComparisonItem[] = [];
  const expectedDays = expectedDaysInRange(range);

  const longestEpisodesByDay: Map<string, SleepEpisode> = new Map();
  if (mergedSleepRows) {
    for (const row of mergedSleepRows) {
      const start = parseDate(row.start || row.Start || row.begin);
      const end = parseDate(row.end || row.End || row.stop);
      if (!start || !end) continue;
      if (!withinRangeStart(start, range)) continue;
      const durationMinutes = (end.getTime() - start.getTime()) / 60000;
      if (durationMinutes <= 0) continue;
      const key = dayKey(start);
      const existing = longestEpisodesByDay.get(key);
      if (!existing || durationMinutes > existing.durationMinutes) {
        longestEpisodesByDay.set(key, { start, end, durationMinutes });
      }
    }
  }

  const startMinutes = Array.from(longestEpisodesByDay.values()).map((episode) => minutesSinceMidnight(episode.start));
  const endMinutes = Array.from(longestEpisodesByDay.values()).map((episode) => minutesSinceMidnight(episode.end));
  const medianStart = median(startMinutes);
  const medianEnd = median(endMinutes);
  const bedtimeResponse = parseTimeResponse(responses['bedtime']);
  const waketimeResponse = parseTimeResponse(responses['waketime']);

  if (responses['bedtime']) {
    const differenceMinutes = minimalCircularDifference(medianStart, bedtimeResponse);
    comparisons.push({
      field: 'Bedtime',
      response: responses['bedtime'],
      computed: formatMinutes(medianStart),
      difference: formatDifferenceMinutes(differenceMinutes),
      status:
        bedtimeResponse !== null && medianStart !== null && Math.abs(differenceMinutes ?? Infinity) <= 30
          ? 'match'
          : medianStart === null
          ? 'unavailable'
          : 'mismatch',
      reason:
        'Median start time of the longest merged sleep episode per day within the timeframe, calculated from sleep_episodes_merged.csv to compare against the questionnaire-reported habitual bedtime.',
    });
  }

  if (responses['waketime']) {
    const differenceMinutes = minimalCircularDifference(medianEnd, waketimeResponse);
    comparisons.push({
      field: 'Wake time',
      response: responses['waketime'],
      computed: formatMinutes(medianEnd),
      difference: formatDifferenceMinutes(differenceMinutes),
      status:
        waketimeResponse !== null && medianEnd !== null && Math.abs(differenceMinutes ?? Infinity) <= 30
          ? 'match'
          : medianEnd === null
          ? 'unavailable'
          : 'mismatch',
      reason:
        'Median end time of the longest merged sleep episode per day over the timeframe, derived from sleep_episodes_merged.csv, contrasted with the reported wake time.',
    });
  }

  if (responses['sleeptime']) {
    const responseHours = parseHoursResponse(responses['sleeptime']);
    const sleepMinutes =
      sleepEfficiencyRows
        ?.map((row) => Number(row.sleep_min ?? row.Sleep_min))
        .filter((value) => !Number.isNaN(value)) ?? [];

    const medianSleepMinutes = median(sleepMinutes);
    const computedHours = medianSleepMinutes !== null ? medianSleepMinutes / 60 : null;

    // stability metrics
    const sleepIqrMin = iqr(sleepMinutes);
    const sleepCoverage = expectedDays > 0 ? sleepMinutes.length / expectedDays : null;
    const sleepQualitySuffix =
      ` (coverage ${formatPct(sleepCoverage)}, IQR ${formatIqr(sleepIqrMin, 'min')})`;

    const sleepFlag = qualityFlagFromCoverage(sleepCoverage, DAILY_COVERAGE_THRESHOLD);
    const sleepWarning =
      sleepFlag === 'low'
        ? ` Data quality: LOW QUALITY (only ${formatPct(sleepCoverage)} of days have sleep_min).`
        : '';

    const diffHours = responseHours !== null && computedHours !== null ? computedHours - responseHours : null;
    comparisons.push({
      field: 'Sleep duration',
      response: responses['sleeptime'],
      computed: computedHours !== null ? `${computedHours.toFixed(2)} h${sleepQualitySuffix}` : `—${sleepQualitySuffix}`,
      difference: formatDifferenceHours(diffHours),
      status:
        responseHours !== null && computedHours !== null && Math.abs(diffHours ?? Infinity) <= 0.5
          ? 'match'
          : computedHours === null
          ? 'unavailable'
          : 'mismatch',
      reason:
        'Median nightly sleep duration sourced from sleep_efficiency.csv (sleep_min column) compared to the self-reported average hours of sleep.' + sleepWarning,
    });
  }

  if (responses['wokeup']) {
    // Interpreting questionnaire response as:
    // "In how many nights did you wake up at least once?"
    const responseCount = parseNumericResponse(responses['wokeup']);

    let nightsWithAwakeningGap = 0;
    let nightsObserved = 0;

    // Helper: assign episodes to a "night"
    // Anything between 00:00–11:59 counts toward the previous night's key.
    const nightKey = (dt: Date) => {
      const shifted = new Date(dt.getTime());
      if (shifted.getHours() < 12) shifted.setDate(shifted.getDate() - 1);
      return dayKey(shifted);
    };

    if (rawSleepRows) {
      const byNight = new Map<string, SleepEpisode[]>();

      for (const row of rawSleepRows) {
        const start = parseDate(row.start || row.Start || row.begin);
        const end = parseDate(row.end || row.End || row.stop);
        if (!start || !end) continue;

        // Keep your existing timeframe rule:
        // you used withinRangeStart(start, range)
        if (!withinRangeStart(start, range)) continue;

        const key = nightKey(start);
        if (!byNight.has(key)) byNight.set(key, []);
        byNight.get(key)!.push({
          start,
          end,
          durationMinutes: (end.getTime() - start.getTime()) / 60000,
        });
      }

      nightsObserved = byNight.size;

      for (const episodes of byNight.values()) {
        episodes.sort((a, b) => a.start.getTime() - b.start.getTime());

        // Count this night as having an "awakening" if it has at least one gap in (5, 30) minutes
        let hasAwakeningGap = false;

        for (let i = 0; i < episodes.length - 1; i += 1) {
          const gapMinutes =
            (episodes[i + 1].start.getTime() - episodes[i].end.getTime()) / 60000;

          if (gapMinutes > 5 && gapMinutes < 30) {
            hasAwakeningGap = true;
            break;
          }
        }

        if (hasAwakeningGap) nightsWithAwakeningGap += 1;
      }
    }

    const delta =
      responseCount !== null ? nightsWithAwakeningGap - responseCount : null;

    const nightsNote =
      nightsObserved > 0
        ? ` (${nightsWithAwakeningGap}/${nightsObserved} nights = ${formatPct(
            nightsWithAwakeningGap / nightsObserved,
          )})`
        : '';

    comparisons.push({
      field: 'Nights with ≥1 awakening',
      response: responses['wokeup'],
      computed: `${nightsWithAwakeningGap}${nightsNote}`,
      difference: formatDifferenceCount(delta, 'night'),
      status:
        responseCount !== null && rawSleepRows
          ? nightsWithAwakeningGap === responseCount
            ? 'match'
            : 'mismatch'
          : rawSleepRows
          ? 'info'
          : 'unavailable',
      reason:
        'Counts the number of nights within the timeframe where raw sleep episodes contain at least one intra-night gap > 5 min and < 30 min (from sleep_episodes.csv). Interpreted as “nights with at least one awakening-like interruption”, not the total number of awakenings.',
    });
  }

  if (responses['sleepquality']) {
    const efficiencyValues =
      sleepEfficiencyRows?.map((row) => Number(row.efficiency_0_100 ?? row.Efficiency_0_100)).filter((value) => !Number.isNaN(value)) ?? [];
    const medianEfficiency = median(efficiencyValues);

    // stability metrics
    const effIqr = iqr(efficiencyValues);
    const effCoverage = expectedDays > 0 ? efficiencyValues.length / expectedDays : null;
    const effQualitySuffix =
      ` (coverage ${formatPct(effCoverage)}, IQR ${formatIqr(effIqr, '%')})`;


    const effFlag = qualityFlagFromCoverage(effCoverage, DAILY_COVERAGE_THRESHOLD);
    const effWarning =
      effFlag === 'low'
        ? ` Data quality: LOW QUALITY (only ${formatPct(effCoverage)} of days have efficiency values).`
        : '';

    comparisons.push({
      field: 'Sleep quality',
      response: responses['sleepquality'],
      computed: medianEfficiency !== null ? `${medianEfficiency.toFixed(1)} % efficiency${effQualitySuffix}` : `—${effQualitySuffix}`,
      difference: undefined,
      status: medianEfficiency === null ? 'unavailable' : 'info',
      reason:
          'Median objective sleep efficiency (0–100%) from sleep_efficiency.csv presented alongside the ordinal PSQI quality rating for contextual interpretation.' + effWarning,
    });
  }

  if (responses['alertnessdifficult']) {
    const qualitativeNotes: string[] = [];
    const filenameAligned = alertnessImages.some((file) => matchesRangeInPath(file.path, range));
    if (alertnessImages.length > 0) {
      qualitativeNotes.push('TMP/TPM_dates alertness trend plotted below for qualitative comparison.');
      if (timelineSupportPaths.size > 0) {
        qualitativeNotes.push(
          `Timeframe validated via ${Array.from(timelineSupportPaths).join(', ')}.`,
        );
      } else if (filenameAligned) {
        qualitativeNotes.push('Filenames include the requested dates, providing direct timeframe confirmation.');
      } else if (referencedJobIds.size > 0) {
        qualitativeNotes.push('Chart associated to biometric exports through matching job metadata.');
      }
    } else {
      qualitativeNotes.push('No matching TMP/TPM_dates_* FIPS alertness chart detected for this timeframe.');
      if (timelineIssues.length > 0 || timelineErrorsWithoutJob.length > 0) {
        qualitativeNotes.push(
          `FIPS timeline evaluation issues: ${[...timelineIssues, ...timelineErrorsWithoutJob].join('; ') || 'no timeline coverage within tolerance.'}`,
        );
      }
    }
    if (timelineSupportNotes.length > 0) {
      qualitativeNotes.push(...timelineSupportNotes);
    }
    const imageNote = qualitativeNotes.join(' ');
    comparisons.push({
      field: 'Daytime alertness difficulty',
      response: responses['alertnessdifficult'],
      computed: alertnessImages.length > 0 ? 'FIPS alertness chart available' : '—',
      difference: undefined,
      status: alertnessImages.length > 0 ? 'info' : 'unavailable',
      reason:
        `Qualitative check against FIPS TMP/TPM_dates image (alertness timeline) for the same timeframe. ${imageNote}`,
    });
  }

  return {
    availability,
    comparisons,
    charts,
    alertnessImages,
    contextImages,
  };
}


async function evaluateMFI(
  workspace: string,
  files: WorkspaceFile[],
  range: DateRange,
  responses: QuestionnaireResponses | null,
  ): Promise<EvaluationResult> {
  const availability: Record<string, DataAvailability> = {
    steps: {
      status: 'missing',
      message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_MFI.steps),
    },
    hrCosinor: {
      status: 'missing',
      message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_MFI.hrCosinor),
    },
    sleepEfficiency: {
      status: 'missing',
      message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_MFI.sleepEfficiency),
    },
    fipsFatigue: {
      status: 'missing',
      message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_MFI.fipsFatigue),
    },
  };

  // --- locate relevant files ---

  const stepsFile =
    // 1) Prefer interpolated minute steps (your canonical location)
    findFile(
      files,
      (file) =>
        file.path.toLowerCase().includes('/outputs/interpolated/') &&
        file.path.toLowerCase().endsWith('steps_minute.csv'),
    ) ??
    // 2) Fallback: any steps_minute.csv anywhere
    findFile(files, (file) => file.path.toLowerCase().endsWith('steps_minute.csv')) ??
    // 3) Fallback: any steps.csv (interpolated daily or raw)
    findFile(files, (file) => file.path.toLowerCase().endsWith('steps.csv'));

  const hrCosinorFile = findFile(
    files,
    (file) => file.path.toLowerCase().includes('hr_cosinor'),
  );

  const sleepEfficiencyFile = findFile(
    files,
    (file) => file.path.toLowerCase().includes('sleep_efficiency.csv'),
  );

  // Use the same FIPS artefacts as PSQI: TMP_dates, sleep timing & midpoint
  const fipsFatigueImages = filterFiles(
    files,
    (file) =>
      (
        /(tmp|tpm)_dates_/i.test(file.path) ||
        /sleep_timing_and_duration_/i.test(file.path) ||
        /sleep_midpoint_/i.test(file.path)
      ) &&
      file.path.toLowerCase().endsWith('.png'),
  );

  // --- parse CSVs (steps, hr cosinor, sleep efficiency) ---

  const [stepsAggResult, hrCosinorRows, sleepEfficiencyRows] = await Promise.all([
    
    // STEPS
    (async () => {
      if (!stepsFile) return null;
      try {
        const rowsAll = await parseCsv(stepsFile.path, workspace);

        // Aggregate FIRST (more robust than trying to filter raw rows)
        const aggAll = aggregateStepsToDaily(rowsAll);

        // Now filter aggregated daily points to the questionnaire range
        const pointsInRange = aggAll.points.filter((p) =>
          withinRangeStart(new Date(p.x), range),
        );

        availability.steps = {
          status: pointsInRange.length > 0 ? 'present' : 'empty',
          path: stepsFile.path,
          message: buildAvailabilityMessage(
            pointsInRange.length > 0 ? 'present' : 'empty',
            REQUIRED_DATASETS_MFI.steps,
          ),
        };

        // Return the aggregated object so later code can use it directly
        return {
          aggAll,
          pointsInRange,
        };
      } catch (err) {
        availability.steps = {
          status: 'error',
          path: stepsFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_MFI.steps),
        };
        return null;
      }
    })(),

    // --- hr cosinor ---
    (async () => {
      if (!hrCosinorFile) return null;
      try {
        const rows = await parseCsv(hrCosinorFile.path, workspace);
        const filtered = rows.filter((row) => {
          const day = parseDateFromDay(row.date || row.Date);
          if (!day) return false;
          return withinRangeStart(day, range);
        });

        availability.hrCosinor = {
          status: filtered.length > 0 ? 'present' : 'empty',
          path: hrCosinorFile.path,
          message: buildAvailabilityMessage(
            filtered.length > 0 ? 'present' : 'empty',
            REQUIRED_DATASETS_MFI.hrCosinor,
          ),
        };

        return filtered;
      } catch (err) {
        availability.hrCosinor = {
          status: 'error',
          path: hrCosinorFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_MFI.hrCosinor),
        };
        return null;
      }
    })(),

    //SLEEP EFFICIENCY
    (async () => {
      if (!sleepEfficiencyFile) return null;
      try {
        const rows = await parseCsv(sleepEfficiencyFile.path, workspace);
        const filtered = rows.filter((row) => {
          const day = parseDateFromDay(row.date || row.Date);
          if (!day) return false;
          return withinRangeStart(day, range);
        });
        availability.sleepEfficiency = {
          status: filtered.length > 0 ? 'present' : 'empty',
          path: sleepEfficiencyFile.path,
          message: buildAvailabilityMessage(
            filtered.length > 0 ? 'present' : 'empty',
            REQUIRED_DATASETS_MFI.sleepEfficiency,
          ),
        };
        return filtered;
      } catch (err) {
        availability.sleepEfficiency = {
          status: 'error',
          path: sleepEfficiencyFile.path,
          message: buildAvailabilityMessage('error', REQUIRED_DATASETS_MFI.sleepEfficiency),
        };
        return null;
      }
    })(),
  ]);

    // --- link MFI metrics to their jobIds (for FIPS timeline matching) ---
  const jobIdsWithCoverage = new Set<string>();

  if (stepsAggResult && stepsAggResult.pointsInRange.length > 0 && stepsFile) {
    const jobId = extractJobId(stepsFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }

  if (hrCosinorRows && hrCosinorRows.length > 0 && hrCosinorFile) {
    const jobId = extractJobId(hrCosinorFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }

  if (sleepEfficiencyRows && sleepEfficiencyRows.length > 0 && sleepEfficiencyFile) {
    const jobId = extractJobId(sleepEfficiencyFile.path);
    if (jobId) jobIdsWithCoverage.add(jobId);
  }


  const fipsTimelineFiles = filterFiles(
    files,
    (file) =>
      /fips_timeline_/i.test(file.path) &&
      file.path.toLowerCase().endsWith('.csv'),
  );

  const fipsTimelineCoverages: FipsTimelineCoverage[] = await Promise.all(
    fipsTimelineFiles.map(async (file) => {
      try {
        const rows = await parseCsv(file.path, workspace);
        const datetimes = rows
          .map((row) =>
            parseDateTime(
              row.datetime ??
                row.DateTime ??
                row.date ??
                row.Date ??
                row.timestamp ??
                row.Timestamp,
            ),
          )
          .filter((v): v is Date => v !== null)
          .sort((a, b) => a.getTime() - b.getTime());

        if (datetimes.length === 0) {
          return {
            path: file.path,
            jobId: extractJobId(file.path),
            matches: false,
            startAligned: false,
            endAligned: false,
            error: 'No datetime entries present in FIPS timeline.',
          };
        }

        const startWindowStart = startOfDay(addDays(range.start, -1));
        const startWindowEnd = endOfDay(addDays(range.start, 1));
        const endWindowStart = startOfDay(addDays(range.end, -1));
        const endWindowEnd = endOfDay(addDays(range.end, 1));

        const startAligned = datetimes.some(
          (d) => d >= startWindowStart && d <= startWindowEnd,
        );
        const endAligned = datetimes.some(
          (d) => d >= endWindowStart && d <= endWindowEnd,
        );

        return {
          path: file.path,
          jobId: extractJobId(file.path),
          matches: startAligned && endAligned,
          startAligned,
          endAligned,
          earliest: datetimes[0],
          latest: datetimes[datetimes.length - 1],
        };
      } catch (err) {
        return {
          path: file.path,
          jobId: extractJobId(file.path),
          matches: false,
          startAligned: false,
          endAligned: false,
          error: err instanceof Error ? err.message : String(err),
        };
      }
    }),
  );

  const timelineInfoByJob = new Map<string, FipsTimelineCoverage>();
  const timelineKnownJobs = new Set<string>();
  const timelineMatchedJobs = new Set<string>();

  for (const coverage of fipsTimelineCoverages) {
    const { jobId } = coverage;
    if (!jobId) continue;
    timelineInfoByJob.set(jobId, coverage);
    timelineKnownJobs.add(jobId);
    if (coverage.matches) {
      timelineMatchedJobs.add(jobId);
    }
  }

  const matchesTimeframe = (path: string) => {
    const jobId = extractJobId(path);
    if (jobId && timelineKnownJobs.has(jobId)) {
      return timelineMatchedJobs.has(jobId);
    }
    // fallback: date range directly in filename
    if (matchesRangeInPath(path, range)) {
      return true;
    }
    return jobId ? jobIdsWithCoverage.has(jobId) : false;
  };


   // --- FIPS fatigue availability with jobId + timeline logic (like PSQI) ---

  const alignedFipsImages = fipsFatigueImages.filter((file) =>
    matchesTimeframe(file.path),
  );

  if (fipsFatigueImages.length > 0) {
    const effective = alignedFipsImages.length > 0 ? alignedFipsImages : fipsFatigueImages;

    availability.fipsFatigue = {
      status: alignedFipsImages.length > 0 ? 'present' : 'empty',
      path: effective.map((f) => f.path).join(', '),
      message: buildAvailabilityMessage(
        alignedFipsImages.length > 0 ? 'present' : 'empty',
        REQUIRED_DATASETS_MFI.fipsFatigue,
      ),
    };
  } else {
    availability.fipsFatigue = {
      status: 'missing',
      message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_MFI.fipsFatigue),
    };
  }

  // split into alertness vs context images using the aligned ones
  const mfiAlertnessImages = alignedFipsImages.filter((file) =>
    /(tmp|tpm)_dates_/i.test(file.path),
  );
  const mfiContextImages = alignedFipsImages.filter(
    (file) => !/(tmp|tpm)_dates_/i.test(file.path),
  );

  // --- build charts ---

  const charts: ChartBundle[] = [];

  // Daily steps chart (aggregate to daily totals, regardless of granularity)
  let dailyStepTotals: number[] = [];

  if (stepsAggResult && stepsFile) {
    const { aggAll, pointsInRange } = stepsAggResult;

    dailyStepTotals = pointsInRange.map((p) => p.y);

    if (pointsInRange.length > 0) {
      charts.push({
        filePath: stepsFile.path,
        title: 'Daily steps',
        columns: [
          {
            label: 'Steps per day',
            data: pointsInRange,
            color: paletteForLabel('steps'),
          },
        ],
        reason:
          'Daily step totals aggregated from steps_minute.csv / steps.csv and then filtered to the questionnaire timeframe. Partial overlap is expected and is treated as valid coverage.',
      });
    }
  }

  // HR cosinor (mesor & amplitude) chart
  let mesorValues: number[] = [];
  let amplitudeValues: number[] = [];

  if (hrCosinorRows && hrCosinorRows.length > 0 && hrCosinorFile) {
    const mesorSeries: { x: number; y: number }[] = [];
    const ampSeries: { x: number; y: number }[] = [];

    hrCosinorRows.forEach((row) => {
      const day = parseDateFromDay(row.date || row.Date);
      if (!day) return;

      const mesor = Number(row.mesor ?? row.Mesor);
      const amp = Number(row.amplitude ?? row.Amplitude ?? row.amp ?? row.Amp);

      if (!Number.isNaN(mesor)) {
        mesorSeries.push({ x: day.getTime(), y: mesor });
        mesorValues.push(mesor);
      }
      if (!Number.isNaN(amp)) {
        ampSeries.push({ x: day.getTime(), y: amp });
        amplitudeValues.push(amp);
      }
    });

    const columns: ChartColumn[] = [];
    if (mesorSeries.length > 0) {
      mesorSeries.sort((a, b) => a.x - b.x);
      columns.push({
        label: 'HR mesor (bpm)',
        data: mesorSeries,
        color: paletteForLabel('mesor'),
      });
    }
    if (ampSeries.length > 0) {
      ampSeries.sort((a, b) => a.x - b.x);
      columns.push({
        label: 'HR amplitude (bpm)',
        data: ampSeries,
        color: paletteForLabel('amplitude'),
      });
    }

    if (columns.length > 0) {
      charts.push({
        filePath: hrCosinorFile.path,
        title: 'HR cosinor metrics',
        columns,
        reason:
          'Daily mesor and amplitude from hr_cosinor.csv for the selected period, reflecting overall cardiovascular activation and circadian robustness.',
      });
    }
  }

  // Sleep duration chart (from sleep_efficiency.csv)
  let sleepMinutesValues: number[] = [];

  if (sleepEfficiencyRows && sleepEfficiencyRows.length > 0 && sleepEfficiencyFile) {
    const sleepSeries: { x: number; y: number }[] = [];

    sleepEfficiencyRows.forEach((row) => {
      const day = parseDateFromDay(row.date || row.Date);
      const minutes = Number(row.sleep_min ?? row.Sleep_min);
      if (!day || Number.isNaN(minutes)) return;
      sleepSeries.push({ x: day.getTime(), y: minutes });
      sleepMinutesValues.push(minutes);
    });

    if (sleepSeries.length > 0) {
      sleepSeries.sort((a, b) => a.x - b.x);
      charts.push({
        filePath: sleepEfficiencyFile.path,
        title: 'Sleep duration',
        columns: [
          {
            label: 'Sleep minutes',
            data: sleepSeries,
            color: paletteForLabel('sleep_min'),
          },
        ],
        reason:
          'Nightly sleep duration from sleep_efficiency.csv within the selected timeframe, used to interpret restorative rest in the “I am rested” item.',
      });
    }
  }

  // --- if no questionnaire responses yet, just return requirements + charts ---

  if (!responses) {
    return {
      availability,
      comparisons: null,
      charts,
      alertnessImages: mfiAlertnessImages,
      contextImages: mfiContextImages,
    };
  }

  // --- build comparison items ---

  const comparisons: ComparisonItem[] = [];
  const expectedDays = expectedDaysInRange(range);

  const medianSteps =
  dailyStepTotals.length > 0 ? median(dailyStepTotals) : null;

  const stepsIqr = iqr(dailyStepTotals);
  const stepsCoverage = expectedDays > 0 ? dailyStepTotals.length / expectedDays : null;
  const stepsQualitySuffix =
    ` (coverage ${formatPct(stepsCoverage)}, IQR ${formatIqr(stepsIqr, 'steps')})`;

  const stepsFlag = qualityFlagFromCoverage(stepsCoverage, DAILY_COVERAGE_THRESHOLD);
  const stepsWarning =
    stepsFlag === 'low'
      ? ` Data quality: LOW QUALITY (steps present for only ${formatPct(stepsCoverage)} of expected days).`
      : '';

  
  const medianMesor =
  mesorValues.length > 0 ? median(mesorValues) : null;

  const medianAmplitude =
    amplitudeValues.length > 0 ? median(amplitudeValues) : null;

  const mesorIqr = iqr(mesorValues);
  const ampIqr = iqr(amplitudeValues);

  const mesorCoverage = expectedDays > 0 ? mesorValues.length / expectedDays : null;
  const ampCoverage = expectedDays > 0 ? amplitudeValues.length / expectedDays : null;

  const mesorFlag = qualityFlagFromCoverage(mesorCoverage, DAILY_COVERAGE_THRESHOLD);
  const ampFlag = qualityFlagFromCoverage(ampCoverage, DAILY_COVERAGE_THRESHOLD);

  const cosinorWarning =
    mesorFlag === 'low' || ampFlag === 'low'
      ? ` Data quality: LOW QUALITY (coverage mesor ${formatPct(mesorCoverage)}, amp ${formatPct(ampCoverage)}).`
      : '';

  const cosinorQualitySuffix =
    ` (coverage mesor ${formatPct(mesorCoverage)}, amp ${formatPct(ampCoverage)};` +
    ` IQR mesor ${formatIqr(mesorIqr, 'bpm')}, amp ${formatIqr(ampIqr, 'bpm')})`;
  
  const medianSleepMinutes =
    sleepMinutesValues.length > 0 ? median(sleepMinutesValues) : null;
  const medianSleepHours =
    medianSleepMinutes !== null ? medianSleepMinutes / 60 : null;

  const sleepIqrMin = iqr(sleepMinutesValues);
  const sleepCoverage = expectedDays > 0 ? sleepMinutesValues.length / expectedDays : null;

  const sleepQualitySuffix =
    ` (coverage ${formatPct(sleepCoverage)}, IQR ${formatIqr(sleepIqrMin, 'min')})`;


  const sleepFlag = qualityFlagFromCoverage(sleepCoverage, DAILY_COVERAGE_THRESHOLD);
  const sleepWarning =
    sleepFlag === 'low'
      ? ` Data quality: LOW QUALITY (sleep duration available for only ${formatPct(sleepCoverage)} of expected days).`
      : '';
      
  const formatSteps = (value: number | null) =>
    value === null ? '—' : `${Math.round(value).toLocaleString()} steps/day`;

  const formatBpm = (value: number | null) =>
    value === null ? '—' : `${value.toFixed(1)} bpm`;

  const formatHours = (value: number | null) =>
    value === null ? '—' : `${value.toFixed(2)} h`;

  // Item 3: feel_active
  if (responses['feel_active'] !== undefined) {
    comparisons.push({
      field: 'I feel very active (Item 3)',
      response: responses['feel_active'],
      computed: `${formatSteps(medianSteps)}${stepsQualitySuffix}`,
      difference: undefined,
      status: medianSteps === null ? 'unavailable' : 'info',
      reason:
          'This item is compared to the median number of daily steps in the selected timeframe, derived from steps.csv. It links the subjective feeling of being active with objectively measured locomotor activity and the daily step-count trajectory. A more detailed graph can be seen below.' + stepsWarning,
    });
  }

  // Item 7: do_alot
  if (responses['do_alot'] !== undefined) {
    comparisons.push({
      field: 'I think I do a lot in a day (Item 7)',
      response: responses['do_alot'],
      computed:
        medianMesor === null
          ? `—${cosinorQualitySuffix}`
          : `Heart-Rate Mesor: ${formatBpm(medianMesor)}${cosinorQualitySuffix}`,
      difference: undefined,
      status: medianMesor === null ? 'unavailable' : 'info',
      reason:
          'This item is mapped to the median diurnal heart-rate mesor from hr_cosinor.csv. A higher mesor reflects a generally elevated heart rate across the day, which can indicate increased physiological activation or “doing a lot”, but it may also be influenced by stress or other factors.' + cosinorWarning,
    });
  }

  // Item 13: rested
  if (responses['rested'] !== undefined) {
    const ampQualitySuffix =
      ` (coverage ${formatPct(ampCoverage)}, IQR ${formatIqr(ampIqr, 'bpm')})`;

    const ampWarning =
      ampFlag === 'low'
        ? ` Data quality: LOW QUALITY (HR amplitude available for only ${formatPct(ampCoverage)} of expected days).`
        : '';
    const combined =
      medianAmplitude !== null || medianSleepHours !== null
        ? `HR amplitude: ${formatBpm(medianAmplitude)}${ampQualitySuffix}; Sleep duration: ${formatHours(medianSleepHours)}${sleepQualitySuffix}`
        : `—${sleepQualitySuffix}`;

    comparisons.push({
      field: 'I am rested (Item 13)',
      response: responses['rested'],
      computed: combined,
      difference: undefined,
      status:
        medianAmplitude === null && medianSleepHours === null
          ? 'unavailable'
          : 'info',
      reason:
          'This item combines the median heart-rate amplitude from hr_cosinor.csv (circadian robustness and nighttime recovery) with median sleep duration from sleep_efficiency.csv. Longer sleep and larger amplitude (greater nighttime recovery relative to daytime activation) jointly suggest more restorative rest.' + sleepWarning,
    });
  }

  // Items 14 (tire_easily) and 1 (feel_fit) vs FIPS TMP fatigue projections
  const fipsComputed =
    alignedFipsImages.length > 0
      ? 'TMP fatigue projection chart below'
      : '—';

  if (responses['tire_easily'] !== undefined) {
    comparisons.push({
      field: 'I tire easily (Item 14)',
      response: responses['tire_easily'],
      computed: fipsComputed,
      difference: undefined,
      status: alignedFipsImages.length > 0 ? 'info' : 'unavailable',
      reason:
        'This item is compared qualitatively with the FIPS TMP-based fatigue projection graph. The idea is to relate self-reported tiredness to a physiological fatigue index derived from heart-rate and activity patterns.',
    });
  }

  if (responses['feel_fit'] !== undefined) {
    comparisons.push({
      field: 'I feel fit (Item 1)',
      response: responses['feel_fit'],
      computed: fipsComputed,
      difference: undefined,
      status: alignedFipsImages.length > 0 ? 'info' : 'unavailable',
      reason:
        'This item is shown alongside the same FIPS TMP fatigue projection graph. Agreement or disagreement between the self-reported feeling of fitness and the physiological fatigue prediction can highlight possible under- or overestimation of fatigue.',
    });
  }

  return {
    availability,
    comparisons,
    charts,
    alertnessImages: mfiAlertnessImages,
    contextImages: mfiContextImages,
  };
}


function medianByDay(points: { x: number; y: number }[]) {
  const byDay = new Map<string, number[]>();
  for (const p of points) {
    const d = new Date(p.x);
    const key = dayKey(startOfDay(d));
    if (!byDay.has(key)) byDay.set(key, []);
    byDay.get(key)!.push(p.y);
  }
  const out: number[] = [];
  for (const vals of byDay.values()) {
    const m = median(vals.filter((v) => Number.isFinite(v)));
    if (m !== null) out.push(m);
  }
  return out;
}

function percentileRank(sorted: number[], value: number) {
  // returns [0..1]
  if (sorted.length === 0) return null;
  // first index with v >= value
  let lo = 0, hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (sorted[mid] >= value) hi = mid;
    else lo = mid + 1;
  }
  let idx = lo; // in [0..n]
  if (sorted.length === 1) return 1;

  // clamp to last valid index for percentile mapping
  if (idx >= sorted.length) idx = sorted.length - 1;

  const pct = idx / (sorted.length - 1);
  return pct;
}

function circularMeanMinutes(values: number[]) {
  if (values.length === 0) return null;
  const angles = values.map((m) => (m / (24 * 60)) * 2 * Math.PI);
  const sinMean = angles.reduce((a, t) => a + Math.sin(t), 0) / angles.length;
  const cosMean = angles.reduce((a, t) => a + Math.cos(t), 0) / angles.length;
  const ang = Math.atan2(sinMean, cosMean);
  const wrapped = (ang + 2 * Math.PI) % (2 * Math.PI);
  return (wrapped / (2 * Math.PI)) * 24 * 60;
}

function circularDistanceMinutes(a: number, b: number) {
  // minimal signed distance on circle, return absolute distance in minutes
  const period = 24 * 60;
  let d = ((a - b) % period + period) % period;
  if (d > period / 2) d = period - d;
  return Math.abs(d);
}

function toHours(valuesMinutes: number[]) {
  return valuesMinutes
    .map((m) => (Number.isFinite(m) ? m / 60 : NaN))
    .filter((h) => Number.isFinite(h));
}

function clamp(value: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, value));
}

function normalize0to100_fromRange(value: number | null, minV: number, maxV: number) {
  if (value === null) return null;
  if (maxV <= minV) return null;
  return clamp(((value - minV) / (maxV - minV)) * 100, 0, 100);
}

function computePss10Total(raw: Record<string, string>) {
  const keys = [
    'upset',
    'no_controll',
    'stressed',
    'confident',   // reverse
    'fortunate',   // reverse
    'cant_cope',
    'controll',    // reverse
    'on_top',      // reverse
    'anger',
    'overwhelmed',
  ];

  const reverse = new Set(['confident', 'fortunate', 'controll', 'on_top']);

  let sum = 0;
  let n = 0;
  const issues: string[] = [];

  for (const k of keys) {
    const vRaw = (raw[k] ?? '').toString().trim();
    const v = Number(vRaw);

    if (Number.isNaN(v)) {
      issues.push(`Missing/non-numeric "${k}"`);
      continue;
    }
    if (v < 0 || v > 4) {
      issues.push(`Out of range "${k}"=${v} (expected 0–4)`);
      continue;
    }

    const scored = reverse.has(k) ? (4 - v) : v;
    sum += scored;
    n += 1;
  }

  if (n !== 10) {
    return { total: null as number | null, issues };
  }
  return { total: sum, issues };
}

function formatScoreBarLine(label: string, value0to100: number | null, suffix = '') {
  if (value0to100 === null) return `${label}: —`;
  return `${label}: ${value0to100.toFixed(0)}/100${suffix}`;
}

type WindowAgg = {
  sessionLabel: string;
  sessionDate: Date;
  windowStart: Date;
  windowEnd: Date;
  pssTotal: number | null;

  dailyHrvMedians: number[];     // ~30 values
  sleepHoursVals: number[];      // ~30 values
  irregularityDevs: number[];    // per-night deviation minutes

  hrvMedian: number | null;
  moodmlMedian: number | null;

  // Components for SRI (window-level aggregates)
  restHrMed: number | null;
  hrvMed: number | null;
  sleepHoursMed: number | null;
  irregularity: number | null; // circular std in minutes

  // Coverage
  hrvCoverage: number | null;       // observed minutes / expected minutes
  moodmlCoverage: number | null;    // observed days / expected days
  restHrCoverage: number | null;
  sleepCoverage: number | null;
  sleepDaysUsed: number;

  // Chart series
  hrvSeries: { x: number; y: number }[];
  moodmlSeries: { x: number; y: number }[];

  issues: string[];
};

function circularStdMinutes(valuesMinutes: number[]) {
  // values in [0,1440)
  if (valuesMinutes.length < 2) return null;
  const angles = valuesMinutes.map((m) => (m / (24 * 60)) * 2 * Math.PI);
  const sinMean = angles.reduce((a, t) => a + Math.sin(t), 0) / angles.length;
  const cosMean = angles.reduce((a, t) => a + Math.cos(t), 0) / angles.length;
  const R = Math.sqrt(sinMean * sinMean + cosMean * cosMean);
  if (R <= 0) return null;
  const stdRad = Math.sqrt(-2 * Math.log(R));
  const stdMinutes = (stdRad / (2 * Math.PI)) * 24 * 60;
  return stdMinutes;
}

async function evaluatePSS10(
  workspace: string,
  files: WorkspaceFile[],
  range: DateRange,
  responses: Pss10Responses | null,
): Promise<EvaluationResult> {
  const availability: Record<string, DataAvailability> = {
    hrv: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSS10.hrv) },
    moodml: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSS10.moodml) },
    restingHr: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSS10.restingHr) },
    sleepEfficiency: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSS10.sleepEfficiency) },
    mergedSleep: { status: 'missing', message: buildAvailabilityMessage('missing', REQUIRED_DATASETS_PSS10.mergedSleep) },
  };

  // --- locate files ---
  const hrvMinuteFile =
    findFile(files, (f) => f.path.toLowerCase().includes('hrv_minute.csv')) ??
    findFile(files, (f) => f.path.toLowerCase().endsWith('hrv.csv'));

  const moodmlFile = findFile(files, (f) => f.path.toLowerCase().includes('expected_outcome_de.csv'));

  const restingHrFile = findFile(files, (f) => f.path.toLowerCase().includes('resting_hr.csv'));
  const sleepEfficiencyFile = findFile(files, (f) => f.path.toLowerCase().includes('sleep_efficiency.csv'));
  const mergedSleepFile = findFile(files, (f) => f.path.toLowerCase().includes('sleep_episodes_merged.csv'));

  // --- parse once (full), filter per-session window later ---
  const [hrvRowsAll, moodmlRowsAll, restingHrRowsAll, sleepEffRowsAll, mergedSleepRowsAll] = await Promise.all([
    (async () => {
      if (!hrvMinuteFile) return null;
      try {
        const rows = await parseCsv(hrvMinuteFile.path, workspace);
        availability.hrv = { status: 'present', path: hrvMinuteFile.path, message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSS10.hrv) };
        return rows;
      } catch {
        availability.hrv = { status: 'error', path: hrvMinuteFile.path, message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSS10.hrv) };
        return null;
      }
    })(),
    (async () => {
      if (!moodmlFile) return null;
      try {
        const rows = await parseCsv(moodmlFile.path, workspace);
        availability.moodml = { status: 'present', path: moodmlFile.path, message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSS10.moodml) };
        return rows;
      } catch {
        availability.moodml = { status: 'error', path: moodmlFile.path, message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSS10.moodml) };
        return null;
      }
    })(),
    (async () => {
      if (!restingHrFile) return null;
      try {
        const rows = await parseCsv(restingHrFile.path, workspace);
        availability.restingHr = { status: 'present', path: restingHrFile.path, message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSS10.restingHr) };
        return rows;
      } catch {
        availability.restingHr = { status: 'error', path: restingHrFile.path, message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSS10.restingHr) };
        return null;
      }
    })(),
    (async () => {
      if (!sleepEfficiencyFile) return null;
      try {
        const rows = await parseCsv(sleepEfficiencyFile.path, workspace);
        availability.sleepEfficiency = { status: 'present', path: sleepEfficiencyFile.path, message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSS10.sleepEfficiency) };
        return rows;
      } catch {
        availability.sleepEfficiency = { status: 'error', path: sleepEfficiencyFile.path, message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSS10.sleepEfficiency) };
        return null;
      }
    })(),
    (async () => {
      if (!mergedSleepFile) return null;
      try {
        const rows = await parseCsv(mergedSleepFile.path, workspace);
        availability.mergedSleep = { status: 'present', path: mergedSleepFile.path, message: buildAvailabilityMessage('present', REQUIRED_DATASETS_PSS10.mergedSleep) };
        return rows;
      } catch {
        availability.mergedSleep = { status: 'error', path: mergedSleepFile.path, message: buildAvailabilityMessage('error', REQUIRED_DATASETS_PSS10.mergedSleep) };
        return null;
      }
    })(),
  ]);

  const charts: ChartBundle[] = [];

  // If no questionnaire uploaded yet, just show availability (like your other evaluators)
  if (!responses) {
    // If files exist but empty overlap for the *selected timeframe*, reflect that
    // (optional, but consistent with PSQI/MFI style)
    return { availability, comparisons: null, charts, alertnessImages: [], contextImages: [] };
  }

  // --- filter PSS sessions to datepicker range ---
  const sessionsInRange = responses.filter((s) => withinRangeStart(startOfDay(s.scheduled), range));
  if (sessionsInRange.length === 0) {
    return {
      availability,
      comparisons: [
        {
          field: 'PSS-10',
          response: '—',
          computed: 'No questionnaire sessions fall inside the selected timeframe.',
          difference: undefined,
          status: 'unavailable',
          reason: `Adjust the date range so it includes your PSS-10 Session Scheduled Time values.`,
        },
      ],
      charts,
      alertnessImages: [],
      contextImages: [],
    };
  }

  // --- build per-session window aggregates (30-day lookback) ---
  const windowAggs: WindowAgg[] = [];

  for (const session of sessionsInRange) {
    const windowEnd = endOfDay(session.scheduled);
    const windowStart = startOfDay(addDays(session.scheduled, -29)); // inclusive 30 days

    const issues: string[] = [];
    const { total: pssTotal, issues: pssIssues } = computePss10Total(session.raw);
    issues.push(...pssIssues);

    // HRV minute series in window
    const hrvSeries =
      (hrvRowsAll ?? [])
        .map((row) => {
          const dt = parseDateTime(row.minute ?? row.datetime ?? row.DateTime ?? row.timestamp);
          const v = Number(row.rmssd_ms ?? row.rmssd ?? row.RMSSD ?? row.value);
          if (!dt || Number.isNaN(v)) return null;
          if (dt < windowStart || dt > windowEnd) return null;
          return { x: dt.getTime(), y: v };
        })
        .filter((p): p is { x: number; y: number } => p !== null)
        .sort((a, b) => a.x - b.x);

    const hrvMedian = median(hrvSeries.map((p) => p.y));

    const dailyHrvMedians = medianByDay(hrvSeries).sort((a, b) => a - b);

    

    const expectedMinutes = expectedMinutesInRange({ start: windowStart, end: windowEnd, labelStart: '', labelEnd: '' });
    const hrvCoverage = expectedMinutes > 0 ? hrvSeries.length / expectedMinutes : null;

    // MoodML prob_de series in window (daily)
    const moodmlSeries =
      (moodmlRowsAll ?? [])
        .map((row) => {
          const d = parseDateFromDay(row.date ?? row.Date);
          const v = Number(row.prob_de ?? row.Prob_de ?? row.prob_DE);
          if (!d || Number.isNaN(v)) return null;
          if (d < windowStart || d > windowEnd) return null;
          return { x: d.getTime(), y: v };
        })
        .filter((p): p is { x: number; y: number } => p !== null)
        .sort((a, b) => a.x - b.x);

    const moodmlMedian = median(moodmlSeries.map((p) => p.y));
    const expectedDays = expectedDaysInRange({ start: windowStart, end: windowEnd, labelStart: '', labelEnd: '' });
    const moodmlCoverage = expectedDays > 0 ? moodmlSeries.length / expectedDays : null;

    // Resting HR (daily) in window
    const restHrVals =
      (restingHrRowsAll ?? [])
        .map((row) => {
          const d = parseDateFromDay(row.date ?? row.Date);
          const v = Number(row.resting_bpm ?? row.Resting_bpm ?? row.bpm);
          if (!d || Number.isNaN(v)) return null;
          if (d < windowStart || d > windowEnd) return null;
          return v;
        })
        .filter((v): v is number => v !== null);

    const restHrMed = median(restHrVals);
    const restHrCoverage = expectedDays > 0 ? restHrVals.length / expectedDays : null;

    // Sleep duration (daily sleep_min) in window
    const sleepMinVals =
      (sleepEffRowsAll ?? [])
        .map((row) => {
          const d = parseDateFromDay(row.date ?? row.Date);
          const v = Number(row.sleep_min ?? row.Sleep_min);
          if (!d || Number.isNaN(v)) return null;
          if (d < windowStart || d > windowEnd) return null;
          return v;
        })
        .filter((v): v is number => v !== null);

    const sleepHoursMed = (() => {
      const medMin = median(sleepMinVals);
      return medMin === null ? null : medMin / 60;
    })();
    const sleepCoverage = expectedDays > 0 ? sleepMinVals.length / expectedDays : null;


    const sleepHoursVals = toHours(sleepMinVals); // distribution across the 30 days

    // Sleep timing irregularity: circular std of onset+wake (minutes since midnight)
    // We use merged sleep episodes; derive onset/wake minutes per day and compute circular std.
    const onsetMinutes: number[] = [];
    const wakeMinutes: number[] = [];
    const midpointMinutes: number[] = [];

    if (mergedSleepRowsAll) {
      for (const row of mergedSleepRowsAll) {
        const start = parseDate(row.start ?? row.Start ?? row.begin);
        const end = parseDate(row.end ?? row.End ?? row.stop);
        if (!start || !end) continue;
        if (start < windowStart || start > windowEnd) continue;

        const on = minutesSinceMidnight(start);
        const off = minutesSinceMidnight(end);

        onsetMinutes.push(((on % (24 * 60)) + (24 * 60)) % (24 * 60));
        wakeMinutes.push(((off % (24 * 60)) + (24 * 60)) % (24 * 60));

        const mid = new Date((start.getTime() + end.getTime()) / 2);
        const midm = minutesSinceMidnight(mid);
        midpointMinutes.push(((midm % (24 * 60)) + (24 * 60)) % (24 * 60));
      }
    }



    const midMean = circularMeanMinutes(midpointMinutes);
    const midpointDevs =
      midMean === null
        ? []
        : midpointMinutes.map((m) => circularDistanceMinutes(m, midMean));

    const onsetStd = circularStdMinutes(onsetMinutes);
    const wakeStd = circularStdMinutes(wakeMinutes);
    const midStd = circularStdMinutes(midpointMinutes);

    // prefer onset+wake if both exist; else fallback to midpoint
    const irregularity = midStd !== null ? midStd : null;

    windowAggs.push({
      sessionLabel: session.scheduledLabel,
      sessionDate: session.scheduled,
      windowStart,
      windowEnd,
      pssTotal,
      hrvMedian,
      moodmlMedian,
      restHrMed,
      hrvMed: hrvMedian,
      sleepHoursMed,
      irregularity,
      hrvCoverage,
      moodmlCoverage,
      restHrCoverage,
      sleepCoverage,
      sleepDaysUsed: sleepMinVals.length,
      hrvSeries,
      moodmlSeries,
      issues,
      dailyHrvMedians,
      sleepHoursVals,
      irregularityDevs: midpointDevs,
    });

    // --- charts per session ---
    if (hrvSeries.length > 0 && hrvMinuteFile) {
      charts.push({
        filePath: hrvMinuteFile.path,
        title: `HRV (RMSSD) trajectory · PSS-10 session ${dayKey(session.scheduled)} (30d window)`,
        columns: [
          { label: 'RMSSD (ms)', data: hrvSeries, color: paletteForLabel('rmssd_ms') },
        ],
        reason: `Minute-level HRV from ${dayKey(windowStart)} to ${dayKey(windowEnd)}. Median is used for comparison to PSS-10.`,
      });
    }

    if (moodmlSeries.length > 0 && moodmlFile) {
      charts.push({
        filePath: moodmlFile.path,
        title: `MoodML depression risk (prob_de) · PSS-10 session ${dayKey(session.scheduled)} (30d window)`,
        columns: [
          { label: 'prob_de', data: moodmlSeries, color: paletteForLabel('prob_de') },
        ],
        reason: `Daily MoodML depression risk (prob_de) in the same 30-day window. Median prob_de is used for the comparison.`,
      });
    }
  }
  

  // Directionality per your definition:
  // + resting HR, - HRV, - sleep duration, + irregularity

  // --- build comparisons list ---
  const comparisons: ComparisonItem[] = [];

  for (const w of windowAggs) {

    // --- SRI (0–100) computed within the 30-day window via range-normalization ---
    const sri0to100 = (() => {
      // Rest HR: higher -> worse
      const restVals = (restingHrRowsAll ?? [])
        .map((row) => {
          const d = parseDateFromDay(row.date ?? row.Date);
          const v = Number(row.resting_bpm ?? row.Resting_bpm ?? row.bpm);
          if (!d || Number.isNaN(v)) return null;
          if (d < w.windowStart || d > w.windowEnd) return null;
          return v;
        })
        .filter((v): v is number => v !== null);

      const restMin = restVals.length ? Math.min(...restVals) : null;
      const restMax = restVals.length ? Math.max(...restVals) : null;
      const rest0to100 = (restMin === null || restMax === null) ? null : normalize0to100_fromRange(w.restHrMed, restMin, restMax);

      // HRV: lower -> worse (normalize then invert)
      const hrvRef = (w.dailyHrvMedians ?? []).filter(Number.isFinite);
      const hrvMin = hrvRef.length ? Math.min(...hrvRef) : null;
      const hrvMax = hrvRef.length ? Math.max(...hrvRef) : null;
      const hrv0to100_raw = (hrvMin === null || hrvMax === null) ? null : normalize0to100_fromRange(w.hrvMedian, hrvMin, hrvMax);
      const hrv0to100 = hrv0to100_raw === null ? null : 100 - hrv0to100_raw;

      // Sleep duration: lower -> worse (normalize then invert)
      const sleepRef = (w.sleepHoursVals ?? []).filter(Number.isFinite);
      const sleepMin = sleepRef.length ? Math.min(...sleepRef) : null;
      const sleepMax = sleepRef.length ? Math.max(...sleepRef) : null;
      const sleep0to100_raw = (sleepMin === null || sleepMax === null) ? null : normalize0to100_fromRange(w.sleepHoursMed, sleepMin, sleepMax);
      const sleep0to100 = sleep0to100_raw === null ? null : 100 - sleep0to100_raw;

      // Irregularity: higher -> worse
      const irrRef = windowAggs.map(x => x.irregularity).filter((v): v is number => v !== null);
      const irrMin = irrRef.length ? Math.min(...irrRef) : null;
      const irrMax = irrRef.length ? Math.max(...irrRef) : null;
      const irr0to100 =
        (irrMin === null || irrMax === null) ? null : normalize0to100_fromRange(w.irregularity, irrMin, irrMax);

      const parts = [rest0to100, hrv0to100, sleep0to100, irr0to100].filter((v): v is number => v !== null);
      if (parts.length === 0) return null;
      return parts.reduce((a, b) => a + b, 0) / parts.length;
    })();

    // normalized “comparable” view (0–100)
    const pssNorm = w.pssTotal === null ? null : (w.pssTotal / 40) * 100;
    const depNorm = w.moodmlMedian === null ? null : w.moodmlMedian * 100;

    const hrvStress = (() => {
      if (w.hrvMedian === null) return null;

      let ref = (w.dailyHrvMedians ?? []).filter(Number.isFinite);

      // 🔑 Fallback: if only one day available, use minute-level HRV distribution
      if (ref.length < 2) {
        ref = w.hrvSeries.map(p => p.y).filter(Number.isFinite);
      }

      if (ref.length < 2) return null;

      ref.sort((a, b) => a - b);
      const pct = percentileRank(ref, w.hrvMedian);
      if (pct === null) return null;
      return (1 - pct) * 100;
    })();


    const coverageNote =
      `Coverage: HRV ${formatPct(w.hrvCoverage)}, MoodML ${formatPct(w.moodmlCoverage)}, RestHR ${formatPct(w.restHrCoverage)}, Sleep ${formatPct(w.sleepCoverage)}.`;

    const issuesNote = w.issues.length > 0 ? ` Data issues: ${w.issues.join('; ')}.` : '';

    const computedLines = [
      `Window: ${dayKey(w.windowStart)} → ${dayKey(w.windowEnd)} (30 days)`,
      formatScoreBarLine('PSS-10 (stress)', pssNorm, w.pssTotal === null ? '' : ` (raw ${w.pssTotal}/40)`),
      formatScoreBarLine('HRV-derived stress', hrvStress, w.hrvMedian === null ? '' : ` (median ${w.hrvMedian.toFixed(1)} ms)`),
      formatScoreBarLine('MoodML depression risk', depNorm, w.moodmlMedian === null ? '' : ` (median ${w.moodmlMedian.toFixed(3)})`),
      formatScoreBarLine('SRI (stress index)', sri0to100, sri0to100 === null ? '' : ' (0–100)'),
    ].join(' · ');

    comparisons.push({
      field: `PSS-10 total · Session ${dayKey(w.sessionDate)}`,
      response: w.pssTotal === null ? '—' : `${w.pssTotal} / 40`,
      computed: computedLines,
      difference: undefined,
      status:
        w.pssTotal === null ? 'unavailable' :
        (w.hrvMedian === null && w.moodmlMedian === null && sri0to100 === null) ? 'unavailable' : 'info',
      reason:
        `PSS-10 total is computed by reverse scoring items 4, 5, 7, and 8, then summing all 10 items. ` +
        `Comparisons use the median HRV (rmssd_ms) and median MoodML depression risk (prob_de) within the same 30-day lookback window ending on the session date. ` +
        `SRI is computed from resting HR (+), HRV (−), sleep duration (−), and sleep timing irregularity (+) by mapping each component to 0–100 via min–max normalization (within the session window for RestHR/HRV/Sleep, and across sessions for irregularity), then averaging available components. ` +
        `${coverageNote}${issuesNote}`,
    });
  }

  // Refine availability statuses to reflect overlap with at least one session window
  // (optional but helpful)
  const anyHrv = windowAggs.some((w) => w.hrvSeries.length > 0);
  if (availability.hrv.status === 'present' && !anyHrv) {
    availability.hrv = { ...availability.hrv, status: 'empty', message: buildAvailabilityMessage('empty', REQUIRED_DATASETS_PSS10.hrv) };
  }
  const anyMood = windowAggs.some((w) => w.moodmlSeries.length > 0);
  if (availability.moodml.status === 'present' && !anyMood) {
    availability.moodml = { ...availability.moodml, status: 'empty', message: buildAvailabilityMessage('empty', REQUIRED_DATASETS_PSS10.moodml) };
  }

  return {
    availability,
    comparisons,
    charts,
    alertnessImages: [],
    contextImages: [],
  };
}



function QuestionnaireCard({ comparison }: { comparison: ComparisonItem }) {
  return (
    <div className={`questionnaire-row questionnaire-${comparison.status}`}>
      <div className="questionnaire-field">
        <span className="questionnaire-label">{comparison.field}</span>
        <span className="questionnaire-response">{comparison.response ?? '—'}</span>
      </div>
      <div className="questionnaire-computed">
        <span className="questionnaire-label">Computed</span>
        <span>{comparison.computed ?? '—'}</span>
      </div>
      <div className="questionnaire-difference">
        <span className="questionnaire-label">Difference</span>
        <span>{comparison.difference ?? '—'}</span>
      </div>
      <p className="questionnaire-reason">{comparison.reason}</p>
    </div>
  );
}

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  scales: {
    x: {
      type: 'time' as const,
      time: { unit: 'day' as const },
      ticks: { color: '#cbd5e1' },
      grid: { color: 'rgba(148, 163, 184, 0.2)' },
    },
    y: {
      ticks: { color: '#cbd5e1' },
      grid: { color: 'rgba(148, 163, 184, 0.2)' },
    },
  },
  plugins: {
    legend: {
      labels: { color: '#e2e8f0' },
    },
    tooltip: {
      callbacks: {
        label(context: any) {
          const label = context.dataset.label || '';
          const value = context.parsed.y;
          return `${label}: ${value}`;
        },
      },
    },
  },
};



export function QuestionnairesSection({ workspace, files, onFilesUpdated }: QuestionnairesSectionProps) {
  const [questionnaire, setQuestionnaire] = useState<QuestionnaireKind>('psqi');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');



  const [psqiResponses, setPsqiResponses] = useState<QuestionnaireResponses | null>(null);
  const [mfiResponses, setMfiResponses] = useState<QuestionnaireResponses | null>(null);

  const [pss10Responses, setPss10Responses] = useState<Pss10Responses | null>(null);
  const [pss10UploadedFileName, setPss10UploadedFileName] = useState<string | null>(null);


  const [psqiUploadedFileName, setPsqiUploadedFileName] = useState<string | null>(null);
  const [mfiUploadedFileName, setMfiUploadedFileName] = useState<string | null>(null);

  // Convenience getters for the currently selected questionnaire
  const currentResponses =
    questionnaire === 'psqi' ? psqiResponses :
    questionnaire === 'mfi' ? mfiResponses :
    pss10Responses;

  const currentUploadedFileName =
    questionnaire === 'psqi' ? psqiUploadedFileName :
    questionnaire === 'mfi' ? mfiUploadedFileName :
    pss10UploadedFileName;



  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [availability, setAvailability] = useState<Record<string, DataAvailability>>(
    buildInitialAvailability('psqi'),
  );
  const [comparisons, setComparisons] = useState<ComparisonItem[] | null>(null);
  const [charts, setCharts] = useState<ChartBundle[]>([]);
  const [alertnessImages, setAlertnessImages] = useState<WorkspaceFile[]>([]);
  const [contextImages, setContextImages] = useState<WorkspaceFile[]>([]);

  const range = useMemo(() => toDateRange(startDate, endDate), [startDate, endDate]);

  const refreshEvaluation = useCallback(async () => {
    if (!workspace || !range) {
      setAvailability(buildInitialAvailability(questionnaire));
      setComparisons(null);
      setCharts([]);
      setAlertnessImages([]);
      setContextImages([]);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const result =
        questionnaire === 'psqi'
          ? await evaluatePSQI(workspace, files, range, psqiResponses)
          : questionnaire === 'mfi'
          ? await evaluateMFI(workspace, files, range, mfiResponses)
          : await evaluatePSS10(workspace, files, range, pss10Responses);

      setAvailability(result.availability);
      setComparisons(result.comparisons);
      setCharts(result.charts);
      setAlertnessImages(result.alertnessImages);
      setContextImages(result.contextImages);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to evaluate questionnaire');
    } finally {
      setLoading(false);
    }
  }, [workspace, range, files, currentResponses, questionnaire]);

  useEffect(() => {
    void refreshEvaluation();
  }, [refreshEvaluation]);

  const handleUpload = async (file: File) => {
    if (!workspace) { setError('Workspace is not ready yet.'); return; }
    if (!range) { setError('Please select a timeframe before uploading a questionnaire.'); return; }

    setLoading(true);
    setError(null);

    try {
      const text = await file.text();
      const parsed = Papa.parse<Record<string, string>>(text, { header: true, skipEmptyLines: true });
      if (parsed.errors.length > 0) throw new Error(parsed.errors[0].message);
      if (parsed.data.length === 0) throw new Error('The uploaded CSV does not contain any rows.');

      // ✅ normalize key helper
      const normalizeRow = (row: Record<string, string>) => {
        const out: Record<string, string> = {};
        for (const [k, v] of Object.entries(row)) {
          if (!k) continue;
          out[k.trim().toLowerCase()] = (v ?? '').toString();
        }
        return out;
      };

      if (questionnaire === 'pss10') {
        const instances: Pss10Instance[] = [];

        for (const row of parsed.data) {
          const r = normalizeRow(row);

          const participantId =
            (r['participant id'] ?? r['participant_id'] ?? r['user'] ?? '').trim();

          const scheduledRaw =
            (r['session scheduled time'] ?? r['scheduled time'] ?? r['date'] ?? '').trim();

          const scheduled =
            parseDateFromDay(scheduledRaw) ??
            parseDateTime(scheduledRaw) ??
            null;

          if (!scheduled) continue; // skip unusable rows

          instances.push({
            participantId: participantId || 'UNKNOWN',
            scheduled,
            scheduledLabel: scheduledRaw || dayKey(scheduled),
            raw: r,
          });
        }

        if (instances.length === 0) {
          throw new Error(
            'No valid rows found. Expected at least "Session Scheduled Time" and the 10 item columns.',
          );
        }

        // sort by date
        instances.sort((a, b) => a.scheduled.getTime() - b.scheduled.getTime());

        setPss10Responses(instances);
        setPss10UploadedFileName(file.name);
      } else {
        // PSQI/MFI: keep your existing "single-row" behavior
        const firstRow = parsed.data[0];
        const normalized = normalizeRow(firstRow);
        if (questionnaire === 'psqi') {
          setPsqiResponses(normalized);
          setPsqiUploadedFileName(file.name);
        } else {
          setMfiResponses(normalized);
          setMfiUploadedFileName(file.name);
        }
      }

      const targetPath = `questionnaires/${questionnaire}/${Date.now()}-${file.name}`;
      await uploadFile(workspace, file, targetPath);
      if (onFilesUpdated) await onFilesUpdated();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to process questionnaire');
    } finally {
      setLoading(false);
    }
  };

  const questionnaireStatus = useMemo(() => {
    const name = QUESTIONNAIRE_SHORT_LABEL[questionnaire];
    if (!range) return `Select a timeframe to begin the ${name} review.`;
    if (!currentResponses) return `Upload ${name} responses to compare against biometric data.`;
    if (loading) return `Evaluating ${name} responses…`;
    return `${name} responses evaluated against biometric and derived metrics.`;
  }, [range, currentResponses, loading, questionnaire]);

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>5 · Questionnaires</h3>
        <p className="small">
          Upload {QUESTIONNAIRE_SHORT_LABEL[questionnaire]} results, validate responses
          against biometric records, and review supporting context.
        </p>
      </div>

      <div className="questionnaire-controls">
        <div className="field">
          <label htmlFor="questionnaire-type">Questionnaire</label>
          <select
            id="questionnaire-type"
            value={questionnaire}
            onChange={(event) => {
              const next = event.target.value as QuestionnaireKind;
              setQuestionnaire(next);
              setAvailability(buildInitialAvailability(next));
              setComparisons(null);
              setCharts([]);
              setAlertnessImages([]);
              setContextImages([]);
            }}
          >
            <option value="psqi">PSQI (Pittsburgh Sleep Quality Index)</option>
            <option value="mfi">MFI-20 (Multidimensional Fatigue Inventory)</option>
            <option value="pss10">PSS-10 (Perceived Stress Scale)</option>
          </select>
        </div>

        <div className="field">
          <label htmlFor="psqi-start">Timeframe start</label>
          <input
            id="psqi-start"
            type="date"
            value={startDate}
            onChange={(event) => setStartDate(event.target.value)}
          />
        </div>
        <div className="field">
          <label htmlFor="psqi-end">Timeframe end</label>
          <input id="psqi-end" type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} />
        </div>
        <div className="field">
          <label htmlFor="psqi-upload">
            {QUESTIONNAIRE_RESPONSE_LABEL[questionnaire]}
          </label>
          <input
            id="psqi-upload"
            type="file"
            accept=".csv"
            onChange={(event) => {
              const file = event.target.files?.[0];
              if (file) {
                void handleUpload(file);
              }
            }}
          />
          {currentUploadedFileName && (
            <span className="small">Last uploaded: {currentUploadedFileName}</span>
          )}
        </div>
      </div>

      <p className="status-text">{questionnaireStatus}</p>
      {error && <p className="status error">{error}</p>}

      {range && (
        <div className="availability-grid">
          {Object.entries(availability).map(([key, info]) => (
            <div key={key} className={`availability-card availability-${info.status}`}>
              <h4>{DATASET_LABELS[questionnaire][key] ?? key}</h4>
              <p>{info.message}</p>
              {info.path && <p className="small">Source: {info.path}</p>}
            </div>
          ))}
        </div>
      )}

      {comparisons && (
        <div className="questionnaire-results">
          <h4>
            {questionnaire === 'psqi' ? 'PSQI cross-check' : 'MFI-20 cross-check'}
          </h4>

          {questionnaire === 'mfi' && (
            <div className="mfi-scale">
              <div className="mfi-scale-labels">
                <span>Yes, that is true (1)</span>
                <span>No, that is not true (5)</span>
              </div>
              <div className="mfi-scale-bar" />
            </div>
          )}

          <div className="questionnaire-grid">
            {comparisons.map((comparison) => (
              <QuestionnaireCard key={comparison.field} comparison={comparison} />
            ))}
          </div>
        </div>
      )}

      {charts.length > 0 && (
        <div className="questionnaire-charts">
          <h4>Contextual metrics</h4>
          <div className="chart-grid">
            {charts.map((chart) => (
              <div key={chart.filePath} className="chart-card">
                <div className="chart-header">
                  <h5>{chart.title}</h5>
                  <p className="small">{chart.reason}</p>
                </div>
                <div className="chart-canvas">
                  <Line
                    options={chartOptions}
                    data={{
                      datasets: chart.columns.map((column) => ({
                        label: column.label,
                        data: column.data,
                        fill: false,
                        borderColor: column.color,
                        backgroundColor: column.color,
                        tension: 0.25,
                        pointRadius: 2,
                      })),
                    }}
                  />
                </div>
                <p className="small">Source: {chart.filePath}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {(alertnessImages.length > 0 || contextImages.length > 0) && (
        <div className="questionnaire-images">
          <h4>FIPS visualizations</h4>
          <div className="image-grid">
            {alertnessImages.map((image) => (
              <figure key={image.path}>
                <img src={resolveDownloadUrl(image.download_url)} alt={image.path} />
                <figcaption>TMP alertness chart · {image.path}</figcaption>
              </figure>
            ))}
            {contextImages.map((image) => (
              <figure key={image.path}>
                <img src={resolveDownloadUrl(image.download_url)} alt={image.path} />
                <figcaption>{image.path}</figcaption>
              </figure>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default QuestionnairesSection;
