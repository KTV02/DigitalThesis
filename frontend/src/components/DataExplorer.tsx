import { useEffect, useMemo, useState } from 'react';
import Papa from 'papaparse';
import { Line } from 'react-chartjs-2';
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
import 'chartjs-adapter-date-fns';
import { WorkspaceFile, downloadFile } from '../api';
import { format } from 'date-fns';

ChartJS.register(LineElement, PointElement, TimeScale, LinearScale, Tooltip, Legend, Filler);

export interface DataExplorerProps {
  workspace: string;
  files: WorkspaceFile[];
}

interface ColumnSeries {
  column: string;
  data: { x: number; y: number }[];
  color: string;
}

interface FileSeries {
  filePath: string;
  columns: ColumnSeries[];
  start: number;
  end: number;
}

function chunkColumns(columns: ColumnSeries[], size: number): ColumnSeries[][] {
  const chunks: ColumnSeries[][] = [];
  for (let i = 0; i < columns.length; i += size) {
    chunks.push(columns.slice(i, i + size));
  }
  return chunks;
}

const COLORS = ['#60a5fa', '#34d399', '#f472b6', '#fbbf24', '#38bdf8', '#c084fc', '#f97316'];
function paletteForLabel(label: string) {
  let hash = 0;
  for (const char of label) {
    hash = (hash * 31 + char.charCodeAt(0)) % COLORS.length;
  }
  return COLORS[Math.abs(hash) % COLORS.length];
}

function normalizeTimestamp(value: string | undefined): number | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const isoCandidate = trimmed.replace(' ', 'T');
  const parsed = Date.parse(isoCandidate);
  if (!Number.isNaN(parsed)) {
    return parsed;
  }
  const numeric = Number(trimmed);
  if (Number.isNaN(numeric)) {
    return null;
  }
  if (numeric > 1e12) {
    return numeric;
  }
  if (numeric > 1e9) {
    return numeric * 1000;
  }
  return null;
}

function detectTimeIndex(headers: string[], body: string[][]): number {
  const prioritized = headers
    .map((header, idx) => ({ header, idx }))
    .sort((a, b) => {
      const regex = /minute|datetime|timestamp|date|time|start|end/i;
      const aPriority = regex.test(a.header) ? 0 : 1;
      const bPriority = regex.test(b.header) ? 0 : 1;
      if (aPriority === bPriority) {
        return a.idx - b.idx;
      }
      return aPriority - bPriority;
    });

  for (const candidate of prioritized) {
    let valid = 0;
    let observed = 0;
    for (const row of body) {
      const cell = row[candidate.idx];
      if (!cell || !cell.trim()) continue;
      observed += 1;
      if (normalizeTimestamp(cell) !== null) {
        valid += 1;
      }
    }
    if (observed === 0) continue;
    const threshold = observed < 3 ? observed : Math.max(3, Math.ceil(observed * 0.5));
    if (valid >= threshold) {
      return candidate.idx;
    }
  }

  return -1;
}

function inferSeries(parsed: Papa.ParseResult<string[]>, filePath: string): FileSeries | null {
  const rows = parsed.data;
  if (rows.length < 2) return null;

  const headers = rows[0];
  const body = rows.slice(1).filter((row) => row.some((value) => value && value.trim().length > 0));
  if (body.length === 0) return null;

  const timeIndex = detectTimeIndex(headers, body);
  if (timeIndex === -1) {
    return null;
  }

  const numericColumns = headers
    .map((header, idx) => ({ header, idx }))
    .filter(({ idx }) => idx !== timeIndex)
    .filter(({ idx }) => {
      let numericCount = 0;
      let total = 0;
      for (const row of body) {
        const value = row[idx];
        if (!value || !value.trim()) continue;
        total += 1;
        if (!Number.isNaN(Number(value))) {
          numericCount += 1;
        }
      }
      if (total === 0) return false;
      const threshold = total < 3 ? total : Math.max(3, Math.ceil(total * 0.5));
      return numericCount >= threshold;
    });

  const columns: ColumnSeries[] = [];
  for (const { header, idx } of numericColumns) {
    const points: { x: number; y: number }[] = [];
    for (const row of body) {
      const timestamp = normalizeTimestamp(row[timeIndex]);
      if (timestamp === null) continue;
      const rawValue = row[idx];
      if (!rawValue || !rawValue.trim()) continue;
      const numericValue = Number(rawValue);
      if (Number.isNaN(numericValue)) continue;
      points.push({ x: timestamp, y: numericValue });
    }
    if (points.length === 0) continue;
    points.sort((a, b) => a.x - b.x);
    columns.push({
      column: header,
      data: points,
      color: paletteForLabel(`${filePath}-${header}`),
    });
  }

  if (columns.length === 0) {
    return null;
  }

  const starts = columns.map((column) => column.data[0]?.x ?? Number.POSITIVE_INFINITY);
  const ends = columns.map((column) => column.data[column.data.length - 1]?.x ?? 0);

  return {
    filePath,
    columns,
    start: Math.min(...starts),
    end: Math.max(...ends),
  };
}

export function DataExplorer({ workspace, files }: DataExplorerProps) {
  const [selected, setSelected] = useState<string[]>([]);
  const [seriesGroups, setSeriesGroups] = useState<FileSeries[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadSeries = async () => {
      if (selected.length === 0) {
        setSeriesGroups([]);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const groups: FileSeries[] = [];
        for (const path of selected) {
          const file = files.find((item) => item.path === path);
          if (!file) continue;
          const blob = await downloadFile(workspace, file.path);
          const text = await blob.text();
          const parsed = Papa.parse<string[]>(text, { skipEmptyLines: true });
          if (parsed.errors.length > 0) {
            throw new Error(parsed.errors[0].message);
          }
          const group = inferSeries(parsed, file.path);
          if (group) {
            groups.push(group);
          }
        }
        setSeriesGroups(groups);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load series');
      } finally {
        setLoading(false);
      }
    };
    void loadSeries();
  }, [workspace, selected, files]);

  const options = useMemo(() => files.filter((file) => file.path.endsWith('.csv')), [files]);

  return (
    <div className="panel chart-panel">
      <div className="panel-header">
        <h3>5 · Data explorer</h3>
        <p className="small">Overlay multiple metrics, zoom into intervals, and compare raw vs interpolated series.</p>
      </div>
      <div className="chart-controls">
        <div className="field">
          <label htmlFor="chart-select">CSV series</label>
          <select
            id="chart-select"
            multiple
            value={selected}
            onChange={(event) => {
              const values = Array.from(event.target.selectedOptions).map((option) => option.value);
              setSelected(values);
            }}
            size={Math.min(10, options.length)}
          >
            {options.map((file) => (
              <option key={file.path} value={file.path}>{file.path}</option>
            ))}
          </select>
        </div>
        {seriesGroups.length > 0 && (
          <div className="field">
            <label>Summary</label>
            <ul className="small">
              {seriesGroups.map((group) => (
                <li key={group.filePath}>
                  {group.filePath} · {group.columns.length} metrics · range {format(group.start, 'yyyy-MM-dd')} → {format(group.end, 'yyyy-MM-dd')}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
      {loading && <p className="status uploading">Loading series…</p>}
      {error && <p className="status error">{error}</p>}
      {seriesGroups.length > 0 && (
        <div className="chart-stacks">
          {seriesGroups.map((group) => (
            <div key={group.filePath} className="chart-stack">
              <h4>{group.filePath}</h4>
              <div className="chart-grid">
                {chunkColumns(group.columns, 2).map((row, rowIndex) => (
                  <div key={`${group.filePath}-row-${rowIndex}`} className="chart-row">
                    {row.map((column) => (
                      <div key={column.column} className="chart-card">
                        <div className="chart-card-header">
                          <h5>{column.column}</h5>
                        </div>
                        <div className="chart-card-body">
                          <Line
                            data={{
                              datasets: [
                                {
                                  label: column.column,
                                  data: column.data,
                                  borderColor: column.color,
                                  backgroundColor: `${column.color}55`,
                                  borderWidth: 2,
                                  pointRadius: 0,
                                  fill: true,
                                },
                              ],
                            }}
                            options={{
                              responsive: true,
                              maintainAspectRatio: false,
                              scales: {
                                x: {
                                  type: 'time',
                                  time: { unit: 'day' },
                                  ticks: { color: '#cbd5e1' },
                                  grid: { color: 'rgba(148, 163, 184, 0.2)' },
                                },
                                y: {
                                  ticks: { color: '#cbd5e1' },
                                  grid: { color: 'rgba(148, 163, 184, 0.2)' },
                                },
                              },
                              plugins: {
                                legend: { display: false },
                                tooltip: {
                                  callbacks: {
                                    title(items) {
                                      if (!items.length) return '';
                                      const date = new Date(items[0].parsed.x as number);
                                      return format(date, 'yyyy-MM-dd HH:mm');
                                    },
                                  },
                                },
                              },
                            }}
                            style={{ minHeight: 240 }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
