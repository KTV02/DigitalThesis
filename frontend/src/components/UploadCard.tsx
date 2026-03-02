import { ChangeEvent, useRef, useState } from 'react';
import { ArrowUpOnSquareIcon } from '@heroicons/react/24/outline';
import clsx from 'clsx';

export interface UploadCardProps {
  title: string;
  description: string;
  accept: string;
  accent?: 'emerald' | 'sky';
  onUpload: (file: File, params: UploadFormState) => Promise<void>;
  defaults: UploadFormState;
}

export interface UploadFormState {
  start: string;
  end: string;
  userId: string;
  sourceName?: string;
}

export function UploadCard({ title, description, accept, accent = 'emerald', onUpload, defaults }: UploadCardProps) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [form, setForm] = useState<UploadFormState>(defaults);
  const [status, setStatus] = useState<'idle' | 'uploading' | 'done' | 'error'>('idle');
  const [error, setError] = useState<string | null>(null);

  const handleFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) {
      return;
    }
    const file = event.target.files[0];
    if (!form.start || !form.end) {
      setError('Please provide both start and end dates.');
      setStatus('error');
      return;
    }
    try {
      setStatus('uploading');
      setError(null);
      await onUpload(file, form);
      setStatus('done');
    } catch (err) {
      setStatus('error');
      setError(err instanceof Error ? err.message : 'Upload failed');
    } finally {
      event.target.value = '';
    }
  };

  return (
    <div
      className={clsx(
        'upload-card',
        accent === 'emerald' ? 'upload-card-emerald' : 'upload-card-sky',
        status === 'uploading' && 'upload-card-busy'
      )}
      onClick={() => inputRef.current?.click()}
      role="button"
      tabIndex={0}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          inputRef.current?.click();
        }
      }}
    >
      <div className="upload-card-header">
        <ArrowUpOnSquareIcon className="upload-card-icon" />
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
      </div>
      <div className="upload-card-body">
        <div className="field">
          <label htmlFor={`${title}-start`}>Start date</label>
          <input
            id={`${title}-start`}
            type="date"
            value={form.start}
            onChange={(event) => setForm({ ...form, start: event.target.value })}
          />
        </div>
        <div className="field">
          <label htmlFor={`${title}-end`}>End date</label>
          <input
            id={`${title}-end`}
            type="date"
            value={form.end}
            onChange={(event) => setForm({ ...form, end: event.target.value })}
          />
        </div>
        <div className="field">
          <label htmlFor={`${title}-user`}>User ID</label>
          <input
            id={`${title}-user`}
            type="text"
            value={form.userId}
            onChange={(event) => setForm({ ...form, userId: event.target.value })}
          />
        </div>
        <div className="field">
          <label htmlFor={`${title}-source`}>Source filter (optional)</label>
          <input
            id={`${title}-source`}
            type="text"
            value={form.sourceName ?? ''}
            onChange={(event) => setForm({ ...form, sourceName: event.target.value || undefined })}
          />
        </div>
      </div>
      <div className="upload-card-footer">
        <p>Click to select a file ({accept}).</p>
        {status === 'uploading' && <p className="status uploading">Uploading & processing…</p>}
        {status === 'done' && <p className="status done">Completed ✓</p>}
        {status === 'error' && error && <p className="status error">{error}</p>}
      </div>
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        hidden
        onChange={handleFileChange}
      />
    </div>
  );
}
