import { useState, useCallback, useEffect } from 'react';
import type {
  CreateJobRequest,
  DatabaseSourcesResponse,
  OpenClawHealth,
  ReportJob,
  SSEEvent,
} from '../types/report';

const API_BASE = 'http://localhost:3001/api';

export function useCreateJob() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const create = useCallback(async (req: CreateJobRequest): Promise<string | null> => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/report-jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req),
      });
      if (!res.ok) {
        const body = await res.json();
        throw new Error(body.error || 'Request failed');
      }
      const data = await res.json();
      return data.jobId;
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  return { create, loading, error };
}

export function useJobList() {
  const [jobs, setJobs] = useState<ReportJob[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchJobs = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/report-jobs`);
      const data = await res.json();
      setJobs(data);
    } finally {
      setLoading(false);
    }
  }, []);

  return { jobs, loading, fetchJobs };
}

export function useOpenClawHealth() {
  const [health, setHealth] = useState<OpenClawHealth | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchHealth = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/openclaw/health`);
      const data = (await res.json()) as OpenClawHealth;
      setHealth(data);
      if (!res.ok && data.details.length > 0) {
        setError(data.details[0]);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setHealth(null);
    } finally {
      setLoading(false);
    }
  }, []);

  return { health, loading, error, fetchHealth };
}

export function useJobEvents(jobId: string | null) {
  const [events, setEvents] = useState<SSEEvent[]>([]);
  const [connected, setConnected] = useState(false);

  const connect = useCallback(() => {
    if (!jobId) return;

    setEvents([]);
    setConnected(true);

    const url = `${API_BASE}/report-jobs/${jobId}/events`;
    const source = new EventSource(url);

    source.onmessage = (e) => {
      try {
        const evt: SSEEvent = JSON.parse(e.data);
        setEvents(prev => [...prev, evt]);
        if (evt.type === 'done') {
          source.close();
          setConnected(false);
        }
      } catch {
        // ignore parse errors
      }
    };

    source.onerror = () => {
      source.close();
      setConnected(false);
    };

    return () => source.close();
  }, [jobId]);

  return { events, connected, connect };
}

export async function fetchJob(jobId: string): Promise<ReportJob> {
  const res = await fetch(`${API_BASE}/report-jobs/${jobId}`);
  if (!res.ok) throw new Error('Failed to fetch job');
  return res.json();
}

export function useJobPolling(jobId: string | null) {
  const [job, setJob] = useState<ReportJob | null>(null);
  const [loading, setLoading] = useState(Boolean(jobId));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!jobId) return undefined;

    let cancelled = false;
    let timer: number | undefined;

    const load = async () => {
      try {
        const next = await fetchJob(jobId);
        if (cancelled) return;
        setJob(next);
        setError(null);
        setLoading(false);

        if (next.status === 'queued' || next.status === 'running') {
          timer = window.setTimeout(load, 3000);
        }
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
        timer = window.setTimeout(load, 5000);
      }
    };

    void load();

    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, [jobId]);

  return { job, loading, error };
}

export async function fetchJobResult(jobId: string) {
  const res = await fetch(`${API_BASE}/report-jobs/${jobId}/result`);
  if (!res.ok) throw new Error('Failed to fetch result');
  return res.json();
}

export async function fetchDatabaseSources(jobId: string): Promise<DatabaseSourcesResponse> {
  const res = await fetch(`${API_BASE}/report-jobs/${jobId}/database-sources`);
  if (!res.ok) throw new Error('Failed to fetch database sources');
  return res.json();
}

export function getDownloadUrl(jobId: string, format = 'md') {
  return `${API_BASE}/report-jobs/${jobId}/download?format=${format}`;
}
