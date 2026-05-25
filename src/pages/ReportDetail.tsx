import { useCallback, useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import DOMPurify from 'dompurify';
import { fetchDatabaseSources, fetchJobResult, getDownloadUrl, useJobPolling } from '../hooks/useApi';
import type { DatabaseSourcesResponse } from '../types/report';

function DatabaseSourcesCard({ jobId, jobStatus }: { jobId: string; jobStatus?: string }) {
  const [data, setData] = useState<DatabaseSourcesResponse | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchDatabaseSources(jobId);
      setData(result);
    } catch {
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    void load();
    if (jobStatus === 'queued' || jobStatus === 'running') {
      const timer = setInterval(load, 8000);
      return () => clearInterval(timer);
    }
  }, [load, jobStatus]);

  const isRunning = jobStatus === 'queued' || jobStatus === 'running';
  const showUnavailable = !data || data.status === 'unavailable';

  if (loading && !data) {
    return (
      <div className="db-sources-card">
        <h3>数据库信源检索</h3>
        <p className="db-sources-placeholder">正在检查数据库信源...</p>
      </div>
    );
  }

  if (showUnavailable) {
    return (
      <div className="db-sources-card">
        <h3>数据库信源检索</h3>
        <p className="db-sources-placeholder">
          {isRunning ? '数据库信源仍在检索或尚未落盘' : '数据库信源仍在检索或尚未落盘'}
        </p>
      </div>
    );
  }

  const visibleSources = expanded ? data.sources : data.sources.slice(0, 8);
  const hasMore = data.sources.length > 8;

  return (
    <div className="db-sources-card">
      <h3>数据库信源检索</h3>

      {data.status === 'hit' && (
        <>
          <div className="db-sources-summary">
            数据库命中 <strong>{data.totalHits}</strong> 条信源
            {data.updatedAt && (
              <span className="db-sources-time">
                {' '}| {new Date(data.updatedAt).toLocaleString('zh-CN')}
              </span>
            )}
          </div>
          <ul className="db-sources-list">
            {visibleSources.map((source, i) => (
              <li key={i} className="db-source-item">
                <div className="db-source-title">
                  {source.url ? (
                    <a href={source.url} target="_blank" rel="noopener noreferrer">
                      {source.title || source.url}
                    </a>
                  ) : (
                    <span>{source.title || '(无标题)'}</span>
                  )}
                </div>
                {source.summary && (
                  <p className="db-source-summary">{source.summary}</p>
                )}
                <div className="db-source-meta">
                  {source.websiteName && <span>{source.websiteName}</span>}
                  {source.publishTime && <span>{source.publishTime}</span>}
                </div>
              </li>
            ))}
          </ul>
          {hasMore && (
            <button className="db-sources-toggle" onClick={() => setExpanded(!expanded)}>
              {expanded ? '收起' : `查看更多 (共 ${data.sources.length} 条)`}
            </button>
          )}
        </>
      )}

      {(data.status === 'empty' || data.status === 'fallback') && (
        <div className="db-sources-fallback">
          <p>数据库无直接命中，已回退公开检索</p>
          {data.fallbackReason && (
            <p className="db-sources-reason">原因：{data.fallbackReason}</p>
          )}
        </div>
      )}
    </div>
  );
}

export function ReportDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const { job, loading, error } = useJobPolling(jobId || null);
  const [html, setHtml] = useState('');
  const [resultLoaded, setResultLoaded] = useState(false);
  const [resultError, setResultError] = useState<string | null>(null);

  useEffect(() => {
    if (!jobId || job?.status !== 'succeeded' || resultLoaded) return;

    fetchJobResult(jobId)
      .then((data) => {
        setHtml(DOMPurify.sanitize(data.html || ''));
        setResultLoaded(true);
        setResultError(null);
      })
      .catch((err) => {
        setResultError(err instanceof Error ? err.message : String(err));
      });
  }, [job?.status, jobId, resultLoaded]);

  const isRunning = job?.status === 'queued' || job?.status === 'running';
  const failedMessage = job?.errorMessage || error || resultError;

  return (
    <div className="page">
      <div className="detail-header">
        <Link to="/reports">返回任务列表</Link>
        <h1>报告任务 {jobId?.slice(0, 8)}</h1>
      </div>

      <div className="long-task-tip">
        <strong>长报告后台任务模式</strong>
        <p>
          点击生成后，前端只轮询任务状态；后端独立运行 OpenClaw，成功后把 Markdown 落盘，页面再读取文件内容展示。
        </p>
      </div>

      <div className="detail-grid">
        <div className="detail-sidebar">
          <h3>任务状态</h3>
          <div className="log-box">
            {loading && <div className="log-entry running">正在读取任务状态...</div>}
            {job && (
              <>
                <div className="log-entry">
                  <span className="log-stage">[status]</span> {job.status}
                </div>
                {job.stage && (
                  <div className="log-entry">
                    <span className="log-stage">[stage]</span> {job.stage}
                  </div>
                )}
                {job.resultPath && (
                  <div className="log-entry">
                    <span className="log-stage">[file]</span> {job.resultPath}
                  </div>
                )}
              </>
            )}
            {isRunning && <div className="log-entry running">后台正在生成，页面会自动轮询状态...</div>}
            {failedMessage && <div className="log-entry error">{failedMessage}</div>}
          </div>

          {jobId && <DatabaseSourcesCard jobId={jobId} jobStatus={job?.status} />}

          {resultLoaded && jobId && (
            <div className="download-section">
              <a href={getDownloadUrl(jobId, 'md')} download className="btn-primary">
                下载 Markdown
              </a>
            </div>
          )}
        </div>

        <div className="detail-main">
          <h3>报告预览</h3>
          <div className="markdown-preview" dangerouslySetInnerHTML={{ __html: html }} />
          {!html && isRunning && <p className="placeholder">报告生成中。生成成功后会自动加载 Markdown 文件。</p>}
          {!html && job?.status === 'failed' && <p className="placeholder">报告生成失败，请查看左侧错误信息。</p>}
          {!html && job?.status === 'waiting_approval' && <p className="placeholder">OpenClaw 正在等待工具审批。</p>}
          {!html && !isRunning && !failedMessage && <p className="placeholder">暂无报告内容。</p>}
        </div>
      </div>
    </div>
  );
}
