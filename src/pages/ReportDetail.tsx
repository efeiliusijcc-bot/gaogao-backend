import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import DOMPurify from 'dompurify';
import { fetchJobResult, getDownloadUrl, useJobPolling } from '../hooks/useApi';

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
