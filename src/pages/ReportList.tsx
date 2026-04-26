import { useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useJobList } from '../hooks/useApi';
import type { JobStatus, SkillName } from '../types/report';

const SKILL_LABELS: Record<SkillName, string> = {
  'risk-assessment-reports': '风险评估报告',
  'person-intelligence-report': '人物报告',
};

const STATUS_LABELS: Record<JobStatus, string> = {
  queued: '排队中',
  running: '生成中',
  succeeded: '已完成',
  failed: '失败',
  cancelled: '已取消',
  waiting_approval: '等待审批',
};

export function ReportList() {
  const { jobs, loading, fetchJobs } = useJobList();

  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, 5000);
    return () => clearInterval(interval);
  }, [fetchJobs]);

  return (
    <div className="page">
      <div className="list-header">
        <div>
          <h1>报告任务</h1>
          <p className="page-note">长报告任务通常需要 1 到 15 分钟，生成过程中可以进入详情页查看实时日志。</p>
        </div>
        <Link to="/reports/new" className="btn-primary">新建报告</Link>
      </div>

      {loading && jobs.length === 0 ? (
        <p>正在加载任务列表...</p>
      ) : jobs.length === 0 ? (
        <p className="placeholder">暂无报告任务，点击“新建报告”开始测试长报告生成。</p>
      ) : (
        <table className="job-table">
          <thead>
            <tr>
              <th>任务 ID</th>
              <th>报告类型</th>
              <th>状态</th>
              <th>创建时间</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr key={job.jobId}>
                <td className="mono">{job.jobId.slice(0, 8)}</td>
                <td>{SKILL_LABELS[job.skill]}</td>
                <td>
                  <span className={`status status-${job.status}`}>
                    {STATUS_LABELS[job.status]}
                  </span>
                </td>
                <td>{new Date(job.createdAt).toLocaleString('zh-CN')}</td>
                <td>
                  <Link to={`/reports/${job.jobId}`}>查看详情</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
