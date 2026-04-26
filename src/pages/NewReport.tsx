import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { RiskForm } from '../components/RiskForm';
import { PersonForm } from '../components/PersonForm';
import { useCreateJob, useOpenClawHealth } from '../hooks/useApi';
import type { SkillName, RiskAssessmentPayload, PersonReportPayload } from '../types/report';

const SKILLS: { value: SkillName; label: string; desc: string }[] = [
  {
    value: 'risk-assessment-reports',
    label: '风险评估报告',
    desc: '领导人出访、外国领导人来访、国内假期风险评估',
  },
  {
    value: 'person-intelligence-report',
    label: '人物报告',
    desc: '新上任领导人、来访外国政要画像',
  },
];

export function NewReport() {
  const [skill, setSkill] = useState<SkillName>('risk-assessment-reports');
  const { create, loading, error } = useCreateJob();
  const { health, loading: healthLoading, error: healthError, fetchHealth } = useOpenClawHealth();
  const navigate = useNavigate();

  useEffect(() => {
    fetchHealth();
  }, [fetchHealth]);

  const canSubmit = health?.ok ?? false;

  const handleRiskSubmit = async (payload: RiskAssessmentPayload) => {
    if (!canSubmit) {
      return;
    }

    const jobId = await create({ skill: 'risk-assessment-reports', payload });
    if (jobId) {
      navigate(`/reports/${jobId}`);
    }
  };

  const handlePersonSubmit = async (payload: PersonReportPayload) => {
    if (!canSubmit) {
      return;
    }

    const jobId = await create({ skill: 'person-intelligence-report', payload });
    if (jobId) {
      navigate(`/reports/${jobId}`);
    }
  };

  return (
    <div className="page">
      <h1>新建报告</h1>

      <div className="skill-selector">
        <label>报告类型</label>
        <div className="skill-cards">
          {SKILLS.map((item) => (
            <button
              key={item.value}
              type="button"
              className={`skill-card ${skill === item.value ? 'active' : ''}`}
              onClick={() => setSkill(item.value)}
            >
              <strong>{item.label}</strong>
              <span>{item.desc}</span>
            </button>
          ))}
        </div>
      </div>

      <div className={`health-banner health-${health?.status || 'down'}`}>
        <div className="health-banner-header">
          <strong>OpenClaw 运行检查</strong>
          <button type="button" className="btn-secondary" onClick={fetchHealth} disabled={healthLoading}>
            {healthLoading ? '检测中...' : '重新检测'}
          </button>
        </div>
        <p>
          {healthLoading && !health
            ? '正在检测 OpenClaw、本地 provider 和环境变量。'
            : health?.ok
              ? '运行条件正常，可以提交报告生成任务。'
              : healthError || health?.details[0] || 'OpenClaw 当前不可用，暂不允许提交任务。'}
        </p>
        {health && !health.ok && health.details.length > 1 && (
          <ul className="health-list">
            {health.details.slice(1).map((detail) => (
              <li key={detail}>{detail}</li>
            ))}
          </ul>
        )}
      </div>

      <div className="long-task-tip">
        <strong>长报告任务模式</strong>
        <p>提交后会创建后台任务并持续执行，通常需要 1 到 5 分钟。生成过程中可以离开当前页面，稍后回到任务列表查看进度。</p>
      </div>

      <div className="form-container">
        {skill === 'risk-assessment-reports' && (
          <RiskForm onSubmit={handleRiskSubmit} loading={loading || !canSubmit} />
        )}
        {skill === 'person-intelligence-report' && (
          <PersonForm onSubmit={handlePersonSubmit} loading={loading || !canSubmit} />
        )}
        {error && <p className="error">{error}</p>}
      </div>
    </div>
  );
}
