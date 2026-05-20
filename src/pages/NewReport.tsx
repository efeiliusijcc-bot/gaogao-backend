import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { RiskForm } from '../components/RiskForm';
import { PersonForm } from '../components/PersonForm';
import { useCreateJob, useOpenClawHealth } from '../hooks/useApi';
import type {
  PersonReportPayload,
  RiskAssessmentPayload,
  SkillName,
  WriteHbPayload,
} from '../types/report';

type ReportChoice =
  | { id: 'write-hb-hb'; skill: 'write-hb'; label: string; desc: string }
  | { id: 'risk-assessment-reports'; skill: 'risk-assessment-reports'; label: string; desc: string }
  | { id: 'person-intelligence-report'; skill: 'person-intelligence-report'; label: string; desc: string };

const REPORT_CHOICES: ReportChoice[] = [
  {
    id: 'write-hb-hb',
    skill: 'write-hb',
    label: 'HB报',
    desc: '六段式深度事件分析、趋势研判与建议。',
  },
  {
    id: 'risk-assessment-reports',
    skill: 'risk-assessment-reports',
    label: '风险评估报告',
    desc: '领导人出访、外方来访、国内假期等风险评估。',
  },
  {
    id: 'person-intelligence-report',
    skill: 'person-intelligence-report',
    label: '人物报告',
    desc: '新任领导人、来访外国政要人物画像。',
  },
];

export function NewReport() {
  const [choiceId, setChoiceId] = useState<ReportChoice['id']>('write-hb-hb');
  const [hbTopic, setHbTopic] = useState('');
  const [hbOutline, setHbOutline] = useState('');
  const [hbFocusAreas, setHbFocusAreas] = useState('');
  const [hbKnownContext, setHbKnownContext] = useState('');
  const { create, loading, error } = useCreateJob();
  const { health, loading: healthLoading, error: healthError, fetchHealth } = useOpenClawHealth();
  const navigate = useNavigate();

  useEffect(() => {
    fetchHealth();
  }, [fetchHealth]);

  const canSubmit = health?.ok ?? false;
  const choice = REPORT_CHOICES.find((item) => item.id === choiceId) ?? REPORT_CHOICES[0];
  const submitLoading = loading || !canSubmit;

  const createAndNavigate = async (skill: SkillName, payload: RiskAssessmentPayload | PersonReportPayload | WriteHbPayload) => {
    if (!canSubmit) return;
    const jobId = await create({ skill, payload });
    if (jobId) navigate(`/reports/${jobId}`);
  };

  const handleHbSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const focusAreas = hbFocusAreas
      .split(/[,，\n]/)
      .map((item) => item.trim())
      .filter(Boolean);

    void createAndNavigate('write-hb', {
      topic: hbTopic,
      report_type: 'HB报',
      outline: hbOutline || undefined,
      focus_areas: focusAreas.length > 0 ? focusAreas : undefined,
      known_context: hbKnownContext || undefined,
      language: 'zh-CN',
    });
  };

  const handleRiskSubmit = (payload: RiskAssessmentPayload) => {
    void createAndNavigate('risk-assessment-reports', payload);
  };

  const handlePersonSubmit = (payload: PersonReportPayload) => {
    void createAndNavigate('person-intelligence-report', payload);
  };

  return (
    <div className="page">
      <h1>新建报告</h1>

      <div className="skill-selector">
        <label>报告类型</label>
        <div className="skill-cards">
          {REPORT_CHOICES.map((item) => (
            <button
              key={item.id}
              type="button"
              className={`skill-card ${choiceId === item.id ? 'active' : ''}`}
              onClick={() => setChoiceId(item.id)}
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
        <p>提交后会创建后台任务并持续执行，生成过程中可以离开当前页面，稍后回到任务列表查看进度。</p>
      </div>

      <div className="form-container">
        {choice.skill === 'write-hb' && (
          <form onSubmit={handleHbSubmit}>
            <div className="form-tip">
              <strong>HB报编报任务</strong>
              <p>填写主题和必要背景后提交后台任务，系统会先规划调研方向，再生成正式报告。</p>
            </div>

            <div className="form-group">
              <label>报告主题 *</label>
              <input
                value={hbTopic}
                onChange={(event) => setHbTopic(event.target.value)}
                placeholder="例如：某国际事件涉我风险研判"
                required
              />
            </div>

            <div className="form-group">
              <label>自定义大纲</label>
              <textarea
                value={hbOutline}
                onChange={(event) => setHbOutline(event.target.value)}
                rows={3}
                placeholder="可选。填写希望覆盖的章节、角度或格式要求。"
              />
            </div>

            <div className="form-group">
              <label>重点方向</label>
              <textarea
                value={hbFocusAreas}
                onChange={(event) => setHbFocusAreas(event.target.value)}
                rows={2}
                placeholder="可选。用逗号或换行分隔，例如：政策背景、各方立场、风险外溢"
              />
            </div>

            <div className="form-group">
              <label>已知背景</label>
              <textarea
                value={hbKnownContext}
                onChange={(event) => setHbKnownContext(event.target.value)}
                rows={4}
                placeholder="可选。填写已掌握事实、指定信源、选题约束或补充材料。"
              />
            </div>

            <button type="submit" className="btn-primary" disabled={submitLoading}>
              {submitLoading ? '正在提交编报任务...' : '生成HB报'}
            </button>
          </form>
        )}
        {choice.skill === 'risk-assessment-reports' && (
          <RiskForm onSubmit={handleRiskSubmit} loading={submitLoading} />
        )}
        {choice.skill === 'person-intelligence-report' && (
          <PersonForm onSubmit={handlePersonSubmit} loading={submitLoading} />
        )}
        {error && <p className="error">{error}</p>}
      </div>
    </div>
  );
}
