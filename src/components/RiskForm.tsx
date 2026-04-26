import { useState } from 'react';
import type { RiskAssessmentPayload, RiskScenario } from '../types/report';

const SCENARIOS: { value: RiskScenario; label: string }[] = [
  { value: 'leader_outbound', label: '领导人出访风险评估' },
  { value: 'foreign_leader_visit', label: '外国领导人来访风险评估' },
  { value: 'domestic_holiday', label: '国内假期风险评估' },
];

const FOCUS_OPTIONS = ['公共安全', '舆情', '治安', '传染病', '气象灾害', '交通', '涉外机构', '抗议活动', '社会稳定', '极端天气'];

interface Props {
  onSubmit: (payload: RiskAssessmentPayload) => void;
  loading: boolean;
}

export function RiskForm({ onSubmit, loading }: Props) {
  const [scenario, setScenario] = useState<RiskScenario>('leader_outbound');
  const [targetCountry, setTargetCountry] = useState('');
  const [targetCity, setTargetCity] = useState('');
  const [visitTime, setVisitTime] = useState('');
  const [holidayName, setHolidayName] = useState('');
  const [holidayTime, setHolidayTime] = useState('');
  const [timeWindow, setTimeWindow] = useState('近30天');
  const [focusAreas, setFocusAreas] = useState<string[]>([]);
  const [knownContext, setKnownContext] = useState('');

  const toggleFocus = (item: string) => {
    setFocusAreas((prev) => (prev.includes(item) ? prev.filter((value) => value !== item) : [...prev, item]));
  };

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    onSubmit({
      scenario,
      target_country: targetCountry || undefined,
      target_city: targetCity || undefined,
      visit_time: visitTime || undefined,
      holiday_name: holidayName || undefined,
      holiday_time: holidayTime || undefined,
      time_window: timeWindow || undefined,
      focus_areas: focusAreas.length > 0 ? focusAreas : undefined,
      known_context: knownContext || undefined,
      language: 'zh-CN',
    });
  };

  return (
    <form onSubmit={handleSubmit}>
      <div className="form-tip">
        <strong>长任务提示</strong>
        <p>风险评估报告会触发较多检索与交叉核验，建议填写准确的国家、城市和时间范围，减少无效搜索。</p>
      </div>

      <div className="form-group">
        <label>报告场景 *</label>
        <select value={scenario} onChange={(event) => setScenario(event.target.value as RiskScenario)}>
          {SCENARIOS.map((item) => (
            <option key={item.value} value={item.value}>{item.label}</option>
          ))}
        </select>
      </div>

      {(scenario === 'leader_outbound' || scenario === 'foreign_leader_visit') && (
        <>
          <div className="form-group">
            <label>目标国家 *</label>
            <input value={targetCountry} onChange={(event) => setTargetCountry(event.target.value)} placeholder="如：法国" required />
          </div>

          {scenario === 'leader_outbound' && (
            <div className="form-group">
              <label>目标城市 *</label>
              <input value={targetCity} onChange={(event) => setTargetCity(event.target.value)} placeholder="如：巴黎" required />
            </div>
          )}

          <div className="form-group">
            <label>访问时间 *</label>
            <input value={visitTime} onChange={(event) => setVisitTime(event.target.value)} placeholder="如：2026-05-12 至 2026-05-15" required />
          </div>
        </>
      )}

      {scenario === 'domestic_holiday' && (
        <>
          <div className="form-group">
            <label>假期名称 *</label>
            <input value={holidayName} onChange={(event) => setHolidayName(event.target.value)} placeholder="如：五一假期" required />
          </div>
          <div className="form-group">
            <label>假期时间 *</label>
            <input value={holidayTime} onChange={(event) => setHolidayTime(event.target.value)} placeholder="如：2026-05-01 至 2026-05-05" required />
          </div>
        </>
      )}

      <div className="form-group">
        <label>检索时间窗口</label>
        <input value={timeWindow} onChange={(event) => setTimeWindow(event.target.value)} />
      </div>

      <div className="form-group">
        <label>重点关注领域</label>
        <div className="tag-group">
          {FOCUS_OPTIONS.map((item) => (
            <button
              type="button"
              key={item}
              className={`tag ${focusAreas.includes(item) ? 'active' : ''}`}
              onClick={() => toggleFocus(item)}
            >
              {item}
            </button>
          ))}
        </div>
      </div>

      <div className="form-group">
        <label>已知背景</label>
        <textarea
          value={knownContext}
          onChange={(event) => setKnownContext(event.target.value)}
          rows={3}
          placeholder="填写你已掌握的背景事实、访问目的或风险重点"
        />
      </div>

      <button type="submit" className="btn-primary" disabled={loading}>
        {loading ? '正在提交长报告任务...' : '生成风险长报告'}
      </button>
    </form>
  );
}
