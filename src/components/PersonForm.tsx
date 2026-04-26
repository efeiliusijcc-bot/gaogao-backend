import { useState } from 'react';
import type { OutputDepth, PersonReportPayload, PersonReportType } from '../types/report';

const REPORT_TYPES: { value: PersonReportType; label: string }[] = [
  { value: 'new_leader', label: '新上任领导人人物报告' },
  { value: 'visiting_dignitary', label: '来访外国政要人物报告' },
];

const DEPTH_OPTIONS: { value: OutputDepth; label: string }[] = [
  { value: 'brief', label: '简版' },
  { value: 'standard', label: '标准版' },
  { value: 'detailed', label: '详版' },
];

const FOCUS_OPTIONS = ['对华贸易', '台海', '科技', '能源', '安全', '人权', '经贸合作', '科技政策', '南海', '一带一路'];

interface Props {
  onSubmit: (payload: PersonReportPayload) => void;
  loading: boolean;
}

export function PersonForm({ onSubmit, loading }: Props) {
  const [targetName, setTargetName] = useState('');
  const [countryOrRegion, setCountryOrRegion] = useState('');
  const [currentPosition, setCurrentPosition] = useState('');
  const [reportType, setReportType] = useState<PersonReportType>('visiting_dignitary');
  const [visitContext, setVisitContext] = useState('');
  const [appointmentContext, setAppointmentContext] = useState('');
  const [focusAreas, setFocusAreas] = useState<string[]>([]);
  const [outputDepth, setOutputDepth] = useState<OutputDepth>('standard');

  const toggleFocus = (item: string) => {
    setFocusAreas((prev) => (prev.includes(item) ? prev.filter((value) => value !== item) : [...prev, item]));
  };

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    onSubmit({
      target_name: targetName,
      country_or_region: countryOrRegion,
      current_position: currentPosition,
      report_type: reportType,
      visit_context: visitContext || undefined,
      appointment_context: appointmentContext || undefined,
      focus_areas: focusAreas.length > 0 ? focusAreas : undefined,
      output_depth: outputDepth,
      language: 'zh-CN',
    });
  };

  return (
    <form onSubmit={handleSubmit}>
      <div className="form-tip">
        <strong>长任务提示</strong>
        <p>人物报告通常需要较长的检索和整理时间，建议填写尽可能明确的背景信息以减少空转。</p>
      </div>

      <div className="form-group">
        <label>目标人物姓名 *</label>
        <input value={targetName} onChange={(event) => setTargetName(event.target.value)} placeholder="如：Jane Doe" required />
      </div>

      <div className="form-group">
        <label>所属国家/地区 *</label>
        <input value={countryOrRegion} onChange={(event) => setCountryOrRegion(event.target.value)} placeholder="如：英国" required />
      </div>

      <div className="form-group">
        <label>当前职务 *</label>
        <input value={currentPosition} onChange={(event) => setCurrentPosition(event.target.value)} placeholder="如：外交大臣" required />
      </div>

      <div className="form-group">
        <label>报告类型 *</label>
        <select value={reportType} onChange={(event) => setReportType(event.target.value as PersonReportType)}>
          {REPORT_TYPES.map((item) => (
            <option key={item.value} value={item.value}>{item.label}</option>
          ))}
        </select>
      </div>

      {reportType === 'visiting_dignitary' && (
        <div className="form-group">
          <label>来访背景</label>
          <textarea
            value={visitContext}
            onChange={(event) => setVisitContext(event.target.value)}
            rows={3}
            placeholder="如：访问背景、访问级别、会见场景、双边议题等"
          />
        </div>
      )}

      {reportType === 'new_leader' && (
        <div className="form-group">
          <label>上任背景</label>
          <textarea
            value={appointmentContext}
            onChange={(event) => setAppointmentContext(event.target.value)}
            rows={3}
            placeholder="如：选举、任命、党内变化或组阁背景"
          />
        </div>
      )}

      <div className="form-group">
        <label>重点关注议题</label>
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
        <label>报告深度</label>
        <select value={outputDepth} onChange={(event) => setOutputDepth(event.target.value as OutputDepth)}>
          {DEPTH_OPTIONS.map((item) => (
            <option key={item.value} value={item.value}>{item.label}</option>
          ))}
        </select>
      </div>

      <button type="submit" className="btn-primary" disabled={loading}>
        {loading ? '正在提交长报告任务...' : '生成人物长报告'}
      </button>
    </form>
  );
}
