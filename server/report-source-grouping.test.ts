import assert from 'node:assert/strict';
import test from 'node:test';
import { ReportsService } from './reports.service.js';

type GroupingHarness = {
  toolSearchChannelSources: (
    researchSources: Array<Record<string, unknown>>,
    reportRefs: Array<Record<string, unknown>>,
    databaseRecall: Array<Record<string, unknown>>,
  ) => Array<{ id: string; url?: string }>;
};

function serviceForGroupingTests(): GroupingHarness {
  return Object.create(ReportsService.prototype) as GroupingHarness;
}

test('internet sources exclude report references without a public URL', () => {
  const service = serviceForGroupingTests();
  const result = service.toolSearchChannelSources(
    [{
      id: 'research-1',
      sourceGroup: 'tool_search',
      title: '外交部公开报道',
      url: 'https://www.mfa.gov.cn/example.html',
      sourceName: '外交部',
      publishTime: '2026-05-15',
      summary: '',
      sourceType: 'Tavily搜索',
      engine: 'tavily',
    }],
    [{
      id: 'reference-gap',
      sourceGroup: 'report_refs',
      title: '会晤成果框架存在不确定性',
      url: '',
      sourceName: '',
      publishTime: '',
      summary: '该项仍待核验。',
      sourceType: '报告引用',
    }],
    [],
  );

  assert.equal(result.length, 1);
  assert.equal(result[0]?.id, 'research-1');
  assert.ok(result.every((item) => Boolean(item.url)));
});

test('URL-backed report references remain available as internet fallback data', () => {
  const service = serviceForGroupingTests();
  const result = service.toolSearchChannelSources(
    [],
    [{
      id: 'reference-public',
      sourceGroup: 'report_refs',
      title: '公开报道',
      url: 'https://example.com/public-report',
      sourceName: 'example.com',
      publishTime: '',
      summary: '',
      sourceType: '报告引用',
    }],
    [],
  );

  assert.equal(result.length, 1);
  assert.equal(result[0]?.url, 'https://example.com/public-report');
});
