import assert from 'node:assert/strict';
import test from 'node:test';
import { derivePublishTime, deriveSourceName, parseReportReferenceEntry } from './report-reference-parser.js';

test('keeps a publisher with brackets and commas separate from the title', () => {
  const result = parseReportReferenceEntry(
    1,
    '[1] 新浪财经（转载《财经》杂志，张明），《2026年全球黄金价格走势展望》，2025年12月16日，https://finance.sina.com.cn/roll/2025-12-16/doc-inhazaxf8303962.shtml',
  );

  assert.deepEqual(result, {
    sourceName: '新浪财经（转载《财经》杂志，张明）',
    title: '《2026年全球黄金价格走势展望》',
    summary: '新浪财经（转载《财经》杂志，张明），《2026年全球黄金价格走势展望》，2025年12月16日',
    url: 'https://finance.sina.com.cn/roll/2025-12-16/doc-inhazaxf8303962.shtml',
    publishTime: '2025-12-16',
  });
});

test('supports a simple reference without optional metadata', () => {
  const result = parseReportReferenceEntry(2, '[2] 联合早报，《金价走势分析》');
  assert.equal(result?.sourceName, '联合早报');
  assert.equal(result?.title, '《金价走势分析》');
  assert.equal(result?.url, '');
  assert.equal(result?.publishTime, '');
});

test('derives source name and date from public search metadata', () => {
  const title = '王毅向媒体介绍中美元首会晤情况和共识 - 外交部';
  const url = 'https://www.mfa.gov.cn/web/ziliao_674904/zyjh_674906/202605/t20260515_11911513.shtml';
  assert.equal(deriveSourceName(title, url), '外交部');
  assert.equal(derivePublishTime(title, url), '2026-05-15');
});

test('falls back to hostname and supports dates in a Chinese title', () => {
  assert.equal(deriveSourceName('一条没有机构后缀的公开报道', 'https://example.org/article/42'), 'example.org');
  assert.equal(derivePublishTime('2026年5月15日外交部发言人主持记者会', ''), '2026-05-15');
});

test('derives a publication date from a standalone date near the page heading', () => {
  const metadata = [
    '## 赵明昊：中美元首立春通话，“春天有约”',
    '',
    '2026-02-07',
    '',
    '2月4日晚，两国元首通电话。',
  ].join('\n');
  assert.equal(derivePublishTime('赵明昊：中美元首立春通话，“春天有约”', 'https://example.org/article', metadata), '2026-02-07');
});

test('derives a publication date from an explicit source metadata block', () => {
  const metadata = [
    '搜索搜索',
    '首页 > 新闻发布',
    '来源：新华社',
    '类型：转载 分类：新闻 2026-05-14 14:08',
    '习近平同美国总统特朗普会谈',
  ].join('\n');
  assert.equal(derivePublishTime('习近平同美国总统特朗普会谈', 'https://example.org/article', metadata), '2026-05-14');
});

test('does not treat a date mentioned in article content as publication metadata', () => {
  const metadata = [
    '## 会谈背景与影响',
    '',
    '文章回顾了2026年5月7日的例行记者会，并分析后续影响。',
  ].join('\n');
  assert.equal(derivePublishTime('会谈背景与影响', 'https://example.org/article', metadata), '');
});
