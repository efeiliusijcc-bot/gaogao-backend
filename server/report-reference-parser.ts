export interface ParsedReportReference {
  sourceName: string;
  title: string;
  summary: string;
  url: string;
  publishTime: string;
}

export function cleanReferenceText(value: unknown): string {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .replace(/^[，,。.；;\s]+|[，,。.；;\s]+$/g, '')
    .trim();
}

function extractUrl(value: string): string {
  return value.match(/https?:\/\/\S+/i)?.[0]?.replace(/[),.;，。；）]+$/g, '') || '';
}

function extractPublishTime(value: string): string {
  const match = value.match(/((?:19|20)\d{2})\s*[年\/-]\s*(\d{1,2})\s*[月\/-]\s*(\d{1,2})\s*日?/);
  if (!match) return '';
  return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`;
}

export function deriveSourceName(title: string, url: string): string {
  const titleSuffix = title.match(/\s(?:-|—|–)\s*([^|]+)$/)?.[1]?.trim()
    || title.match(/[_＿]\s*([^|]+)$/)?.[1]?.trim();
  if (titleSuffix && titleSuffix.length <= 100 && !/[。！？.!?]$/.test(titleSuffix)) {
    return titleSuffix;
  }
  try {
    return new URL(url).hostname.replace(/^www\./i, '');
  } catch {
    return '';
  }
}

export function derivePublishTime(title: string, url: string): string {
  const text = `${title} ${url}`;
  const separated = text.match(/(?:^|\D)((?:19|20)\d{2})[-\/]([01]?\d)[-\/]([0-3]?\d)(?:\D|$)/);
  if (separated) {
    return `${separated[1]}-${String(separated[2]).padStart(2, '0')}-${String(separated[3]).padStart(2, '0')}`;
  }
  const chinese = text.match(/((?:19|20)\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?/);
  if (chinese) {
    return `${chinese[1]}-${String(chinese[2]).padStart(2, '0')}-${String(chinese[3]).padStart(2, '0')}`;
  }
  const compact = text.match(/(?:^|\D)((?:19|20)\d{2})([01]\d)([0-3]\d)(?:\D|$)/);
  if (compact) {
    return `${compact[1]}-${compact[2]}-${compact[3]}`;
  }
  return '';
}

function splitTopLevelComma(value: string): [string, string] {
  let depth = 0;
  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    if ('（('.includes(char)) depth += 1;
    if ('）)'.includes(char)) depth = Math.max(0, depth - 1);
    if (depth === 0 && '，,'.includes(char)) {
      return [value.slice(0, index), value.slice(index + 1)];
    }
  }
  return [value, ''];
}

export function parseReportReferenceEntry(number: number, value: unknown): ParsedReportReference | null {
  const raw = cleanReferenceText(value);
  if (!number || !raw) return null;
  const withoutNumber = cleanReferenceText(
    raw.replace(new RegExp(`^(?:\\[|〔|【)${number}(?:\\]|〕|】)\\s*`), ''),
  );
  if (!withoutNumber) return null;

  const url = extractUrl(withoutNumber);
  const publishTime = extractPublishTime(withoutNumber);
  const withoutUrl = cleanReferenceText(withoutNumber.replace(url, ''));
  const withoutDate = cleanReferenceText(
    withoutUrl.replace(/(?:19|20)\d{2}\s*[年\/-]\s*\d{1,2}\s*[月\/-]\s*\d{1,2}\s*日?/, ''),
  );

  const titleMatches = [...withoutDate.matchAll(/《[^》]+》/g)];
  let sourceName = '';
  let title = '';
  if (titleMatches.length) {
    const match = titleMatches[titleMatches.length - 1];
    title = cleanReferenceText(match[0]);
    sourceName = cleanReferenceText(withoutDate.slice(0, match.index).replace(/[，,]+$/g, ''));
  } else {
    const [source, parsedTitle] = splitTopLevelComma(withoutDate);
    sourceName = cleanReferenceText(source);
    title = cleanReferenceText(parsedTitle || withoutDate);
  }

  return {
    sourceName: sourceName || '--',
    title: title || '--',
    summary: withoutUrl || withoutNumber,
    url,
    publishTime,
  };
}
