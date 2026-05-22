import axios from 'axios';
import * as cheerio from 'cheerio';
import iconv from 'iconv-lite';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';

export interface TickerInfo {
  Code: string;
  Name: string;
  Mcap: number;
}

export interface FinancialData {
  번호: number;
  종목코드: string;
  종목명: string;
  산업카테고리: string | null;
  '시가총액(억)': number | null;
  '현재 PER': number | null;
  '선행 PER': number | null;
  '전년 영업이익': number | null;
  '추정 영업이익': number | null;
  '추정 ROE': number | null;
  부채비율: number | null;
  이익성장률: number | null;
  DeltaPER: number | null;
}

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
};

const CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days (virtually infinite, only updates on manual refresh)

// Dynamic cache storage helpers for Vercel serverless environments
async function readCacheFile(): Promise<string> {
  const tmpPath = path.join('/tmp', 'delta_per_cache.json');
  try {
    // 1. Try to read from Serverless /tmp space (which might have manually refreshed data)
    return await fs.readFile(tmpPath, 'utf-8');
  } catch (e) {
    // 2. Fallback to bundled cache file in project root
    const rootPath = path.join(process.cwd(), 'delta_per_cache.json');
    return await fs.readFile(rootPath, 'utf-8');
  }
}

async function writeCacheFile(content: string): Promise<void> {
  // 1. Try writing to project root first (standard local development)
  const rootPath = path.join(process.cwd(), 'delta_per_cache.json');
  try {
    await fs.writeFile(rootPath, content, 'utf-8');
    console.log('Successfully saved cache to project root.');
  } catch (e) {
    // 2. If it fails (e.g., Read-only filesystem on Vercel serverless), write to /tmp instead
    const tmpPath = path.join('/tmp', 'delta_per_cache.json');
    try {
      await fs.writeFile(tmpPath, content, 'utf-8');
      console.log('Saved cache to serverless /tmp due to read-only filesystem.');
    } catch (err) {
      console.error('Failed to write cache to both root and /tmp:', err);
      throw err;
    }
  }
}

// Helper to fetch euc-kr encoded HTML (used by sise_market_sum)
async function fetchEucKrHtml(url: string): Promise<string> {
  const response = await axios.get(url, {
    headers: HEADERS,
    responseType: 'arraybuffer',
    timeout: 5000,
  });
  return iconv.decode(Buffer.from(response.data), 'euc-kr');
}

// Helper to fetch utf-8 encoded HTML (used by item/main)
async function fetchUtf8Html(url: string): Promise<string> {
  const response = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
  return await response.text();
}

async function fetchTickerPage(sosok: number, page: number): Promise<TickerInfo[]> {
  const url = `https://finance.naver.com/sise/sise_market_sum.naver?sosok=${sosok}&page=${page}`;
  try {
    const html = await fetchEucKrHtml(url);
    const $ = cheerio.load(html);
    const tickers: TickerInfo[] = [];

    $('table.type_2 tr').each((_, row) => {
      const cols = $(row).find('td');
      if (cols.length <= 1) return;

      const aTag = $(row).find('a[href*="/item/main.naver?code="]');
      if (aTag.length > 0) {
        const href = aTag.attr('href') || '';
        const code = href.split('code=').pop()?.trim() || '';
        const name = aTag.text().trim();
        const mcapStr = $(cols[6]).text().replace(/,/g, '').trim();
        const mcap = parseInt(mcapStr, 10);

        if (code && name && !isNaN(mcap)) {
          tickers.push({ Code: code, Name: name, Mcap: mcap });
        }
      }
    });
    return tickers;
  } catch (error) {
    console.error(`Error fetching tickers for sosok ${sosok} page ${page}:`, error);
    return [];
  }
}

export async function getTop500Tickers(): Promise<TickerInfo[]> {
  const tasks: Promise<TickerInfo[]>[] = [];
  for (const sosok of [0, 1]) {
    for (let page = 1; page <= 5; page++) {
      tasks.push(fetchTickerPage(sosok, page));
    }
  }

  const results = await Promise.all(tasks);
  let allTickers = results.flat();
  allTickers.sort((a, b) => b.Mcap - a.Mcap);
  return allTickers.slice(0, 500);
}

export async function getFinancialData(ticker: TickerInfo): Promise<FinancialData> {
  const { Code: code, Name: name } = ticker;
  const url = `https://finance.naver.com/item/main.naver?code=${code}`;
  
  const data: FinancialData = {
    번호: 0,
    종목코드: code,
    종목명: name,
    산업카테고리: null,
    '시가총액(억)': null,
    '현재 PER': null,
    '선행 PER': null,
    '전년 영업이익': null,
    '추정 영업이익': null,
    '추정 ROE': null,
    부채비율: null,
    이익성장률: null,
    DeltaPER: null,
  };

  try {
    const html = await fetchUtf8Html(url);
    const $ = cheerio.load(html);

    // 1. Category & Mcap
    const categoryTag = $('div.section h4.h_sub.sub_tit7 a');
    if (categoryTag.length > 0) data.산업카테고리 = categoryTag.first().text().trim();

    const mcapTag = $('#_market_sum');
    if (mcapTag.length > 0) {
      const mcapVal = mcapTag.text().replace(/,/g, '').replace(/조/g, '').replace(/\s/g, '');
      const parsed = parseFloat(mcapVal);
      if (!isNaN(parsed)) data['시가총액(억)'] = parsed;
    }

    // 2. PER Values
    const getSafeFloat = (selector: string) => {
      const text = $(selector).text().replace(/,/g, '');
      const val = parseFloat(text);
      return isNaN(val) ? null : val;
    };

    data['현재 PER'] = getSafeFloat('#_per');
    data['선행 PER'] = getSafeFloat('#_cns_per') ?? data['현재 PER'];
    
    if (data['현재 PER'] !== null && data['선행 PER'] !== null) {
      data.DeltaPER = Number((data['현재 PER'] - data['선행 PER']).toFixed(2));
    }

    // 3. Financial Table Data
    const rows = $('table.tb_type1.tb_num.tb_type1_ifrs tbody tr');
    rows.each((_, row) => {
      const thText = $(row).find('th').text().trim();
      const tds = $(row).find('td');
      if (tds.length < 4) return;

      const getVal = (idx1: number, idx2: number) => {
        const v1 = $(tds[idx1]).text().replace(/,/g, '').trim();
        if (v1 && v1 !== '-') {
          const p1 = parseFloat(v1);
          if (!isNaN(p1)) return p1;
        }
        const v2 = $(tds[idx2]).text().replace(/,/g, '').trim();
        if (v2 && v2 !== '-') {
          const p2 = parseFloat(v2);
          if (!isNaN(p2)) return p2;
        }
        return null;
      };

      if (thText.includes('영업이익') && !thText.includes('률')) {
        data['전년 영업이익'] = getVal(2, 2);
        data['추정 영업이익'] = getVal(3, 2);
      } else if (thText.includes('ROE')) {
        data['추정 ROE'] = getVal(3, 2);
      } else if (thText.includes('부채비율')) {
        data['부채비율'] = getVal(3, 2);
      }
    });

    if (data['전년 영업이익'] && data['추정 영업이익'] && data['전년 영업이익'] !== 0) {
      data.이익성장률 = Number(((data['추정 영업이익'] / data['전년 영업이익']) - 1.0).toFixed(4));
    }

  } catch (error) {
    // ignore
  }
  
  return data;
}

export async function scrapeAllData(force: boolean = false): Promise<{ data: FinancialData[], scrapeTime: string }> {
  if (!force) {
    try {
      const cachedData = await readCacheFile();
      const parsed = JSON.parse(cachedData);
      console.log('Serving from local cache (force=false).');
      return parsed;
    } catch (e) {
      console.log('Cache file not found, and force=false. Throwing cache-miss error.');
      throw new Error('캐시된 데이터가 존재하지 않습니다. 먼저 "새로 크롤링"을 눌러 데이터를 수집해주세요.');
    }
  }

  console.log('Starting fresh KOSPI/KOSDAQ top 500 scrape (force=true)...');

  const tickers = await getTop500Tickers();
  const results: FinancialData[] = [];
  
  // Throttle concurrency to 10
  const limit = 10;
  for (let i = 0; i < tickers.length; i += limit) {
    const chunk = tickers.slice(i, i + limit);
    const chunkResults = await Promise.all(chunk.map(getFinancialData));
    results.push(...chunkResults);
  }

  results.forEach((item, idx) => {
    item.번호 = idx + 1;
  });

  const scrapeTime = new Date().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' });
  const finalResult = { data: results, scrapeTime };

  try {
    await writeCacheFile(JSON.stringify(finalResult));
  } catch (e) {
    console.error("Failed to write cache", e);
  }

  return finalResult;
}
