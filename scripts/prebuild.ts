import { scrapeAllData } from '../src/lib/scraper';

async function main() {
  console.log('=== Pre-building KOSPI/KOSDAQ Stocks Cache ===');
  try {
    const result = await scrapeAllData(true); // force=true to trigger Naver Finance scrape and generate root delta_per_cache.json
    console.log(`Pre-build Scrape Success. Scraped ${result.data.length} stocks at ${result.scrapeTime}`);
  } catch (error) {
    console.error('Pre-build Scrape Failed:', error);
    process.exit(1); // Block build on critical scrape failure
  }
}

main();
