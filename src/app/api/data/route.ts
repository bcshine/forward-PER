import { NextResponse } from 'next/server';
import { scrapeAllData } from '@/lib/scraper';

export async function GET(request: Request) {
  // Support forced refresh via query param `?force=true`
  const { searchParams } = new URL(request.url);
  const forceRefresh = searchParams.get('force') === 'true';

  try {
    if (forceRefresh) {
      const fs = await import('fs/promises');
      const path = await import('path');
      const CACHE_FILE = path.join(process.cwd(), 'delta_per_cache.json');
      try {
        await fs.unlink(CACHE_FILE);
      } catch (e) {
        // Ignore if cache doesn't exist
      }
    }

    const result = await scrapeAllData();
    return NextResponse.json(result);
  } catch (error: any) {
    console.error("API Error:", error);
    return NextResponse.json({ error: 'Failed to scrape data', details: error.message }, { status: 500 });
  }
}
