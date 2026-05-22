import { NextResponse } from 'next/server';
import { scrapeAllData } from '@/lib/scraper';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const forceRefresh = searchParams.get('force') === 'true';

  try {
    const result = await scrapeAllData(forceRefresh);
    return NextResponse.json(result);
  } catch (error: any) {
    console.error("API Error in Korea Delta-PER:", error);
    return NextResponse.json(
      { error: 'Failed to retrieve Korea stock data', details: error.message },
      { status: 500 }
    );
  }
}
