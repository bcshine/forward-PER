import { NextResponse } from 'next/server';
import { fetchChartData, calculateSMASeries } from '@/lib/scraper';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const code = searchParams.get('code');

  if (!code || code.length !== 6) {
    return NextResponse.json({ error: 'Valid 6-digit stock code is required' }, { status: 400 });
  }

  try {
    const { dates, prices } = await fetchChartData(code, 300);
    if (!prices || prices.length === 0) {
      return NextResponse.json({ error: 'No chart data found for this code' }, { status: 404 });
    }

    const sma20 = calculateSMASeries(prices, 20);
    const sma120 = calculateSMASeries(prices, 120);

    return NextResponse.json({
      dates,
      prices,
      sma20,
      sma120,
    });
  } catch (error: any) {
    console.error(`API Error in Chart for ${code}:`, error);
    return NextResponse.json(
      { error: 'Failed to retrieve chart data', details: error.message },
      { status: 500 }
    );
  }
}
