'use client';

import { useState, useEffect, useMemo } from 'react';
import { RefreshCw, Search, Filter, ArrowUpDown, Info, Download, Loader2, Menu, X, ExternalLink, TrendingUp } from 'lucide-react';
import { FinancialData } from '@/lib/scraper';
import { motion, AnimatePresence } from 'framer-motion';

interface ChartData {
  dates: string[];
  prices: number[];
  sma20: (number | null)[];
  sma120: (number | null)[];
}

// Custom Premium Interactive SVG Line Chart
function StockChart({ code, name }: { code: string; name: string }) {
  const [chartData, setChartData] = useState<ChartData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  useEffect(() => {
    let active = true;
    const fetchChart = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(`/api/chart?code=${code}`);
        if (!res.ok) throw new Error('차트 데이터를 불러오지 못했습니다.');
        const json = await res.json();
        if (active) {
          setChartData(json);
        }
      } catch (err: any) {
        if (active) {
          setError(err.message || '차트 오류');
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };
    fetchChart();
    return () => {
      active = false;
    };
  }, [code]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-64 bg-slate-50 dark:bg-slate-900/50 rounded-xl border border-slate-200 dark:border-slate-800">
        <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
        <p className="text-xs text-slate-500 mt-2">네이버 실시간 차트 불러오는 중...</p>
      </div>
    );
  }

  if (error || !chartData || chartData.prices.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 bg-slate-50 dark:bg-slate-900/50 rounded-xl border border-slate-200 dark:border-slate-850 text-red-500 text-xs p-4 text-center">
        <p className="font-semibold">차트 데이터를 표시할 수 없습니다.</p>
        <p className="text-slate-400 mt-1">네이버 증권 일봉 데이터 조회 실패</p>
      </div>
    );
  }

  const { dates, prices, sma20, sma120 } = chartData;
  const N = prices.length;

  const validPrices = prices.filter(p => p !== null && !isNaN(p));
  const validSma20 = sma20.filter((p): p is number => p !== null && !isNaN(p));
  const validSma120 = sma120.filter((p): p is number => p !== null && !isNaN(p));
  const allVals = [...validPrices, ...validSma20, ...validSma120];

  if (allVals.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 bg-slate-50 dark:bg-slate-900/50 rounded-xl border border-slate-200 dark:border-slate-800 text-slate-400 text-xs">
        <p>유효한 차트 가격 데이터가 없습니다.</p>
      </div>
    );
  }

  const rawMax = Math.max(...allVals);
  const rawMin = Math.min(...allVals);
  const pad = (rawMax - rawMin) * 0.05 || 1000;
  const maxVal = rawMax + pad;
  const minVal = Math.max(0, rawMin - pad);

  // SVG parameters
  const width = 600;
  const height = 300;
  const paddingLeft = 55;
  const paddingRight = 20;
  const paddingTop = 20;
  const paddingBottom = 40;

  const chartWidth = width - paddingLeft - paddingRight;
  const chartHeight = height - paddingTop - paddingBottom;

  const getX = (index: number) => {
    return paddingLeft + (index / (N - 1)) * chartWidth;
  };

  const getY = (val: number) => {
    return height - paddingBottom - ((val - minVal) / (maxVal - minVal)) * chartHeight;
  };

  const getLinePath = (series: (number | null)[]) => {
    let path = '';
    let first = true;
    for (let i = 0; i < series.length; i++) {
      const val = series[i];
      if (val !== null && !isNaN(val)) {
        const x = getX(i);
        const y = getY(val);
        if (first) {
          path += `M ${x} ${y}`;
          first = false;
        } else {
          path += ` L ${x} ${y}`;
        }
      }
    }
    return path;
  };

  const getAreaPath = () => {
    let path = '';
    let first = true;
    let lastX = paddingLeft;
    for (let i = 0; i < prices.length; i++) {
      const val = prices[i];
      if (val !== null && !isNaN(val)) {
        const x = getX(i);
        const y = getY(val);
        if (first) {
          path += `M ${x} ${y}`;
          first = false;
        } else {
          path += ` L ${x} ${y}`;
        }
        lastX = x;
      }
    }
    const zeroY = height - paddingBottom;
    path += ` L ${lastX} ${zeroY} L ${paddingLeft} ${zeroY} Z`;
    return path;
  };

  const pricePath = getLinePath(prices);
  const areaPath = getAreaPath();
  const sma20Path = getLinePath(sma20);
  const sma120Path = getLinePath(sma120);

  const gridLines = [];
  const gridCount = 5;
  for (let i = 0; i < gridCount; i++) {
    const val = minVal + (i / (gridCount - 1)) * (maxVal - minVal);
    gridLines.push({
      val,
      y: getY(val),
    });
  }

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement, MouseEvent>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const svgX = (mouseX / rect.width) * width;
    
    if (svgX >= paddingLeft && svgX <= width - paddingRight) {
      const fraction = (svgX - paddingLeft) / chartWidth;
      const idx = Math.min(N - 1, Math.max(0, Math.round(fraction * (N - 1))));
      setHoverIndex(idx);
    } else {
      setHoverIndex(null);
    }
  };

  const activeIndex = hoverIndex !== null ? hoverIndex : N - 1;
  const hoverPrice = prices[activeIndex];
  const hoverSma20 = sma20[activeIndex];
  const hoverSma120 = sma120[activeIndex];
  const hoverDate = dates[activeIndex];

  return (
    <div className="space-y-3">
      {/* Chart Hover Legend Panel */}
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 bg-slate-100 dark:bg-slate-900/60 p-3 rounded-xl border border-slate-200 dark:border-slate-800 text-xs">
        <div>
          <span className="font-semibold text-slate-400">일자:</span>{' '}
          <span className="font-mono font-bold text-slate-800 dark:text-slate-200">{hoverDate}</span>
        </div>
        <div className="flex flex-wrap gap-x-3 gap-y-1 font-mono">
          <div className="flex items-center gap-1">
            <span className="w-2.5 h-2.5 rounded-full bg-blue-500 inline-block"></span>
            <span className="text-slate-400">종가:</span>
            <span className="font-bold text-blue-600 dark:text-blue-400">{hoverPrice?.toLocaleString()}원</span>
          </div>
          {hoverSma20 && (
            <div className="flex items-center gap-1">
              <span className="w-2.5 h-2.5 rounded-full bg-orange-500 inline-block"></span>
              <span className="text-slate-400">20 SMA:</span>
              <span className="font-bold text-orange-600 dark:text-orange-400">{hoverSma20.toLocaleString()}원</span>
            </div>
          )}
          {hoverSma120 && (
            <div className="flex items-center gap-1">
              <span className="w-2.5 h-2.5 rounded-full bg-emerald-500 inline-block"></span>
              <span className="text-slate-400">120 SMA:</span>
              <span className="font-bold text-emerald-600 dark:text-emerald-400">{hoverSma120.toLocaleString()}원</span>
            </div>
          )}
        </div>
      </div>

      {/* SVG Container */}
      <div className="relative bg-white dark:bg-slate-950 p-2 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-inner">
        <svg 
          viewBox={`0 0 ${width} ${height}`} 
          className="w-full h-auto select-none overflow-visible"
          onMouseMove={handleMouseMove}
          onMouseLeave={() => setHoverIndex(null)}
        >
          <defs>
            <linearGradient id="priceAreaGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#3B82F6" stopOpacity="0.15" />
              <stop offset="100%" stopColor="#3B82F6" stopOpacity="0.0" />
            </linearGradient>
          </defs>

          {/* Grid lines */}
          {gridLines.map((line, i) => (
            <g key={i} className="opacity-40 dark:opacity-20">
              <line 
                x1={paddingLeft} 
                y1={line.y} 
                x2={width - paddingRight} 
                y2={line.y} 
                stroke="#64748B" 
                strokeDasharray="3,3" 
              />
              <text 
                x={paddingLeft - 8} 
                y={line.y + 3} 
                textAnchor="end" 
                className="fill-slate-500 text-[9px] font-mono"
              >
                {Math.round(line.val).toLocaleString()}
              </text>
            </g>
          ))}

          {/* Filled Area */}
          <path d={areaPath} fill="url(#priceAreaGrad)" />

          {/* Lines */}
          <path 
            d={pricePath} 
            fill="none" 
            stroke="#3B82F6" 
            strokeWidth="2" 
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {sma20Path && (
            <path 
              d={sma20Path} 
              fill="none" 
              stroke="#F97316" 
              strokeWidth="1.5" 
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          )}

          {sma120Path && (
            <path 
              d={sma120Path} 
              fill="none" 
              stroke="#10B981" 
              strokeWidth="1.5" 
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          )}

          {/* X Axis Date labels */}
          <g className="fill-slate-400 dark:fill-slate-500 text-[9px] font-mono">
            <text x={paddingLeft} y={height - 10} textAnchor="start">
              {dates[0]}
            </text>
            <text x={paddingLeft + chartWidth / 2} y={height - 10} textAnchor="middle">
              {dates[Math.floor(N / 2)]}
            </text>
            <text x={width - paddingRight} y={height - 10} textAnchor="end">
              {dates[N - 1]}
            </text>
          </g>

          {/* Interactive Hover Crosshair */}
          {activeIndex !== null && (
            <g>
              <line 
                x1={getX(activeIndex)} 
                y1={paddingTop} 
                x2={getX(activeIndex)} 
                y2={height - paddingBottom} 
                stroke="#6366F1" 
                strokeWidth="1" 
                strokeDasharray="3,3" 
                className="opacity-70"
              />

              {hoverPrice !== null && (
                <circle 
                  cx={getX(activeIndex)} 
                  cy={getY(hoverPrice)} 
                  r="5" 
                  fill="#3B82F6" 
                  stroke="#FFFFFF" 
                  strokeWidth="1.5"
                />
              )}

              {hoverSma20 !== null && (
                <circle 
                  cx={getX(activeIndex)} 
                  cy={getY(hoverSma20)} 
                  r="4" 
                  fill="#F97316" 
                  stroke="#FFFFFF" 
                  strokeWidth="1.5"
                />
              )}

              {hoverSma120 !== null && (
                <circle 
                  cx={getX(activeIndex)} 
                  cy={getY(hoverSma120)} 
                  r="4" 
                  fill="#10B981" 
                  stroke="#FFFFFF" 
                  strokeWidth="1.5"
                />
              )}
            </g>
          )}
        </svg>
      </div>
    </div>
  );
}

export default function Home() {
  const [data, setData] = useState<FinancialData[]>([]);
  const [scrapeTime, setScrapeTime] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [isPcMode, setIsPcMode] = useState(false);

  // Selected stock for sidebar drawer
  const [selectedStock, setSelectedStock] = useState<FinancialData | null>(null);

  // Filters
  const [search, setSearch] = useState('');
  const [categories, setCategories] = useState<string[]>([]);
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [applyFilters, setApplyFilters] = useState(true);
  const [maxPer, setMaxPer] = useState<number>(9999);
  const [minRoe, setMinRoe] = useState<number>(-9999);
  const [maxDebt, setMaxDebt] = useState<number>(9999);
  const [minMcap, setMinMcap] = useState<number>(0);
  const [showMissing, setShowMissing] = useState(false);
  const [showPriorityOnly, setShowPriorityOnly] = useState(false);
  const [showGoldenCrossOnly, setShowGoldenCrossOnly] = useState(false);

  // Priority Stocks Count (선행 PER <= 10 & 현재 PER >= 20)
  const priorityCount = useMemo(() => {
    return data.filter(d => 
      d['선행 PER'] !== null && d['선행 PER'] <= 10 &&
      d['현재 PER'] !== null && d['현재 PER'] >= 20
    ).length;
  }, [data]);

  // Golden Cross Count (골든크로스 === true)
  const goldenCrossCount = useMemo(() => {
    return data.filter(d => d['골든크로스'] === true).length;
  }, [data]);

  // Sorting
  type SortConfig = { key: keyof FinancialData | 'none'; asc: boolean };
  const [primarySort, setPrimarySort] = useState<SortConfig>({ key: 'DeltaPER', asc: false });
  const [secondarySort, setSecondarySort] = useState<SortConfig>({ key: 'none', asc: false });

  const fetchData = async (force = false) => {
    if (force) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/data${force ? '?force=true' : ''}`);
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.details || errorData.error || '데이터를 불러오는데 실패했습니다.');
      }
      const result = await res.json();
      if (result.data) {
        setData(result.data);
        setScrapeTime(result.scrapeTime);
        
        // Extract unique categories
        const cats = Array.from(new Set(result.data.map((d: any) => d.산업카테고리).filter(Boolean))) as string[];
        cats.sort();
        setCategories(cats);
      }
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleToggleCategory = (cat: string) => {
    setSelectedCategories(prev => 
      prev.includes(cat) ? prev.filter(c => c !== cat) : [...prev, cat]
    );
  };

  // Processing Data
  const filteredData = useMemo(() => {
    let result = [...data];

    // 1. Missing data filter
    if (!showMissing) {
      result = result.filter(d => 
        d['선행 PER'] !== null && d['추정 ROE'] !== null && 
        d['부채비율'] !== null && d['시가총액(억)'] !== null
      );
    }

    // 2. Category filter
    if (selectedCategories.length > 0) {
      result = result.filter(d => d.산업카테고리 && selectedCategories.includes(d.산업카테고리));
    }

    // 3. Metric filters
    if (applyFilters) {
      result = result.filter(d => {
        const perCond = d['선행 PER'] === null || d['선행 PER']! <= maxPer;
        const roeCond = d['추정 ROE'] === null || d['추정 ROE']! >= minRoe;
        const debtCond = d['부채비율'] === null || d['부채비율']! <= maxDebt;
        const mcapCond = d['시가총액(억)'] === null || d['시가총액(억)']! >= minMcap;
        return perCond && roeCond && debtCond && mcapCond;
      });
    }

    // 3.5. Priority stocks filter (선행 PER <= 10 & 현재 PER >= 20)
    if (showPriorityOnly) {
      result = result.filter(d => 
        d['선행 PER'] !== null && d['선행 PER'] <= 10 &&
        d['현재 PER'] !== null && d['현재 PER'] >= 20
      );
    }

    // 3.6. Golden Cross filter
    if (showGoldenCrossOnly) {
      result = result.filter(d => d['골든크로스'] === true);
    }

    // 4. Search
    if (search.trim() !== '') {
      const q = search.toLowerCase();
      result = result.filter(d => 
        d.종목명.toLowerCase().includes(q) || d.종목코드.includes(q)
      );
    }

    // 5. Sorting
    result.sort((a: any, b: any) => {
      const pKey = primarySort.key;
      const sKey = secondarySort.key;

      if (pKey !== 'none') {
        const aVal = a[pKey] ?? -Infinity;
        const bVal = b[pKey] ?? -Infinity;
        if (aVal !== bVal) {
          return primarySort.asc ? (aVal > bVal ? 1 : -1) : (aVal < bVal ? 1 : -1);
        }
      }

      if (sKey !== 'none') {
        const aVal = a[sKey] ?? -Infinity;
        const bVal = b[sKey] ?? -Infinity;
        if (aVal !== bVal) {
          return secondarySort.asc ? (aVal > bVal ? 1 : -1) : (aVal < bVal ? 1 : -1);
        }
      }
      return 0;
    });

    return result;
  }, [data, showMissing, selectedCategories, applyFilters, maxPer, minRoe, maxDebt, minMcap, search, primarySort, secondarySort, showPriorityOnly, showGoldenCrossOnly]);

  const handleDownload = () => {
    const headers = ['번호', '종목코드', '종목명', '산업카테고리', '시가총액(억)', 'DeltaPER', '현재 PER', '선행 PER', '추정 ROE', '부채비율', '이익성장률', '골든크로스'];
    const csvContent = [
      headers.join(','),
      ...filteredData.map((row: any) => 
        headers.map(field => `"${row[field] ?? ''}"`).join(',')
      )
    ].join('\n');

    const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `delta_per_data.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const renderSortIcon = (key: string) => {
    if (primarySort.key === key) {
      return <span className="ml-1 text-blue-500">{primarySort.asc ? '▲' : '▼'}</span>;
    }
    return <ArrowUpDown className="ml-1 w-3 h-3 text-slate-400 group-hover:text-slate-600" />;
  };

  const handleSort = (key: keyof FinancialData) => {
    if (primarySort.key === key) {
      setPrimarySort({ key, asc: !primarySort.asc });
    } else {
      setPrimarySort({ key, asc: false });
    }
  };

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50 dark:bg-slate-900 text-slate-900 dark:text-slate-100">
      
      {/* Mobile Sidebar Overlay */}
      {isSidebarOpen && (
        <div className="fixed inset-0 bg-black/50 z-20 md:hidden" onClick={() => setIsSidebarOpen(false)} />
      )}

      {/* Sidebar */}
      <aside className={`fixed inset-y-0 left-0 transform ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full'} md:relative md:translate-x-0 w-80 h-full border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 flex flex-col shadow-xl md:shadow-sm z-30 transition-transform duration-300 ease-in-out`}>
        <div className="p-6 border-b border-slate-200 dark:border-slate-800 flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent flex items-center gap-2">
              <Filter className="w-6 h-6 text-blue-600" /> Delta-PER (KR)
            </h1>
            <p className="text-xs text-slate-500 mt-2">최근 업데이트: {scrapeTime || '-'}</p>
          </div>
          <button className="md:hidden p-2 -mr-2 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg" onClick={() => setIsSidebarOpen(false)}>
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Search */}
          <div className="space-y-2">
            <label className="text-sm font-semibold flex items-center gap-2"><Search className="w-4 h-4"/> 종목 검색</label>
            <input 
              type="text" 
              placeholder="종목명 또는 코드" 
              className="w-full px-3 py-2 bg-slate-100 dark:bg-slate-900 border-none rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>

          <div className="h-px bg-slate-200 dark:bg-slate-800" />

          {/* Settings */}
          <div className="space-y-4">
            <button
              onClick={() => setShowPriorityOnly(!showPriorityOnly)}
              className={`w-full flex items-center justify-between px-4 py-3 rounded-xl border text-sm font-semibold transition-all ${
                showPriorityOnly 
                  ? 'bg-amber-500 border-amber-600 text-white shadow-sm shadow-amber-500/20' 
                  : 'bg-amber-50/50 dark:bg-amber-950/10 border-amber-200 dark:border-amber-900/30 text-amber-700 dark:text-amber-400 hover:bg-amber-100/50 dark:hover:bg-amber-950/20'
              }`}
            >
              <span className="flex items-center gap-2">
                <span className="text-base">★</span> 우선고려종목만 보기
              </span>
              <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${showPriorityOnly ? 'bg-white text-amber-600' : 'bg-amber-100 dark:bg-amber-900/60 text-amber-800 dark:text-amber-300'}`}>
                {priorityCount}개
              </span>
            </button>
            <p className="text-[11px] text-amber-600 dark:text-amber-400/80 -mt-2 px-1 leading-normal">
              ※ 선행 PER 10 이하 & 현재 PER 20 이상
            </p>

            <button
              onClick={() => setShowGoldenCrossOnly(!showGoldenCrossOnly)}
              className={`w-full flex items-center justify-between px-4 py-3 rounded-xl border text-sm font-semibold transition-all ${
                showGoldenCrossOnly 
                  ? 'bg-emerald-600 border-emerald-700 text-white shadow-sm shadow-emerald-500/20' 
                  : 'bg-emerald-50/50 dark:bg-emerald-950/10 border-emerald-200 dark:border-emerald-900/30 text-emerald-700 dark:text-emerald-400 hover:bg-emerald-100/50 dark:hover:bg-emerald-950/20'
              }`}
            >
              <span className="flex items-center gap-2">
                <span className="text-base">📈</span> 골든크로스 종목만 보기
              </span>
              <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${showGoldenCrossOnly ? 'bg-white text-emerald-600' : 'bg-emerald-100 dark:bg-emerald-900/60 text-emerald-800 dark:text-emerald-300'}`}>
                {goldenCrossCount}개
              </span>
            </button>
            <p className="text-[11px] text-emerald-600 dark:text-emerald-400/80 -mt-2 px-1 leading-normal">
              ※ 최근 10영업일 이내 20일선이 120일선을 상향 돌파한 종목
            </p>

            <label className="flex items-center gap-2 cursor-pointer text-sm font-medium pt-2">
              <input type="checkbox" checked={applyFilters} onChange={(e) => setApplyFilters(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500 bg-slate-100 border-slate-300"/>
              필터 적용 (AND 조건)
            </label>
            <label className="flex items-center gap-2 cursor-pointer text-sm font-medium">
              <input type="checkbox" checked={showMissing} onChange={(e) => setShowMissing(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500 bg-slate-100 border-slate-300"/>
              결측치 포함(500개 보기)
            </label>
          </div>

          {/* Metric Filters */}
          <div className="space-y-4 opacity-100 transition-opacity" style={{ opacity: applyFilters ? 1 : 0.5, pointerEvents: applyFilters ? 'auto' : 'none' }}>
            <div className="space-y-1">
              <label className="text-xs text-slate-500">Max 선행 PER</label>
              <input type="number" value={maxPer} onChange={e => setMaxPer(Number(e.target.value))} className="w-full px-3 py-2 bg-slate-100 dark:bg-slate-900 rounded-lg text-sm" />
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-500">Min 추정 ROE (%)</label>
              <input type="number" value={minRoe} onChange={e => setMinRoe(Number(e.target.value))} className="w-full px-3 py-2 bg-slate-100 dark:bg-slate-900 rounded-lg text-sm" />
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-500">Max 부채비율 (%)</label>
              <input type="number" value={maxDebt} onChange={e => setMaxDebt(Number(e.target.value))} className="w-full px-3 py-2 bg-slate-100 dark:bg-slate-900 rounded-lg text-sm" />
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-500">Min 시가총액 (억원)</label>
              <input type="number" value={minMcap} onChange={e => setMinMcap(Number(e.target.value))} className="w-full px-3 py-2 bg-slate-100 dark:bg-slate-900 rounded-lg text-sm" />
            </div>
          </div>

          <div className="h-px bg-slate-200 dark:bg-slate-800" />

          {/* Category Filter */}
          <div className="space-y-2">
            <label className="text-sm font-semibold">산업 카테고리</label>
            <div className="max-h-48 overflow-y-auto space-y-1 p-2 bg-slate-50 dark:bg-slate-900 rounded-lg border border-slate-200 dark:border-slate-800">
              {categories.map(cat => (
                <label key={cat} className="flex items-center gap-2 cursor-pointer text-xs p-1 hover:bg-slate-100 dark:hover:bg-slate-800 rounded">
                  <input type="checkbox" checked={selectedCategories.includes(cat)} onChange={() => handleToggleCategory(cat)} className="rounded text-blue-600"/>
                  {cat}
                </label>
              ))}
              {categories.length === 0 && <p className="text-xs text-slate-400 p-2">로딩 중...</p>}
            </div>
          </div>
        </div>
        
        <div className="p-6 border-t border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900">
          <button 
            onClick={() => fetchData(true)}
            disabled={refreshing}
            className="w-full flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-700 text-white py-3 px-4 rounded-xl font-medium transition-colors disabled:opacity-70 disabled:cursor-not-allowed shadow-sm shadow-blue-500/20"
          >
            <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
            {refreshing ? '크롤링 진행 중...' : '새로 크롤링'}
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col h-full overflow-hidden w-full relative">
        
        {/* Top bar */}
        <header className="h-16 border-b border-slate-200 dark:border-slate-800 bg-white/50 dark:bg-slate-955/50 backdrop-blur-md flex items-center justify-between px-2.5 md:px-8 z-10 shrink-0">
          <div className="flex items-center gap-1.5 md:gap-3 overflow-x-auto no-scrollbar scroll-smooth">
            <button className="md:hidden p-2 -ml-1.5 text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg" onClick={() => setIsSidebarOpen(true)}>
              <Menu className="w-6 h-6" />
            </button>
            <div className="bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 px-2 py-0.5 md:px-2.5 md:py-1 rounded-full text-xs md:text-sm font-medium flex items-center gap-1 shrink-0">
              유효 종목 
              <span className="bg-blue-600 text-white px-1.5 py-0.5 rounded-full text-[10px] md:text-xs">{filteredData.length}</span>
            </div>

            <button
              onClick={() => setShowPriorityOnly(!showPriorityOnly)}
              className={`flex items-center gap-1 px-2.5 py-1 md:px-3.5 md:py-1.5 rounded-full text-xs md:text-sm font-semibold transition-all border shrink-0 ${
                showPriorityOnly 
                  ? 'bg-amber-500 border-amber-600 text-white shadow-sm shadow-amber-500/20 ring-2 ring-amber-300 dark:ring-amber-800' 
                  : 'bg-amber-50/50 hover:bg-amber-100/50 dark:bg-amber-950/15 dark:hover:bg-amber-950/30 border-amber-200 dark:border-amber-900/30 text-amber-700 dark:text-amber-400'
              }`}
            >
              <span className="text-amber-500">★</span> 우선고려
              <span className={`ml-0.5 px-1.5 py-0.1 rounded-full text-[10px] md:text-xs font-bold ${showPriorityOnly ? 'bg-white text-amber-600' : 'bg-amber-100/80 dark:bg-amber-900/60 text-amber-800 dark:text-amber-300'}`}>
                {priorityCount}
              </span>
            </button>

            <button
              onClick={() => setShowGoldenCrossOnly(!showGoldenCrossOnly)}
              className={`flex items-center gap-1 px-2.5 py-1 md:px-3.5 md:py-1.5 rounded-full text-xs md:text-sm font-semibold transition-all border shrink-0 ${
                showGoldenCrossOnly 
                  ? 'bg-emerald-600 border-emerald-700 text-white shadow-sm shadow-emerald-500/20 ring-2 ring-emerald-300 dark:ring-emerald-800' 
                  : 'bg-emerald-50/50 hover:bg-emerald-100/50 dark:bg-emerald-950/15 dark:hover:bg-emerald-950/30 border-emerald-200 dark:border-emerald-900/30 text-emerald-700 dark:text-emerald-400'
              }`}
            >
              <span className="text-emerald-500">📈</span> 골든크로스
              <span className={`ml-0.5 px-1.5 py-0.1 rounded-full text-[10px] md:text-xs font-bold ${showGoldenCrossOnly ? 'bg-white text-emerald-600' : 'bg-emerald-100/80 dark:bg-emerald-900/60 text-emerald-800 dark:text-emerald-300'}`}>
                {goldenCrossCount}
              </span>
            </button>
            
            <button 
              onClick={() => setIsPcMode(!isPcMode)}
              className="md:hidden flex items-center gap-0.5 text-[11px] font-semibold px-2 py-1 rounded-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-700 dark:text-slate-300 transition-colors shadow-sm shrink-0"
            >
              {isPcMode ? '모바일 최적화' : 'PC 화면용'}
            </button>
          </div>

          <button onClick={handleDownload} className="flex items-center gap-1.5 text-xs md:text-sm font-medium text-slate-600 dark:text-slate-300 hover:text-blue-600 dark:hover:text-blue-400 transition-colors bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 px-2.5 py-1.5 md:px-4 md:py-2 rounded-lg shadow-sm whitespace-nowrap">
            <Download className="w-3.5 h-3.5" /> <span className="hidden sm:inline">엑셀 다운로드</span>
          </button>
        </header>

        {/* Info Banner */}
        <div className="px-2.5 md:px-8 py-3 shrink-0">
          <div className="bg-gradient-to-r from-indigo-50 to-blue-50 dark:from-indigo-950/40 dark:to-blue-950/40 border border-indigo-100 dark:border-indigo-900/50 p-3.5 rounded-xl flex items-start gap-2.5 shadow-sm">
            <Info className="w-4 h-4 text-indigo-600 dark:text-indigo-400 shrink-0 mt-0.5" />
            <div className="text-xs md:text-sm text-indigo-900 dark:text-indigo-200">
              <strong className="font-semibold block mb-0.5">Delta-PER (KR) 이란?</strong>
              현재 PER - 선행(추정) PER. 값이 클수록 미래 수익 개선이 기대되어 투자 가치가 높음. 각 행을 클릭해 실시간 네이버 일봉 차트와 20/120선 골든크로스 현황을 조회해 보세요.
            </div>
          </div>
        </div>

        {/* Table Area */}
        <div className="flex-1 px-2.5 md:px-8 pb-3 md:pb-8 flex flex-col min-h-0">
          <div className="bg-white dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-sm flex flex-col flex-1 overflow-hidden relative">
            {loading ? (
              <div className="flex flex-col items-center justify-center h-full text-slate-500 gap-4 min-h-[300px]">
                <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
                <p>데이터를 불러오는 중입니다...</p>
              </div>
            ) : error ? (
              <div className="flex flex-col items-center justify-center h-full text-red-500 gap-2 min-h-[300px]">
                <p className="font-semibold">오류 발생</p>
                <p className="text-sm opacity-80">{error}</p>
              </div>
            ) : (
              <div className="overflow-auto flex-1">
                <table className="w-full text-sm text-left relative">
                  <thead className="text-xs text-slate-600 dark:text-slate-400 bg-slate-100 dark:bg-slate-900 sticky top-0 z-20 shadow-sm border-b border-slate-200 dark:border-slate-800">
                    <tr>
                      <th className={`px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap ${isPcMode ? '' : 'hidden md:table-cell'}`} onClick={() => handleSort('번호')}>
                        <div className="flex items-center">No {renderSortIcon('번호')}</div>
                      </th>
                      <th className="px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap" onClick={() => handleSort('종목명')}>
                        <div className="flex items-center">종목명 {renderSortIcon('종목명')}</div>
                      </th>
                      <th className={`px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap ${isPcMode ? '' : 'hidden md:table-cell'}`} onClick={() => handleSort('산업카테고리')}>
                        <div className="flex items-center">산업군 {renderSortIcon('산업카테고리')}</div>
                      </th>
                      <th className="px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap" onClick={() => handleSort('DeltaPER')}>
                        <div className="flex items-center text-blue-600 dark:text-blue-400 font-bold">Delta {renderSortIcon('DeltaPER')}</div>
                      </th>
                      <th className="px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap" onClick={() => handleSort('현재 PER')}>
                        <div className="flex items-center">현재 {renderSortIcon('현재 PER')}</div>
                      </th>
                      <th className="px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap" onClick={() => handleSort('선행 PER')}>
                        <div className="flex items-center">선행 {renderSortIcon('선행 PER')}</div>
                      </th>
                      <th className="px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap" onClick={() => handleSort('골든크로스')}>
                        <div className="flex items-center">골든크로스 {renderSortIcon('골든크로스')}</div>
                      </th>
                      <th className={`px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap ${isPcMode ? '' : 'hidden md:table-cell'}`} onClick={() => handleSort('추정 ROE')}>
                        <div className="flex items-center">추정 ROE {renderSortIcon('추정 ROE')}</div>
                      </th>
                      <th className={`px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap ${isPcMode ? '' : 'hidden md:table-cell'}`} onClick={() => handleSort('부채비율')}>
                        <div className="flex items-center">부채비율 {renderSortIcon('부채비율')}</div>
                      </th>
                      <th className={`px-2 md:px-4 py-2.5 md:py-3 font-medium cursor-pointer group whitespace-nowrap ${isPcMode ? '' : 'hidden md:table-cell'}`} onClick={() => handleSort('시가총액(억)')}>
                        <div className="flex items-center">시총(억) {renderSortIcon('시가총액(억)')}</div>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                    {filteredData.map((row, idx) => {
                      const isPriority = row['선행 PER'] !== null && row['선행 PER'] <= 10 && 
                                         row['현재 PER'] !== null && row['현재 PER'] >= 20;
                      return (
                        <tr 
                          key={`${row.종목코드}-${idx}`} 
                          onClick={() => setSelectedStock(row)}
                          className={`transition-colors cursor-pointer ${
                            selectedStock?.종목코드 === row.종목코드
                              ? 'bg-blue-50/50 dark:bg-blue-900/10 hover:bg-blue-100/50 dark:hover:bg-blue-900/20 border-l-4 border-l-blue-600'
                              : isPriority 
                                ? 'bg-amber-500/5 dark:bg-amber-500/10 hover:bg-amber-500/10 dark:hover:bg-amber-500/15 border-l-4 border-l-amber-500' 
                                : 'hover:bg-slate-50/80 dark:hover:bg-slate-900/50 border-l-4 border-l-transparent'
                          }`}
                        >
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-slate-500 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{idx + 1}</td>
                          <td className="px-2 md:px-4 py-2.5 md:py-3 max-w-[150px] md:max-w-none min-w-0">
                            <div className="flex flex-col min-w-0">
                              <span className="font-semibold text-slate-900 dark:text-slate-100 text-xs md:text-sm flex flex-wrap items-center gap-1">
                                {row.종목명}
                                {isPriority && (
                                  <span className="inline-flex items-center px-1.5 py-0.2 rounded text-[9px] font-bold bg-amber-100 dark:bg-amber-955/80 text-amber-800 dark:text-amber-300 whitespace-nowrap">
                                    ★ 우선고려
                                  </span>
                                )}
                              </span>
                              <span className="text-[10px] text-slate-400 font-mono">{row.종목코드}</span>
                            </div>
                          </td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-slate-600 dark:text-slate-400 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row.산업카테고리 || '-'}</td>
                          <td className="px-2 md:px-4 py-2.5 md:py-3 font-bold text-xs md:text-sm">
                            <span className={row.DeltaPER && row.DeltaPER > 0 ? 'text-blue-600 dark:text-blue-400' : row.DeltaPER && row.DeltaPER < 0 ? 'text-red-500 dark:text-red-400' : ''}>
                              {row.DeltaPER ?? '-'}
                            </span>
                          </td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm font-medium ${isPriority ? 'text-amber-600 dark:text-amber-400' : ''}`}>{row['현재 PER'] ?? '-'}</td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 font-semibold text-xs md:text-sm ${isPriority ? 'text-amber-500' : 'text-indigo-600 dark:text-indigo-400'}`}>{row['선행 PER'] ?? '-'}</td>
                          
                          {/* Golden Cross Badge Cell */}
                          <td className="px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm font-medium">
                            {row['골든크로스'] === true ? (
                              <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/10 dark:bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20 whitespace-nowrap">
                                📈 골든크로스
                              </span>
                            ) : row['골든크로스'] === false ? (
                              <span className="text-slate-400 dark:text-slate-600 text-xs font-medium">데드크로스</span>
                            ) : (
                              <span className="text-slate-300 dark:text-slate-700 text-xs">-</span>
                            )}
                          </td>

                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row['추정 ROE'] ?? '-'}</td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row['부채비율'] ?? '-'}</td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm font-mono ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row['시가총액(억)']?.toLocaleString() ?? '-'}</td>
                        </tr>
                      );
                    })}
                    {filteredData.length === 0 && !loading && (
                      <tr>
                        <td colSpan={10} className="px-4 py-12 text-center text-slate-500">
                          조건에 맞는 종목이 없습니다.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        {/* Premium Slide-over Drawer for Stock Details & XML Chart */}
        <AnimatePresence>
          {selectedStock && (
            <>
              {/* Dark Overlay backdrop */}
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 0.5 }}
                exit={{ opacity: 0 }}
                onClick={() => setSelectedStock(null)}
                className="fixed inset-0 bg-black z-40"
              />

              {/* Drawer panel */}
              <motion.div
                initial={{ x: '100%' }}
                animate={{ x: 0 }}
                exit={{ x: '100%' }}
                transition={{ type: 'spring', damping: 26, stiffness: 220 }}
                className="fixed inset-y-0 right-0 w-full max-w-xl md:max-w-2xl bg-white dark:bg-slate-950 shadow-2xl border-l border-slate-200 dark:border-slate-800 z-50 flex flex-col h-full overflow-hidden"
              >
                {/* Drawer Header */}
                <div className="p-6 border-b border-slate-200 dark:border-slate-800 flex justify-between items-center bg-slate-50/50 dark:bg-slate-900/20 backdrop-blur">
                  <div className="min-w-0">
                    <h2 className="text-xl md:text-2xl font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
                      {selectedStock.종목명}
                      {selectedStock['골든크로스'] === true && (
                        <span className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/10 text-emerald-600 dark:text-emerald-450 border border-emerald-500/20">
                          📈 골든크로스
                        </span>
                      )}
                    </h2>
                    <p className="text-xs text-slate-500 font-mono mt-1">
                      종목코드: {selectedStock.종목코드} | {selectedStock.산업카테고리 || '미분류'}
                    </p>
                  </div>
                  <button 
                    onClick={() => setSelectedStock(null)}
                    className="p-2 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-900 rounded-lg transition-colors"
                  >
                    <X className="w-5 h-5" />
                  </button>
                </div>

                {/* Drawer Body Scroll */}
                <div className="flex-1 overflow-y-auto p-6 space-y-6">
                  {/* SVG Chart Module */}
                  <div>
                    <h3 className="text-sm font-bold text-slate-450 dark:text-slate-400 mb-3 flex items-center gap-1.5">
                      <TrendingUp className="w-4 h-4 text-blue-500" />
                      300일 실시간 시세 & SMA 트렌드 (네이버 증권 API)
                    </h3>
                    <StockChart code={selectedStock.종목코드} name={selectedStock.종목명} />
                  </div>

                  <div className="h-px bg-slate-200 dark:bg-slate-800" />

                  {/* Financial Metrics Cards Grid */}
                  <div>
                    <h3 className="text-sm font-bold text-slate-450 dark:text-slate-400 mb-3">
                      주요 퀀트 & 재무 지표
                    </h3>
                    <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                      {/* Delta PER */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">Delta-PER (현재-선행)</span>
                        <span className={`text-base font-bold font-mono block mt-1 ${selectedStock.DeltaPER && selectedStock.DeltaPER > 0 ? 'text-blue-600 dark:text-blue-400' : 'text-slate-800 dark:text-slate-200'}`}>
                          {selectedStock.DeltaPER ?? '-'}
                        </span>
                      </div>
                      {/* 현재 PER */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">현재 PER (Trailing)</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock['현재 PER'] ?? '-'}
                        </span>
                      </div>
                      {/* 선행 PER */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">선행 PER (Forward)</span>
                        <span className="text-base font-bold font-mono text-indigo-600 dark:text-indigo-400 block mt-1">
                          {selectedStock['선행 PER'] ?? '-'}
                        </span>
                      </div>
                      {/* 추정 ROE */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">추정 ROE</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock['추정 ROE'] !== null ? `${selectedStock['추정 ROE']}%` : '-'}
                        </span>
                      </div>
                      {/* 부채비율 */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">부채비율</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock['부채비율'] !== null ? `${selectedStock['부채비율']}%` : '-'}
                        </span>
                      </div>
                      {/* 이익성장률 */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">영업이익 성장률 (YoY)</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock.이익성장률 !== null ? `${(selectedStock.이익성장률 * 100).toFixed(2)}%` : '-'}
                        </span>
                      </div>
                      {/* 전년 영업이익 */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">전년 영업이익</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock['전년 영업이익'] !== null ? `${selectedStock['전년 영업이익'].toLocaleString()}억` : '-'}
                        </span>
                      </div>
                      {/* 추정 영업이익 */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl">
                        <span className="text-[10px] text-slate-400 block">추정 영업이익</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock['추정 영업이익'] !== null ? `${selectedStock['추정 영업이익'].toLocaleString()}억` : '-'}
                        </span>
                      </div>
                      {/* 시가총액 */}
                      <div className="bg-slate-50 dark:bg-slate-900/40 border border-slate-200 dark:border-slate-800/80 p-3 rounded-xl col-span-2 sm:col-span-1">
                        <span className="text-[10px] text-slate-400 block">시가총액</span>
                        <span className="text-base font-bold font-mono text-slate-800 dark:text-slate-200 block mt-1">
                          {selectedStock['시가총액(억)'] !== null ? `${selectedStock['시가총액(억)'].toLocaleString()}억` : '-'}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Drawer Footer Actions */}
                <div className="p-6 border-t border-slate-200 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/20 flex gap-3">
                  <a 
                    href={`https://m.stock.naver.com/domestic/stock/${selectedStock.종목코드}/total`} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-2 bg-emerald-600 hover:bg-emerald-700 text-white py-3 px-4 rounded-xl font-bold transition-colors shadow-sm text-sm"
                  >
                    <ExternalLink className="w-4 h-4" /> 모바일 네이버 증권 바로가기
                  </a>
                  <button 
                    onClick={() => setSelectedStock(null)}
                    className="flex-1 bg-slate-100 hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800 text-slate-700 dark:text-slate-300 py-3 px-4 rounded-xl font-bold transition-colors text-sm"
                  >
                    닫기
                  </button>
                </div>
              </motion.div>
            </>
          )}
        </AnimatePresence>
      </main>
    </div>
  );
}
