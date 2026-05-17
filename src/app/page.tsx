'use client';

import { useState, useEffect, useMemo } from 'react';
import { RefreshCw, Search, Filter, ArrowUpDown, Info, Download, Loader2, Menu, X } from 'lucide-react';
import { FinancialData } from '@/lib/scraper';

export default function Home() {
  const [data, setData] = useState<FinancialData[]>([]);
  const [scrapeTime, setScrapeTime] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [isPcMode, setIsPcMode] = useState(false);

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

  // Priority Stocks Count (선행 PER < 10 & 현재 PER >= 30)
  const priorityCount = useMemo(() => {
    return data.filter(d => 
      d['선행 PER'] !== null && d['선행 PER'] < 10 &&
      d['현재 PER'] !== null && d['현재 PER'] >= 30
    ).length;
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
      if (!res.ok) throw new Error('데이터를 불러오는데 실패했습니다.');
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

    // 3.5. Priority stocks filter (선행 PER < 10 & 현재 PER >= 30)
    if (showPriorityOnly) {
      result = result.filter(d => 
        d['선행 PER'] !== null && d['선행 PER'] < 10 &&
        d['현재 PER'] !== null && d['현재 PER'] >= 30
      );
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
  }, [data, showMissing, selectedCategories, applyFilters, maxPer, minRoe, maxDebt, minMcap, search, primarySort, secondarySort, showPriorityOnly]);

  const handleDownload = () => {
    const headers = ['번호', '종목코드', '종목명', '산업카테고리', '시가총액(억)', 'DeltaPER', '현재 PER', '선행 PER', '추정 ROE', '부채비율', '이익성장률'];
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
              <Filter className="w-6 h-6 text-blue-600" /> Delta PER
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
              ※ 선행 PER 10 미만 & 현재 PER 30 이상
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
      <main className="flex-1 flex flex-col h-full overflow-hidden w-full">
        
        {/* Top bar */}
        <header className="h-16 border-b border-slate-200 dark:border-slate-800 bg-white/50 dark:bg-slate-950/50 backdrop-blur-md flex items-center justify-between px-2.5 md:px-8 z-10 shrink-0">
          <div className="flex items-center gap-1.5 md:gap-3">
            <button className="md:hidden p-2 -ml-1.5 text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg" onClick={() => setIsSidebarOpen(true)}>
              <Menu className="w-6 h-6" />
            </button>
            <div className="bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 px-2 py-0.5 md:px-2.5 md:py-1 rounded-full text-xs md:text-sm font-medium flex items-center gap-1 shrink-0">
              유효 종목 
              <span className="bg-blue-600 text-white px-1.5 py-0.5 rounded-full text-[10px] md:text-xs">{filteredData.length}</span>
            </div>

            <button
              onClick={() => setShowPriorityOnly(!showPriorityOnly)}
              className={`flex items-center gap-1 px-2.5 py-1 md:px-3.5 md:py-1.5 rounded-full text-xs md:text-sm font-semibold transition-all border ${
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
          <div className="bg-gradient-to-r from-indigo-50 to-blue-50 dark:from-indigo-950/40 dark:to-blue-950/40 border border-indigo-100 dark:border-indigo-900/50 p-3.5 rounded-xl flex items-start gap-2.5">
            <Info className="w-4 h-4 text-indigo-600 dark:text-indigo-400 shrink-0 mt-0.5" />
            <div className="text-xs md:text-sm text-indigo-900 dark:text-indigo-200">
              <strong className="font-semibold block mb-0.5">Delta PER 이란?</strong>
              현재 PER - 선행(추정) PER. 값이 클수록 투자 가치가 높음.
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
                      const isPriority = row['선행 PER'] !== null && row['선행 PER'] < 10 && 
                                         row['현재 PER'] !== null && row['현재 PER'] >= 30;
                      return (
                        <tr 
                          key={`${row.종목코드}-${idx}`} 
                          className={`transition-colors ${
                            isPriority 
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
                                  <span className="inline-flex items-center px-1 py-0.2 rounded text-[9px] font-bold bg-amber-100 dark:bg-amber-950/80 text-amber-800 dark:text-amber-300 whitespace-nowrap">
                                    ★ 우선고려
                                  </span>
                                )}
                              </span>
                              <span className="text-[10px] text-slate-400">{row.종목코드}</span>
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
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row['추정 ROE'] ?? '-'}</td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row['부채비율'] ?? '-'}</td>
                          <td className={`px-2 md:px-4 py-2.5 md:py-3 text-xs md:text-sm ${isPcMode ? '' : 'hidden md:table-cell'}`}>{row['시가총액(억)']?.toLocaleString() ?? '-'}</td>
                        </tr>
                      );
                    })}
                    {filteredData.length === 0 && !loading && (
                      <tr>
                        <td colSpan={9} className="px-4 py-12 text-center text-slate-500">
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
      </main>
    </div>
  );
}
