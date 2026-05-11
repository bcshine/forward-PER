import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from typing import List, Dict, Any, Tuple

# --- Configuration & Constants ---
st.set_page_config(layout="wide", page_title="Naver Finance Screener")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}
CACHE_FILE = "delta_per_cache.csv"
CACHE_TIME_FILE = "delta_per_cache_time.txt"

# --- Scraping Logic ---

def fetch_ticker_page(sosok: int, page: int) -> List[Dict[str, Any]]:
    """Fetch a single page of market cap ranking and return tickers."""
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
    tickers = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find('table', {'class': 'type_2'})
        if not table: return []
        
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) <= 1: continue
            
            a_tag = row.find('a', href=True)
            if a_tag and '/item/main.naver?code=' in a_tag['href']:
                code = a_tag['href'].split('code=')[-1].strip()
                name = a_tag.text.strip()
                try:
                    mcap = int(cols[6].text.replace(',', '').strip())
                    tickers.append({'Code': code, 'Name': name, 'Mcap': mcap})
                except (ValueError, IndexError):
                    continue
    except Exception:
        pass
    return tickers

@st.cache_data(ttl=3600*12, show_spinner=False)
def get_top_500_tickers() -> List[Dict[str, Any]]:
    """Retrieve top 500 stocks using parallel requests."""
    all_tickers = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = [executor.submit(fetch_ticker_page, sosok, page) for sosok in [0, 1] for page in range(1, 6)]
        for future in as_completed(tasks):
            all_tickers.extend(future.result())
            
    # Sort by Market Cap and take Top 500
    all_tickers = sorted([t for t in all_tickers if t['Mcap'] is not None], key=lambda x: x['Mcap'], reverse=True)
    return all_tickers[:500]

def get_financial_data(ticker_info: Dict[str, str]) -> Dict[str, Any]:
    """Scrape financial metrics for a single ticker."""
    code, name = ticker_info['Code'], ticker_info['Name']
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    
    data = {
        '번호': 0, '종목코드': code, '종목명': name, '산업카테고리': None,
        '시가총액(억)': None, '현재 PER': None, '추정 PER': None,
        '전년 영업이익': None, '추정 영업이익': None, '추정 ROE': None,
        '부채비율': None, '이익성장률': None, 'DeltaPER': None
    }
    
    try:
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'lxml')
        
        # 1. Category & Mcap
        category_tag = soup.select_one('div.section h4.h_sub.sub_tit7 a')
        if category_tag: data['산업카테고리'] = category_tag.text.strip()
        
        mcap_tag = soup.find('em', id='_market_sum')
        if mcap_tag:
            mcap_val = mcap_tag.text.replace(',', '').replace('조', '').replace(' ', '').replace('\t', '').replace('\n', '')
            try: data['시가총액(억)'] = float(mcap_val)
            except ValueError: pass

        # 2. PER Values
        def safe_float(selector_id):
            tag = soup.find('em', id=selector_id)
            try: return float(tag.text.replace(',', '')) if tag else None
            except ValueError: return None

        data['현재 PER'] = safe_float('_per')
        data['추정 PER'] = safe_float('_cns_per') or data['현재 PER']
        if data['현재 PER'] and data['추정 PER']:
            data['DeltaPER'] = data['현재 PER'] - data['추정 PER']

        # 3. Financial Table Data
        table = soup.select_one('table.tb_type1.tb_num.tb_type1_ifrs')
        if table:
            rows = table.select('tbody tr')
            for row in rows:
                th_tag = row.find('th')
                if not th_tag: continue
                th_text = th_tag.text.strip()
                tds = row.find_all('td')
                if len(tds) < 4: continue
                
                def get_val(idx1=3, idx2=2):
                    v1 = tds[idx1].text.replace(',', '').strip()
                    if v1 and v1 != '-': 
                        try: return float(v1)
                        except ValueError: pass
                    v2 = tds[idx2].text.replace(',', '').strip()
                    try: return float(v2) if v2 and v2 != '-' else None
                    except ValueError: return None

                if '영업이익' in th_text and '률' not in th_text:
                    data['전년 영업이익'] = get_val(2, 2)
                    data['추정 영업이익'] = get_val(3, 2)
                elif 'ROE' in th_text:
                    data['추정 ROE'] = get_val(3, 2)
                elif '부채비율' in th_text:
                    data['부채비율'] = get_val(3, 2)

        if data['전년 영업이익'] and data['추정 영업이익'] and data['전년 영업이익'] != 0:
            data['이익성장률'] = (data['추정 영업이익'] / data['전년 영업이익']) - 1.0

    except Exception:
        pass
    return data

@st.cache_data(ttl=3600*12, show_spinner=False)
def scrape_all_data(tickers: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, str]:
    """Scrape financial metrics for all tickers in parallel."""
    results = []
    total = len(tickers)
    my_bar = st.progress(0, text="데이터 크롤링 중... 잠시만 기다려주세요.")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_financial_data, t): i for i, t in enumerate(tickers)}
        for i, future in enumerate(as_completed(futures)):
            results.append(future.result())
            if (i + 1) % 10 == 0 or (i + 1) == total:
                my_bar.progress((i + 1) / total, text=f"데이터 크롤링 중... {i+1}/{total}")
                
    my_bar.empty()
    scrape_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return pd.DataFrame(results), scrape_time

# --- UI Components ---

def apply_custom_css():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;700&display=swap');
        html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
        .stMetric { background-color: #f0f2f6; padding: 10px; border-radius: 10px; }
        @media (max-width: 768px) {
            .main .block-container { padding: 1rem 0.5rem !important; }
            h1 { font-size: 1.4rem !important; }
        }
        </style>
    """, unsafe_allow_html=True)

def load_data(force_refresh: bool) -> Tuple[pd.DataFrame, str]:
    """Handle data loading from cache or fresh scraping."""
    df, scrape_time = None, None

    if not force_refresh:
        if os.path.exists(CACHE_FILE) and os.path.exists(CACHE_TIME_FILE):
            try:
                df = pd.read_csv(CACHE_FILE, dtype={'종목코드': str})
                with open(CACHE_TIME_FILE, "r") as f:
                    scrape_time = f.read().strip()
            except Exception:
                df = None

    if df is None or force_refresh:
        status_area = st.empty()
        with status_area.container():
            st.info("📊 데이터 크롤링중... 잠시만 기다려주세요.")
            tickers = get_top_500_tickers()
            df, scrape_time = scrape_all_data(tickers)
        status_area.empty()
            
        try:
            df.to_csv(CACHE_FILE, index=False, encoding='utf-8-sig')
            with open(CACHE_TIME_FILE, "w") as f:
                f.write(scrape_time)
        except Exception:
            pass
            
    return df, scrape_time

def render_sidebar(df: pd.DataFrame) -> Dict[str, Any]:
    """Render sidebar filters and return user selections."""
    st.sidebar.header("🔍 검색 및 필터")
    search_query = st.sidebar.text_input("종목명/코드 검색", "")
    
    st.sidebar.markdown("---")
    apply_filters = st.sidebar.checkbox("필터 적용 (AND 조건)", value=True)
    
    all_categories = sorted(df['산업카테고리'].dropna().unique().tolist()) if df is not None else []
    selected_categories = st.sidebar.multiselect("📂 산업카테고리 선택", all_categories, default=[])

    show_all = st.sidebar.checkbox("결측치 포함(500개 보기)", value=False)
    
    f_per = st.sidebar.number_input("Max 추정 PER", value=9999.0, disabled=not apply_filters)
    f_roe = st.sidebar.number_input("Min 추정 ROE (%)", value=-9999.0, disabled=not apply_filters)
    f_debt = st.sidebar.number_input("Max 부채비율 (%)", value=9999.0, disabled=not apply_filters)
    f_mcap = st.sidebar.number_input("Min 시가총액 (억원)", value=0, step=500, disabled=not apply_filters)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔃 정렬 설정")
    cols_order = ['번호', '종목코드', '종목명', '산업카테고리', '시가총액(억)', 'DeltaPER', '현재 PER', '추정 PER', '추정 ROE', '부채비율', '이익성장률']
    
    s1_col = st.sidebar.selectbox("1순위 정렬", cols_order, index=5)
    s1_asc = st.sidebar.radio("1순위 방향", ["내림차순", "오름차순"], horizontal=True, key="s1") == "오름차순"
    
    s2_col = st.sidebar.selectbox("2순위 정렬 (AND 조건)", ["없음"] + cols_order, index=0)
    s2_asc = st.sidebar.radio("2순위 방향", ["내림차순", "오름차순"], horizontal=True, key="s2") == "오름차순"

    st.sidebar.markdown("---")
    st.sidebar.subheader("📱 디스플레이 설정")
    mobile_view = st.sidebar.checkbox("모바일 뷰 (핵심 지표만)", value=True)

    return {
        'search': search_query, 'apply': apply_filters, 'cats': selected_categories,
        'show_all': show_all, 'per': f_per, 'roe': f_roe, 'debt': f_debt, 'mcap': f_mcap,
        's1': (s1_col, s1_asc), 's2': (s2_col, s2_asc), 'mobile': mobile_view
    }

def process_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Filter and sort the dataframe based on user configuration."""
    f_df = df.copy() if config['show_all'] else df.dropna(subset=['추정 PER', '추정 ROE', '부채비율', '시가총액(억)'])
    
    if config['cats']:
        f_df = f_df[f_df['산업카테고리'].isin(config['cats'])]

    if config['apply']:
        numeric_cols = ['추정 PER', '추정 ROE', '부채비율', '시가총액(억)']
        for c in numeric_cols:
            f_df[c] = pd.to_numeric(f_df[c], errors='coerce')
        cond = (f_df['추정 PER'] <= config['per']) & (f_df['추정 ROE'] >= config['roe']) & \
               (f_df['부채비율'] <= config['debt']) & (f_df['시가총액(억)'] >= config['mcap'])
        f_df = f_df[cond]

    if config['search']:
        q = config['search']
        f_df = f_df[f_df['종목명'].str.contains(q, case=False, na=False) | 
                    f_df['종목코드'].str.contains(q, case=False, na=False)]

    # Multi-sorting
    sort_cols = [config['s1'][0]]
    sort_orders = [config['s1'][1]]
    if config['s2'][0] != "없음":
        sort_cols.append(config['s2'][0])
        sort_orders.append(config['s2'][1])
    
    f_df = f_df.sort_values(by=sort_cols, ascending=sort_orders).reset_index(drop=True)
    f_df['번호'] = f_df.index + 1
    return f_df

# --- Main Application ---

def main():
    apply_custom_css()
    st.title("📈 Delta PER Table")
    
    if 'force_refresh' not in st.session_state:
        st.session_state.force_refresh = False

    df, scrape_time = load_data(st.session_state.force_refresh)
    st.session_state.force_refresh = False

    # Header Controls
    h_col1, h_col2 = st.columns([2, 1])
    with h_col1: st.caption(f"🕒 최근 업데이트: {scrape_time}")
    with h_col2:
        if st.button("🔄 새로 크롤링", use_container_width=True):
            st.cache_data.clear()
            st.session_state.force_refresh = True
            st.rerun()

    # Sidebar & Data Processing
    config = render_sidebar(df)
    filtered_df = process_data(df, config)

    # Sidebar Summary
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 데이터 요약")
    st.sidebar.info(f"전체 {len(df)}개 중 **{len(filtered_df)}개 유효**")

    # Main Content
    st.markdown(f"**✅ 검색 결과: {len(filtered_df)}개 종목**")
    
    cols_order = ['번호', '종목코드', '종목명', '산업카테고리', '시가총액(억)', 'DeltaPER', '현재 PER', '추정 PER', '추정 ROE', '부채비율', '이익성장률']
    
    if config['mobile']:
        st.dataframe(filtered_df[['종목명', 'DeltaPER', '현재 PER', '추정 PER']], 
                     column_config={"종목명": st.column_config.TextColumn(width=100),
                                    "DeltaPER": st.column_config.NumberColumn("Delta", format="%.2f", width=60)},
                     use_container_width=True, hide_index=True)
    else:
        st.dataframe(filtered_df[cols_order], use_container_width=True, hide_index=True)

    # Footer Actions
    st.markdown("---")
    f_col1, f_col2 = st.columns([1, 1])
    with f_col1:
        csv = filtered_df[cols_order].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 엑셀(CSV) 다운로드", data=csv, file_name="delta_per_data.csv", use_container_width=True)
    with f_col2:
        with st.expander("ℹ️ Delta PER 이란?"):
            st.info("- **현재 PER**에서 **선행 PER**를 뺀 값\n- **클수록** 미래 실적 대비 주가가 저평가되어 투자 가치가 높음")
    
    st.caption("💡 Tip: 표의 제목을 클릭하면 정렬됩니다.")

if __name__ == "__main__":
    main()