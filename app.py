import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration & Constants ---
st.set_page_config(layout="wide", page_title="Naver Finance Screener")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

# --- Scraping Logic ---

def fetch_ticker_page(sosok, page):
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

@st.cache_data(ttl=3600*12)
def get_top_500_tickers():
    """Retrieve top 500 stocks using parallel requests for faster initial load."""
    all_tickers = []
    tasks = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for sosok in [0, 1]: # 0: KOSPI, 1: KOSDAQ
            for page in range(1, 6): # Top 5 pages each
                tasks.append(executor.submit(fetch_ticker_page, sosok, page))
        
        for future in as_completed(tasks):
            all_tickers.extend(future.result())
            
    # Sort by Market Cap and take Top 500
    all_tickers = sorted([t for t in all_tickers if t['Mcap'] is not None], key=lambda x: x['Mcap'], reverse=True)
    return all_tickers[:500]

def get_financial_data(ticker_info):
    """Scrape financial metrics for a single ticker with optimized extraction."""
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
                th_text = row.find('th').text.strip() if row.find('th') else ""
                tds = row.find_all('td')
                if len(tds) < 4: continue
                
                # Helper to get the best available value (prefers projected, fallbacks to previous year)
                def get_val(idx1=3, idx2=2):
                    v1 = tds[idx1].text.replace(',', '').strip()
                    if v1 and v1 != '-': return float(v1)
                    v2 = tds[idx2].text.replace(',', '').strip()
                    return float(v2) if v2 and v2 != '-' else None

                try:
                    if '영업이익' in th_text and '률' not in th_text:
                        data['전년 영업이익'] = get_val(2, 2)
                        data['추정 영업이익'] = get_val(3, 2)
                    elif 'ROE' in th_text:
                        data['추정 ROE'] = get_val(3, 2)
                    elif '부채비율' in th_text:
                        data['부채비율'] = get_val(3, 2)
                except (ValueError, IndexError):
                    continue

        if data['전년 영업이익'] and data['추정 영업이익'] and data['전년 영업이익'] != 0:
            data['이익성장률'] = (data['추정 영업이익'] / data['전년 영업이익']) - 1.0

        return data
    except Exception:
        return data

@st.cache_data(ttl=3600*12)
def scrape_all_data(tickers):
    results = []
    my_bar = st.progress(0, text="Scraping Stocks. Please wait...")
    total = len(tickers)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_financial_data, t) for t in tickers]
        for i, future in enumerate(as_completed(futures)):
            results.append(future.result())
            if (i + 1) % 10 == 0 or (i + 1) == total:
                my_bar.progress((i + 1) / total, text=f"Scraping... {i+1}/{total}")
                
    my_bar.empty()
    return pd.DataFrame(results), datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# --- UI Layout ---

def main():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;700&display=swap');
        html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
        @media (max-width: 768px) {
            .main .block-container { padding: 2rem 1rem !important; }
            h1 { font-size: 1.6rem !important; }
            .stMarkdown p { font-size: 0.85rem !important; }
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("📈 Delta PER Table")
    
    with st.spinner("Fetching Tickers..."):
        tickers = get_top_500_tickers()
    
    df, scrape_time = scrape_all_data(tickers)
    
    # Header UI
    col1, col2 = st.columns([2, 1])
    with col1: st.caption(f"🕒 최근 업데이트: {scrape_time}")
    with col2:
        if st.button("🔄 새로 크롤링", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Sidebar & Filtering
    st.sidebar.header("Search & Filter")
    search_query = st.sidebar.text_input("🔍 종목명/코드 검색", "")
    
    st.sidebar.markdown("---")
    apply_filters = st.sidebar.checkbox("필터 적용", value=True)
    show_all = st.sidebar.checkbox("결측치 포함(500개 보기)", value=False)
    
    f_per = st.sidebar.number_input("Max 추정 PER", value=9999.0, disabled=not apply_filters)
    f_roe = st.sidebar.number_input("Min 추정 ROE (%)", value=-9999.0, disabled=not apply_filters)
    f_debt = st.sidebar.number_input("Max 부채비율 (%)", value=9999.0, disabled=not apply_filters)
    f_mcap = st.sidebar.number_input("Min 시가총액 (억원)", value=0, step=500, disabled=not apply_filters)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("📱 디스플레이 설정")
    mobile_view = st.sidebar.checkbox("모바일 뷰 (핵심 지표만)", value=True, key="mobile_view_v3")

    # Data Processing
    filtered_df = df.copy() if show_all else df.dropna(subset=['추정 PER', '추정 ROE', '부채비율', '시가총액(억)'])
    
    if apply_filters:
        for c in ['추정 PER', '추정 ROE', '부채비율', '시가총액(억)']:
            filtered_df[c] = pd.to_numeric(filtered_df[c], errors='coerce')
        cond = (filtered_df['추정 PER'] <= f_per) & (filtered_df['추정 ROE'] >= f_roe) & \
               (filtered_df['부채비율'] <= f_debt) & (filtered_df['시가총액(억)'] >= f_mcap)
        filtered_df = filtered_df[cond]

    if search_query:
        filtered_df = filtered_df[filtered_df['종목명'].str.contains(search_query, case=False, na=False) | 
                                  filtered_df['종목코드'].str.contains(search_query, case=False, na=False)]

    if 'DeltaPER' in filtered_df.columns:
        filtered_df = filtered_df.sort_values(by='DeltaPER', ascending=False)
    
    filtered_df = filtered_df.reset_index(drop=True)
    filtered_df['번호'] = filtered_df.index + 1

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 데이터 요약")
    st.sidebar.info(f"전체 {len(tickers)}개 중 **{len(filtered_df)}개 유효**")

    st.markdown(f"**✅ 검색 결과: {len(filtered_df)}개 종목**")

    # Main Display
    cols_order = ['번호', '종목코드', '종목명', '산업카테고리', '시가총액(억)', 'DeltaPER', '현재 PER', '추정 PER', '추정 ROE', '부채비율', '이익성장률']
    csv_data = filtered_df[cols_order].to_csv(index=False).encode('utf-8-sig')

    if mobile_view:
        st.dataframe(filtered_df[['종목명', 'DeltaPER', '현재 PER', '추정 PER']], 
                     column_config={"종목명": st.column_config.TextColumn(width=100),
                                    "DeltaPER": st.column_config.NumberColumn("Delta", format="%.2f", width=60)},
                     use_container_width=True, hide_index=True)
    else:
        st.dataframe(filtered_df[cols_order], use_container_width=True, hide_index=True)

    # Footer Info
    st.markdown("---")
    c3, c4 = st.columns([1, 1])
    with c3: st.download_button("📥 엑셀(CSV) 다운로드", data=csv_data, file_name="delta_per_data.csv", use_container_width=True)
    with c4:
        with st.expander("ℹ️ Delta PER 이란?"):
            st.info("- **현재 PER**에서 **선행 PER**를 뺀 값\n- **클수록** 미래 실적 대비 주가가 저평가되어 투자 가치가 높음")
    st.caption("💡 Tip: 표의 제목을 클릭하면 정렬됩니다.")

if __name__ == "__main__":
    main()