import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(layout="wide", page_title="Naver Finance Screener")

# Headers to bypass some basic bot blocks
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

@st.cache_data(ttl=3600*12)  # Cache data for 12 hours
def get_top_500_tickers():
    """Retrieve top 500 stocks directly from Naver Finance Market Cap ranking."""
    tickers = []
    
    # We will scrape top 5 pages from KOSPI (250 stocks) and KOSDAQ (250 stocks)
    # Each page has up to 50 stocks.
    
    for sosok, limit in [(0, 5), (1, 5)]:
        # sosok: 0 = KOSPI, 1 = KOSDAQ
        for page in range(1, limit + 1):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, headers=HEADERS, timeout=5)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'lxml')
                
                table = soup.find('table', {'class': 'type_2'})
                if not table: continue
                
                tbody = table.find('tbody')
                if not tbody: continue
                
                rows = tbody.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) > 1:
                        # Find the a tag inside the row
                        a_tag = row.find('a', href=True)
                        if a_tag and '/item/main.naver?code=' in a_tag['href']:
                            code = a_tag['href'].split('code=')[-1].strip()
                            name = a_tag.text.strip()
                            # Get Market Cap from column index 6 if possible
                            mcap = None
                            try:
                                mcap = int(cols[6].text.replace(',', '').strip())
                            except:
                                pass
                            
                            tickers.append({
                                'Code': code,
                                'Name': name,
                                'Mcap': mcap
                            })
            except Exception as e:
                pass
                
    # We now have about 500 stocks (250 KOSPI, 250 KOSDAQ).
    # Sort them globally by Market Cap to get the true Top 500 combined.
    tickers = [t for t in tickers if t['Mcap'] is not None]
    tickers = sorted(tickers, key=lambda x: x['Mcap'], reverse=True)
    
    # Top 500
    # Uncomment next line to test top 10 only:
    # return tickers[:10]
    return tickers[:500]

def get_financial_data(ticker_info):
    """Scrape financial metrics for a single ticker."""
    code = ticker_info['Code']
    name = ticker_info['Name']
    
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        # We need to extract:
        # 1. 시가총액 (Market Cap)
        # 2. 현재 PER (Trailing)
        # 3. 12M PER (Forward)
        # 4. 전년 영업이익, 추정 영업이익
        # 5. 추정 ROE
        # 6. 부채비율
        
        data = {
            '번호': 0, # Will be filled later
            '종목코드': code,
            '종목명': name,
            '산업카테고리': None,
            '시가총액(억)': None,
            '현재 PER': None,
            '추정 PER': None,
            '전년 영업이익': None,
            '추정 영업이익': None,
            '추정 ROE': None,
            '부채비율': None,
            '이익성장률': None,
            'DeltaPER': None
        }
        
        # 0. 산업카테고리 찾기 (section > h4 class='h_sub sub_tit7' -> a tag)
        for sect in soup.find_all('div', {'class': 'section'}):
            h4 = sect.find('h4', {'class': 'h_sub sub_tit7'})
            if h4:
                a_tag = sect.find('a')
                if a_tag:
                    data['산업카테고리'] = a_tag.text.strip()
                break
        
        # 1. 시가총액 찾기
        # <em id="_market_sum">
        mcap_em = soup.find('em', id='_market_sum')
        if mcap_em:
            mcap_str = mcap_em.text.replace(',', '').replace('\t', '').replace('\n', '')
            # Naver usually shows mcap in 억원 (e.g., 3조 5,000 => "35000", or just "35,000")
            # Usually the text might contain '조' which needs parsing.
            mcap_str = mcap_str.replace('조', '').replace(' ', '')
            try:
                data['시가총액(억)'] = float(mcap_str)
            except ValueError:
                pass

        # 2. 현재 PER / 12M PER
        # <em id="_per">
        per_em = soup.find('em', id='_per')
        if per_em:
            try:
                data['현재 PER'] = float(per_em.text)
            except ValueError:
                pass
                
        cns_per_em = soup.find('em', id='_cns_per')
        if cns_per_em:
            try:
                data['추정 PER'] = float(cns_per_em.text)
            except ValueError:
                pass

        # Calculate Delta PER early if possible
        if data['현재 PER'] is not None and data['추정 PER'] is not None:
            data['DeltaPER'] = data['현재 PER'] - data['추정 PER']

        # 3. 달러/이익 등 표 크롤링
        # The financial table class is 'tb_type1 tb_num tb_type1_ifrs'
        table = soup.find('table', {'class': 'tb_type1 tb_num tb_type1_ifrs'})
        if not table:
            return data
            
        tbody = table.find('tbody')
        if not tbody:
            return data
            
        rows = tbody.find_all('tr')
        
        # We need to find the specific rows based on th text
        for row in rows:
            th = row.find('th')
            if not th:
                continue
                
            row_header = th.text.strip()
            tds = row.find_all('td')
            
            # Usually column 3 is last year (e.g. 2023.12), column 4 or 5 is projected (e.g. 2024.12(E))
            # Let's extract values safely. We look at the last column that has data or specific (E) columns.
            
            if '영업이익' in row_header and '영업이익률' not in row_header:
                # Naver has: 최근 연간 실적 (Usually 4 columns)
                # td[2] = 2022, td[3] = 2023, td[4] = 2024(E)
                if len(tds) >= 4:
                    try:
                        val = tds[2].text.replace(',', '').strip()
                        if val: data['전년 영업이익'] = float(val)
                    except: pass
                    try:
                        val = tds[3].text.replace(',', '').strip()
                        if val: 
                            data['추정 영업이익'] = float(val)
                        else:
                            # Fallback to last year if projected is empty
                            val2 = tds[2].text.replace(',', '').strip()
                            if val2: data['추정 영업이익'] = float(val2)
                    except: pass
            
            elif 'ROE' in row_header and 'ROIC' not in row_header:
                if len(tds) >= 4:
                    try:
                        val = tds[3].text.replace(',', '').strip()
                        if val: 
                            data['추정 ROE'] = float(val)
                        else:
                            val2 = tds[2].text.replace(',', '').strip()
                            if val2: data['추정 ROE'] = float(val2)
                    except: pass
                    
            elif '부채비율' in row_header:
                if len(tds) >= 4:
                    try:
                        val = tds[3].text.replace(',', '').strip()
                        if val: 
                            data['부채비율'] = float(val)
                        else:
                            val2 = tds[2].text.replace(',', '').strip()
                            if val2: data['부채비율'] = float(val2)
                    except: pass

        # Fallback for Forward PER if missing
        if data['추정 PER'] is None and data['현재 PER'] is not None:
            data['추정 PER'] = data['현재 PER']

        # Recalculate Delta PER in case 추정 PER fell back to 현재 PER
        if data['현재 PER'] is not None and data['추정 PER'] is not None:
            data['DeltaPER'] = data['현재 PER'] - data['추정 PER']

        # 4. Calculate 이익성장률
        if data['전년 영업이익'] and data['추정 영업이익'] and data['전년 영업이익'] != 0:
            # If previous year was negative, growth rate calculation might be misleading, but we'll apply standard formula for now.
            try:
                data['이익성장률'] = (data['추정 영업이익'] / data['전년 영업이익']) - 1.0
            except ZeroDivisionError:
                pass

        return data

    except Exception as e:
        # print(f"Error processing {name} ({code}): {e}")
        return {
            '종목코드': code,
            '종목명': name,
            '시가총액(억)': None,
            '현재 PER': None,
            '추정 PER': None,
            '전년 영업이익': None,
            '추정 영업이익': None,
            '추정 ROE': None,
            '부채비율': None,
            '이익성장률': None,
            'DeltaPER': None
        }

@st.cache_data(ttl=3600*12)
def scrape_all_data(tickers):
    results = []
    
    # Basic progress bar
    progress_text = "Scraping Top 500 Stocks. Please wait."
    my_bar = st.progress(0, text=progress_text)
    
    total = len(tickers)
    
    # Use ThreadPoolExecutor for concurrent scraping
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(get_financial_data, t): t for t in tickers}
        
        count = 0
        for future in as_completed(future_to_ticker):
            res = future.result()
            results.append(res)
            
            count += 1
            # Update progress bar every 5 items to reduce UI lag
            if count % 5 == 0 or count == total:
                my_bar.progress(count / total, text=f"Scraping... {count}/{total} completed.")
                
    my_bar.empty()
    scrape_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return pd.DataFrame(results), scrape_time

def main():
    st.title("📈 Delta PER Table")
    
    with st.spinner("Fetching Top 500 Tickers..."):
        tickers = get_top_500_tickers()
    
    df, scrape_time = scrape_all_data(tickers)
    st.markdown(f"**데이터 수집 일시:** {scrape_time}")
    
    if st.button("🔄 새로 크롤링하기"):
        st.cache_data.clear()
        st.rerun()
    
    # Sidebar Filters
    st.sidebar.header("Filter Settings")
    
    apply_filters = st.sidebar.checkbox("필터 적용", value=True)
    show_all_500 = st.sidebar.checkbox("결측치 포함(500개 보기)", value=False, help="필수 지표가 비어있는 종목도 표에 포함합니다.")
    max_fwd_per = st.sidebar.number_input("Max 추정 PER (Forward PER)", value=9999.0, step=1.0, disabled=not apply_filters)
    min_roe = st.sidebar.number_input("Min 추정 ROE (%)", value=-9999.0, step=1.0, disabled=not apply_filters)
    max_debt = st.sidebar.number_input("Max 부채비율 (%)", value=9999.0, step=1.0, disabled=not apply_filters)
    min_mcap = st.sidebar.number_input("Min 시가총액 (억원)", value=0, step=500, disabled=not apply_filters)
    
    st.caption(
        f"요약: 요청 종목 {len(tickers)}개 → 수집 결과 {len(df)}행"
    )

    # 1) 결측치 처리: 기본은 엄격 모드(필수 지표가 없으면 제외)
    if show_all_500:
        filtered_df = df.copy()
    else:
        filtered_df = df.dropna(subset=['추정 PER', '추정 ROE', '부채비율', '시가총액(억)'])

    st.caption(
        f"결측치 제거 후 {len(filtered_df)}행"
        + (" (결측치 포함 모드)" if show_all_500 else "")
    )
    
    # 2) 필터 조건 (필터 적용 체크 시에만)
    if apply_filters:
        # 결측치 포함 모드에서는 비교 연산이 NaN을 False로 처리하게 두되,
        # 최소한 숫자 비교 가능한 형태로만 변환합니다.
        for c in ['추정 PER', '추정 ROE', '부채비율', '시가총액(억)']:
            filtered_df[c] = pd.to_numeric(filtered_df[c], errors='coerce')

        cond = (
            (filtered_df['추정 PER'] <= max_fwd_per) &
            (filtered_df['추정 ROE'] >= min_roe) &
            (filtered_df['부채비율'] <= max_debt) &
            (filtered_df['시가총액(억)'] >= min_mcap)
        )
        filtered_df = filtered_df[cond]
        st.caption(f"필터 적용 후 {len(filtered_df)}행")
    
    if 'DeltaPER' in filtered_df.columns:
        filtered_df = filtered_df.sort_values(by='DeltaPER', ascending=False)
        
    st.subheader(f"Filtered Results: {len(filtered_df)} stocks")
    
    # Apply explicit sorting buttons
    st.markdown("**(Tip: 아래 표의 컬럼 제목을 클릭하시면 오름차순/내림차순 정렬이 동작합니다.)**")
    
    # Fill the '번호' column properly after filtering
    filtered_df = filtered_df.reset_index(drop=True)
    filtered_df['번호'] = filtered_df.index + 1

    # Desired Column Order:
    # 번호, code, name, 산업카테고리, delta per, 12m fwd per, 현재 per, ROE, 부채비율, 이익성장율
    cols_order = ['번호', '종목코드', '종목명', '산업카테고리', 'DeltaPER', '추정 PER', '현재 PER', '추정 ROE', '부채비율', '이익성장률']
    # Keep other columns slightly to the side if needed, but display only these primarily.
    
    csv_data = filtered_df[cols_order].to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 엑셀(CSV) 다운로드",
        data=csv_data,
        file_name=f"forward_per_data.csv",
        mime="text/csv",
    )
    
    # Display the dataframe with Streamlit
    st.dataframe(
        filtered_df[cols_order],
        column_config={
            "번호": st.column_config.NumberColumn("번호", format="%d"),
            "종목코드": st.column_config.TextColumn("code"),
            "종목명": st.column_config.TextColumn("name"),
            "산업카테고리": st.column_config.TextColumn("산업카테고리"),
            "DeltaPER": st.column_config.NumberColumn("delta per", format="%.2f"),
            "추정 PER": st.column_config.NumberColumn("12m fwd per", format="%.2f"),
            "현재 PER": st.column_config.NumberColumn("현재 per", format="%.2f"),
            "추정 ROE": st.column_config.NumberColumn("ROE", format="%.2f"),
            "부채비율": st.column_config.NumberColumn("부채비율", format="%.2f"),
            "이익성장률": st.column_config.NumberColumn("이익성장율", format="%.2f"),
        },
        use_container_width=True,
        hide_index=True,
    )

if __name__ == "__main__":
    main()
