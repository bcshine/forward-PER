---
title: Delta-PER
emoji: 📈
colorFrom: blue
colorTo: indigo
sdk: streamlit
sdk_version: 1.52.1
app_file: app.py
pinned: false
---

# 📈 Delta PER Table

현재 PER에서 12개월 선행(Forward) 추정 PER을 뺀 값(Delta PER)을 분석하여 투자 가치가 높은 종목을 발굴하는 도구입니다.

### 🚀 사용 방법
- **Delta PER (ΔPER):** 현재 PER - 12개월 선행 PER. 숫자가 클수록 이익 성장이 기대되는 종목입니다.
- **필터:** 시가총액, ROE, 부채비율 등을 조절하여 원하는 종목을 필터링할 수 있습니다.

### 🛠 기술 스택
- Python
- Streamlit
- Pandas
- BeautifulSoup (Crawling from Naver Finance)
