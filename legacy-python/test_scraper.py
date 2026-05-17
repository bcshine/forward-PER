from app import get_top_500_tickers, scrape_all_data

print("Fetching top 5 tickers to test...")
tickers = get_top_500_tickers()
top_5 = tickers[:5]

print("Top 5:", top_5)

df = scrape_all_data(top_5)
print("Scraping results:")
print(df)
