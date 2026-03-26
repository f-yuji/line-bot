import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

from send_news import fetch_news, filter_sent, record_sent

news = fetch_news()
print(f"\n取得: {len(news)}件")

filtered = filter_sent(news)
print(f"除外後: {len(filtered)}件")

record_sent(filtered)
print("記録完了")

filtered2 = filter_sent(news)
print(f"2回目フィルタ後: {len(filtered2)}件 ← 0になればOK")
