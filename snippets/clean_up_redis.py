import tradepy
from datetime import date

r = tradepy.config.trading.get_redis_client()

today = date.today()

keys = [k for k in r.keys("tradepy:*") if not k.startswith(f"tradepy:{today}")]

if keys:
    r.delete(*keys)
