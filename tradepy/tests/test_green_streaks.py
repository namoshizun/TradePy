import pandas as pd
from tradepy.core.strategy import GreenStreakStrategy

df = pd.read_csv("/Users/dilu/Desktop/Software/Stock/datasets/daily/000001.SZ.csv")
df["code"] = "000001"
df.set_index('code', inplace=True)

GreenStreakStrategy(1e5).backtest(df)
