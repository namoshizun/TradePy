import pandas as pd
from tradepy.strategy.base import GreenStreakStrategy


df = pd.DataFrame(
    [
        ["0001", 66],
        ["0002", 4],
        ["0003", 13],
        ["0004", 25],
    ],
    columns=["code", "close"],
)

budget = 1e5

strategy = GreenStreakStrategy(budget)
positions = strategy.generate_positions("2022-01-02", df)

print(positions)
