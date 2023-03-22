import sys
from pathlib import Path
from tradepy.warehouse import StocksDailyBarsDepot
from tqdm import tqdm


def get_stocks_dir():
    return Path(f'./database/{StocksDailyBarsDepot.folder_name}')


p = get_stocks_dir()
new_p = p.with_suffix('.bak')
if new_p.exists():
    print(f'{new_p} already exists. double check please')
    sys.exit(1)


since_date = "2022-11-08"
df = StocksDailyBarsDepot.load(since_date=since_date)

p.rename(new_p)
p = get_stocks_dir()
p.mkdir()

print(p)
for code, sub_df in tqdm(df.groupby("code")):
    sub_df.to_csv(p / f'{code}.csv')
