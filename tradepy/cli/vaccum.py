import tradepy
import argparse
from datetime import date
from pathlib import Path

from tradepy.utils import get_latest_trade_date
from tradepy.depot.stocks import StocksDailyBarsDepot
from tradepy.trade_cal import trade_cal
from tqdm import tqdm


def delete_stale_redis_keys():
    # Get the Redis client
    redis_client = tradepy.config.trading.get_redis_client()  # type: ignore

    # Get all keys matching the pattern "tradepy:*"
    keys = redis_client.keys("tradepy:*")
    today = date.today()

    # Delete keys not starting with "tradepy:"
    for key in keys:
        if not key.startswith(f"tradepy:{today}"):
            redis_client.delete(key)
            print(f"删除键: {key}")


def delete_database_records(days: int):
    def get_stocks_dir():
        return tradepy.config.common.database_dir / StocksDailyBarsDepot.folder_name

    # Load day bars up to (now - days)
    until_date: str = str(get_latest_trade_date())
    end_idx = trade_cal.index(until_date) + 1
    start_idx = end_idx + days
    since_date = trade_cal[start_idx]
    df = StocksDailyBarsDepot.load(since_date=since_date)

    # Make a backup of the database
    p = get_stocks_dir()
    back_p = Path(str(p) + "-bak-1")
    if back_p.exists():
        n_bak = list(back_p.parent.glob(p.name + "-bak-*"))
        if len(n_bak) >= 1:
            back_p = Path(str(p) + f"-bak-{len(n_bak) + 1}")
    p.rename(back_p)
    print("备份日K数据库: ", back_p)

    # Export the new day bars
    p = get_stocks_dir()
    p.mkdir()
    for code, sub_df in tqdm(df.groupby(level="code")):
        sub_df.drop("code", axis=1, inplace=True)
        sub_df.to_csv(p / f"{code}.csv", index=False)


def main():
    parser = argparse.ArgumentParser(
        prog="vaccum.py",
        description="TradePy的资源清理CLI",
    )
    subparsers = parser.add_subparsers(
        title="sub-commands", dest="sub_command", metavar="sub-command", required=True
    )

    # Sub-parser for 'redis' sub-command
    redis_parser = subparsers.add_parser("redis", help="清理过期Redis键")
    redis_parser.set_defaults(func=delete_stale_redis_keys)

    # Sub-parser for 'database' sub-command
    database_parser = subparsers.add_parser("database", help="清理过期的日K数据")
    database_parser.add_argument("--days", type=int, required=True, help="删除多少天前的日K数据")
    database_parser.set_defaults(func=delete_database_records)

    args = parser.parse_args()

    if hasattr(args, "func"):
        if args.sub_command == "database":
            args.func(args.days)
        else:
            args.func()
    else:
        print("请提供子命令: 'redis' or 'database'.")


if __name__ == "__main__":
    main()
