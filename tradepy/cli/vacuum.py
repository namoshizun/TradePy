import re
import tradepy
import argparse
import shutil
from datetime import date, timedelta
from pathlib import Path
from tqdm import tqdm

from tradepy.utils import get_latest_trade_date
from tradepy.depot.stocks import StocksDailyBarsDepot
from tradepy.trade_cal import trade_cal


def delete_stale_redis_keys(days: int):
    print("删除过期的Redis键")

    # Get the Redis client
    redis_client = tradepy.config.common.get_redis_client()  # type: ignore

    # Get all keys matching the pattern "tradepy:*"
    keys = redis_client.keys("tradepy:*")
    today = date.today()

    # Delete tradepy keys older than `days`
    date_regex = r"\d{4}-\d{2}-\d{2}"
    for key in keys:
        if create_date := re.findall(date_regex, key):
            create_date = create_date[0]
            if date.fromisoformat(create_date) < today - timedelta(days=days):
                redis_client.delete(key)
                print(f"删除键: {key}")


def delete_daily_k(days: int):
    print("删除过期的日K数据")

    # Load day bars up to (now - days)
    until_date: str = str(get_latest_trade_date())
    end_idx = trade_cal.index(until_date) + 1
    start_idx = end_idx + days
    since_date = trade_cal[start_idx]
    df = StocksDailyBarsDepot.load(since_date=since_date)

    # Export the capped day bars
    p = tradepy.config.common.database_dir / StocksDailyBarsDepot.folder_name
    for code, sub_df in tqdm(df.groupby(level="code")):
        sub_df.drop("code", axis=1, inplace=True)
        sub_df.to_csv(p / f"{code}.csv", index=False)


def delete_workspace_dirs(days: int):
    print("删除过期的工作目录")
    workspace_dirs = Path.home() / ".tradepy" / "workspace"
    if not workspace_dirs.exists():
        return

    for p in workspace_dirs.iterdir():
        if p.is_dir() and p.name.startswith("202"):
            p_date = date.fromisoformat(p.name)
            if p_date < date.today() - timedelta(days=days):
                shutil.rmtree(p)
                print(f"删除目录: {p}")


def main():
    parser = argparse.ArgumentParser(
        prog="vacuum.py",
        description="TradePy的资源清理CLI",
    )
    subparsers = parser.add_subparsers(
        title="sub-commands", dest="sub_command", metavar="sub-command", required=True
    )

    # Sub-parser for 'redis' sub-command
    redis_parser = subparsers.add_parser("redis", help="清理过期Redis键")
    redis_parser.add_argument("--days", type=int, required=True, help="删除多少天前的Redis键")
    redis_parser.set_defaults(func=delete_stale_redis_keys)

    # Sub-parser for 'database' sub-command
    database_parser = subparsers.add_parser("database", help="清理过期的日K数据")
    database_parser.add_argument("--days", type=int, required=True, help="删除多少天前的日K数据")
    database_parser.set_defaults(func=delete_daily_k)

    # Sub-parser for 'workspace' sub-command
    workspace_parser = subparsers.add_parser("workspace", help="清理过期的工作目录")
    workspace_parser.add_argument("--days", type=int, required=True, help="删除多少天前的工作目录")
    workspace_parser.set_defaults(func=delete_workspace_dirs)

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args.days)
    else:
        print("请提供子命令: 'redis' or 'database'.")


if __name__ == "__main__":
    main()
