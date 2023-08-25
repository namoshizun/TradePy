import os
import sys


def get_base_settings(mode, database_dir: str):
    return f"""
common:
  mode: "{mode}"
  trade_lot_vol: 100
  database_dir: "{database_dir}"
"""


def create_settings_file(settings) -> str:
    conf_dir = os.path.expanduser("~/.tradepy")
    conf_file = os.path.join(conf_dir, "config.yaml")

    if not os.path.exists(conf_dir):
        os.makedirs(conf_dir)

    with open(conf_file, "w+") as f:
        f.write(settings)

    return conf_file


def settings_file_exists() -> bool:
    conf_dir = os.path.expanduser("~/.tradepy")
    conf_file = os.path.join(conf_dir, "config.yaml")
    return os.path.exists(conf_file)


def main():
    print("[TradePy初始化程序]")
    if settings_file_exists():
        print("👀 已存在配置文件，无需初始化")
        sys.exit(0)

    database_dir = input("> 请输入K线数据的下载目录（完整地址）: ")

    if not database_dir:
        print(f"K线数据的下载目录不能为空!")
        sys.exit(1)

    mode = input("> 请输入运行模式 (backtest=回测, paper-trading=模拟交易, live-trading=实盘交易) : ")
    if mode not in ["backtest", "paper-trading", "live-trading"]:
        print(f"运行模式不正确!")
        sys.exit(1)

    database_dir = os.path.expanduser(database_dir)
    os.makedirs(database_dir, exist_ok=True)

    settings = get_base_settings(mode, database_dir)
    settings_file_path = create_settings_file(settings)

    print(f"👌 已创建配置文件: {settings_file_path}")


if __name__ == "__main__":
    main()
