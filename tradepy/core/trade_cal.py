from datetime import date


class TradeCalendar:
    def __init__(self, trade_dates: set[str]):
        self.trade_dates = trade_dates

    def __contains__(self, date: str) -> bool:
        return date in self.trade_dates

    def min(self) -> str:
        return min(self.trade_dates)

    def max(self) -> str:
        return max(self.trade_dates)

    def is_today_trading(self) -> bool:
        return date.today().strftime("%Y-%m-%d") in self.trade_dates

    def dates_between(self, since: date, until: date) -> set[str]:
        _since = since.strftime("%Y-%m-%d")
        _until = until.strftime("%Y-%m-%d")
        return {dt for dt in self.trade_dates if _since <= dt <= _until}
