import pandas as pd
from tqdm import tqdm
from functools import wraps, partial
from typing import Any, Generator, List, get_args
from pathlib import Path
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteType, WriteApi, WriteOptions
from influxdb_client.domain.write_precision import WritePrecision

import trade
from trade.convertion import convert_code_to_market
from trade.types import FundamentalTags, TickTagSet, TickTags


class TicksDepot:

    caches: dict[str, Any] = dict()

    def __init__(self, folder: str) -> None:
        self.folder = Path('datasets') / folder
        self.folder.mkdir(parents=True, exist_ok=True)

    def size(self) -> int:
        return sum(1 for _ in self.folder.iterdir())

    def save(self, df: pd.DataFrame, filename: str):
        assert filename.endswith('csv')
        df.to_csv(self.folder / filename, index=False)

    def append(self, df: pd.DataFrame, filename: str):
        assert filename.endswith('csv')
        path = self.folder / filename

        if not path.exists():
            df.to_csv(path, index=False)

        _df = pd.read_csv(path, index_col=None)

        (
            pd.concat([df, _df])
            .drop_duplicates()
            .to_csv(path, index=False)
        )

    def exists(self, name: str):
        return (self.folder / f'{name}.csv').exists()

    def traverse(self, always_load=False):
        load = partial(pd.read_csv)

        for path in tqdm(self.folder.iterdir()):
            if str(path).endswith('.csv'):
                if always_load:
                    yield path.stem, load(path)
                else:
                    should_load = yield path.stem
                    if should_load:
                        yield load(path)

    def load_stocks_ticks(self,
                          index_by: str | list[str]="code",
                          since_date: str | None=None) -> pd.DataFrame:
        assert self.folder.name == "daily.stocks", self.folder
        def loader() -> Generator[pd.DataFrame, None, None]:
            source_iter = self.traverse()

            while True:
                try:
                    filename: str = next(source_iter)
                except StopIteration:
                    break

                code, exchange = filename.split('.')
                if not trade.listing.has_code(code):
                    continue

                company = trade.listing.get_by_code(code).name
                market = convert_code_to_market(code)

                df = source_iter.send(True)
                df[["company", "code", "market", "exchange"]] = company, code, market, exchange

                if since_date:
                    yield df.query(f'timestamp >= "{since_date}"')
                else:
                    yield df

        df = pd.concat(loader())
        df.set_index(index_by, inplace=True)
        df.sort_index(inplace=True)
        return df

    def load_index_ticks(self, index_by: str | list[str]="code", cache=False) -> pd.DataFrame:
        cache_key = "index-ticks"
        if cache and cache_key in self.caches:
            return self.caches[cache_key]

        assert self.folder.name == "daily.index", self.folder
        def loader() -> Generator[pd.DataFrame, None, None]:
            for code, df in self.traverse(always_load=True):
                df["code"] = code
                yield df

        df = pd.concat(loader())
        df.set_index(index_by, inplace=True)
        df.sort_index(inplace=True)
        df.sort_values("timestamp", inplace=True)

        if cache:
            self.caches[cache_key] = df.copy()
        return df


class TradeCalendarDepot:

    path = "./datasets/trade_cal.csv"

    @staticmethod
    def load(since_date: str, end_date: str) -> pd.DataFrame:
        df = pd.read_csv(TradeCalendarDepot.path)
        df = df[df["cal_date"] >= since_date]
        df = df[df["cal_date"] <= end_date]
        return df.set_index("cal_date").sort_index()


def with_write_api(fun):
    @wraps(fun)
    def inner(self: 'InfluxDepot', *args, **kwargs):
        with self.client.write_api(write_options=self.write_options) as write_api:
            kwargs['api'] = write_api
            return fun(self, *args, **kwargs)
    return inner


class InfluxDepot:

    def __init__(self,
                 org: str,
                 token: str,
                 bucket: str,
                 write_options=WriteOptions(write_type=WriteType.batching),
                 write_precision=WritePrecision.S,
                 **kwargs) -> None:

        init_args = dict(
            url='http://localhost:8086',
            org=org,
            token=token,
            timeout=10_000,
            debug=False,
        )

        init_args.update(kwargs)
        self.client = InfluxDBClient(**init_args)
        self.bucket = bucket

        # API Sets
        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()

        # Write Conf
        self.write_options = write_options
        self.write_precision = write_precision

        # Health check
        resp = self.client.health()
        assert resp and resp.status == 'pass', resp

    def delete_measurement(self, measurement: str):
        return self.delete_api.delete(
            '1970-01-01T00:00:00Z', '2099-01-01T00:00:00Z',
            f'_measurement="{measurement}"', bucket=self.bucket
        )

    @with_write_api
    def _import_dataframe(self,
                          df: pd.DataFrame,
                          measurement: str,
                          tag_columns: List[str],
                          *, api: WriteApi):

        assert tag_columns, tag_columns
        assert 'timestamp' in df.columns, df.columns

        return api.write(
            self.bucket,
            record=df,
            write_precision=self.write_precision,
            data_frame_measurement_name=measurement,
            data_frame_timestamp_column='timestamp',
            data_frame_tag_columns=tag_columns)
    
    def import_tickets(self, *args, **kwargs):
        kwargs.update(tag_columns=get_args(TickTags))
        return self._import_dataframe(*args, **kwargs)

    def import_fundamentals(self, *args, **kwargs):
        kwargs.update(tag_columns=get_args(FundamentalTags))
        return self._import_dataframe(*args, **kwargs)

    def query_dataframe(self, *args, **kwargs):
        df = self.query_api.query_data_frame(*args, **kwargs)
        if df.empty:
            return df
        return df.pivot(index=['_time', 'code'], columns=['_field'], values=['_value'])

    def get_unique_tags(self, tag: str) -> List[str]:
        df = self.query_dataframe(f"""
            from(bucket: "{self.bucket}")
            |> range(start: -3200d, stop: -100d)
            |> group(columns: ["{tag}"])
            |> distinct(column: "{tag}")
            |> keep(columns: ["_value"])
        """)
        return df['_value'].tolist()

    def __getattr__(self, name):
        if name.startswith('query'):
            return getattr(self.query_api, name)

        raise AttributeError(name)
