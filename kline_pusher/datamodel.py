from __future__ import annotations
import logging
import logging.config
from enum import Enum
from pathlib import PurePath
import datetime as dt

from attrs import define, field, asdict
from sqlalchemy.orm import registry, relationship
from sqlalchemy import (
    ForeignKey,
    Table,
    Column,
    INTEGER,
    String,
    FLOAT,
    TIMESTAMP,
    SMALLINT,
)

from .database import get_engine, get_session


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mapper_registry = registry()


def create_tables():
    mapper_registry.metadata.create_all(get_engine())
    logger.info("Tables are created")


def drop_tables():
    mapper_registry.metadata.drop_all(get_engine())
    logger.info("Tables are dropped")


class TimeFrame(Enum):
    MINUTE_1 = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1mo"

    def get_mapped_table(self) -> KlineTable:
        return KLINE_TABLES[self.name]

    # def get_mapped_class(self) -> KlineRecord:
    #     match self:
    #         case self.MIN_1:
    #             return Min1
    #         case self.MIN_3:
    #             return Min3
    #         case self.MIN_5:
    #             return Min5
    #         case self.MIN_15:
    #             return Min15
    #         case self.MIN_30:
    #             return Min30
    #         case self.HOUR_1:
    #             return Hour1
    #         case self.HOUR_2:
    #             return Hour2
    #         case self.HOUR_4:
    #             return Hour4
    #         case self.HOUR_6:
    #             return Hour6
    #         case self.HOUR_8:
    #             return Hour8
    #         case self.HOUR_12:
    #             return Hour12
    #         case self.DAY_1:
    #             return Day1
    #         case self.DAY_3:
    #             return Day3
    #         case self.WEEK_1:
    #             return Week1
    #         case self.MONTH_1:
    #             return Month1


@define
class KlineZipFile:
    fpath: PurePath = field(converter=PurePath)
    timeframe: TimeFrame | None = None
    pair: Pair | None = None

    def __attrs_post_init__(self):
        if not self.timeframe:
            timeframe = self.fpath.stem.split("-")[1]
            self.timeframe = TimeFrame(timeframe)
        if not self.pair:
            pair_name = self.fpath.stem.split("-")[0].split("_")[0]
            self.pair = Pair(pair_name)


# fmt: off
str2timestamp = lambda x: dt.datetime.fromtimestamp(int(x)/1000)\
                                     .strftime('%Y-%m-%d %H:%M:%S')
@define()
class KlineRecord:
    pid = field(converter=int)  # pair id
    opentime = field(converter=str2timestamp)  # 거래 시작시간
    open = field(converter=float)  # 시가
    high = field(converter=float)  # 최고가
    low = field(converter=float)  # 최저가
    close = field(converter=float)  # 종가
    volume = field(converter=float)  # 거래량 (the first element)
    closetime = field(converter=str2timestamp)  # 거래 종료시간
    quote_asset_volume = field(converter=float)  # 거래량 (the second element)
    number_of_trade = field(converter=int)  # 거래횟수
    taker_buy_base_asset_volume = field(converter=float)  # 테이커 매수주문이 기여한 first element 거래량
    taker_buy_quote_asset_volume = field(converter=float)  # 테이커 매수주문이 기여한 second element 거래량
    ignore = field(converter=float)  # ?

    def asdict(self):
        return asdict(self)
# fmt: on

KlineTable = lambda table_name: Table(
    table_name,
    mapper_registry.metadata,
    Column("pid", SMALLINT, ForeignKey("market_pair.id"), primary_key=True),
    Column("opentime", TIMESTAMP, primary_key=True),
    Column("open", FLOAT, nullable=False),
    Column("high", FLOAT, nullable=False),
    Column("low", FLOAT, nullable=False),
    Column("close", FLOAT, nullable=False),
    Column("volume", FLOAT, nullable=False),
    Column("quote_asset_volume", FLOAT, nullable=False),
    Column("number_of_trade", INTEGER, nullable=False),
    Column("taker_buy_base_asset_volume", FLOAT, nullable=False),
    Column("taker_buy_quote_asset_volume", FLOAT, nullable=False),
)


KLINE_TABLES = {tf.name: KlineTable(tf.name.lower()) for tf in TimeFrame}


@mapper_registry.mapped
@define(slots=False, order=True)
class Pair:
    __table__ = Table(
        "market_pair",
        mapper_registry.metadata,
        Column("id", SMALLINT, primary_key=True),
        Column("name", String(20), unique=True, nullable=False),
    )
    id: int = field(init=False)
    name: str = field(converter=str)


# @mapper_registry.mapped
# @define(slots=False)
# class Min1(KlineRecord):
#     __table__ = KlineTable("minute_1")

# @mapper_registry.mapped
# @define(slots=False)
# class Min3(KlineRecord):
#     __table__ = KlineTable("minute_3")


# @mapper_registry.mapped
# @define(slots=False)
# class Min5(KlineRecord):
#     __table__ = KlineTable("minute_5")


# @mapper_registry.mapped
# @define(slots=False)
# class Min15(KlineRecord):
#     __table__ = KlineTable("minute_15")


# @mapper_registry.mapped
# @define(slots=False)
# class Min30(KlineRecord):
#     __table__ = KlineTable("minute_30")


# @mapper_registry.mapped
# @define(slots=False)
# class Hour1(KlineRecord):
#     __table__ = KlineTable("hour_1")


# @mapper_registry.mapped
# @define(slots=False)
# class Hour2(KlineRecord):
#     __table__ = KlineTable("hour_2")


# @mapper_registry.mapped
# @define(slots=False)
# class Hour4(KlineRecord):
#     __table__ = KlineTable("hour_4")


# @mapper_registry.mapped
# @define(slots=False)
# class Hour6(KlineRecord):
#     __table__ = KlineTable("hour_6")


# @mapper_registry.mapped
# @define(slots=False)
# class Hour8(KlineRecord):
#     __table__ = KlineTable("hour_8")


# @mapper_registry.mapped
# @define(slots=False)
# class Hour12(KlineRecord):
#     __table__ = KlineTable("hour_12")


# @mapper_registry.mapped
# @define(slots=False)
# class Day1(KlineRecord):
#     __table__ = KlineTable("day_1")


# @mapper_registry.mapped
# @define(slots=False)
# class Day3(KlineRecord):
#     __table__ = KlineTable("day_3")


# @mapper_registry.mapped
# @define(slots=False)
# class Week1(KlineRecord):
#     __table__ = KlineTable("week_1")


# @mapper_registry.mapped
# @define(slots=False)
# class Month1(KlineRecord):
#     __table__ = KlineTable("month_1")
