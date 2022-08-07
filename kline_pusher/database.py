from __future__ import annotations
import os
import logging.config
import logging
from pathlib import Path
from typing import Iterable
from contextlib import contextmanager

from attr import define, field, astuple, asdict
from sqlalchemy.engine import Engine, create_engine as _create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_engine: Engine | None = None


@define
class DBConfig:
    username: str = field(converter=str)
    password: str = field(converter=str)
    host: str = field(converter=str)
    port: int = field(converter=int)
    database: str = field(converter=str)

    @staticmethod
    def from_env() -> DBConfig:
        return DBConfig(
            username=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
        )

    def astuple(self):
        return astuple(self)

    def asdict(self):
        return asdict(self)


def create_engine(
    username: str,
    password: str,
    host: str,
    port: int,
    database: str,
    pool_size: int = 5,
    max_overflow: int = 10,
    ssl_cert_folder: Path | None = None,
    **kwargs,
) -> Engine:

    url = URL.create(
        drivername="mysql+pymysql",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    connect_args = {"charset": "utf8mb4", "binary_prefix": True}
    if ssl_cert_folder:
        connect_args["ssl"] = {
            "cert": os.path.join(ssl_cert_folder, "client-cert.pem"),
            "key": os.path.join(ssl_cert_folder, "client-key.pem"),
            "ca": os.path.join(ssl_cert_folder, "server-ca.pem"),
            "check_hostname": False,
        }

    return _create_engine(
        url,
        connect_args=connect_args,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        **kwargs,
    )


def on_startup(db_config: DBConfig, *args, **kwargs):
    global _engine
    _engine = create_engine(
        *db_config.astuple(),
        *args,
        **kwargs,
    )
    logger.info("DB Connection is opend")


def on_shutdown():
    global _engine
    if _engine:
        _engine.dispose()
    logger.info("DB Connection is closed")


def get_engine() -> Engine:
    assert _engine is not None
    return _engine


@contextmanager
def get_session(*args, **kwargs) -> Iterable[Session]:
    sess = Session(bind=get_engine(), *args, **kwargs)
    try:
        yield sess
    finally:
        sess.close()
