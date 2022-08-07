from sqlalchemy import select, insert, join, delete, update
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from .datamodel import KlineRecord, Pair, Table


class MarketPair:
    @classmethod
    def create(cls, sess: Session, pair: Pair | list[Pair]) -> bool:
        if isinstance(pair, list):
            sess.add_all(pair)
        else:
            sess.add(pair)

        try:
            sess.flush()
        except:
            sess.expire_all()

    @classmethod
    def read(cls, sess: Session, name: str) -> Pair:
        stmt = select(Pair).where(Pair.name == name)
        return sess.scalar(stmt)

    @classmethod
    def read_all(cls, sess: Session) -> list[Pair]:
        return sess.scalars(select(Pair)).all()


class KlineTable:
    @classmethod
    def insert_bulk(cls, conn: Connection, table: Table, records: list[KlineRecord]):
        data = [record.asdict() for record in records]
        conn.execute(table.insert(), data)
