from __future__ import annotations
import time
import argparse
import logging
import logging.config
import glob
import csv
import io
import os
import re
import itertools
import sys
from functools import partial
from zipfile import ZipFile, BadZipFile
from pathlib import Path, PurePath
from concurrent.futures import ProcessPoolExecutor
from typing import Iterator, NamedTuple
from traceback import print_exc

from dotenv import load_dotenv

import database
import crud
import datamodel
from datamodel import (
    KlineZipFile,
    KlineRecord,
    TimeFrame,
    Pair,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def get_pairs(dirpath: Path) -> list[Pair]:
    get_pair = lambda x: x.split("-")[0].split("_")[0]
    paths = map(PurePath, glob.glob(os.path.join(dirpath, "*")))
    names = map(lambda x: x.name, paths)
    pairs = set(map(get_pair, names))
    return sorted(map(Pair, pairs))


class JobParam(NamedTuple):
    timeframe: TimeFrame
    pair: Pair

    @staticmethod
    def get_params(dirpath: Path) -> Iterator[JobParam]:
        pairs = get_pairs(dirpath)
        params = itertools.product(TimeFrame, pairs)
        return map(lambda param: JobParam(*param), params)


class KlineParser:
    def __init__(self, pairs: list[Pair]) -> None:
        self.pairs = {pair.name: pair.id for pair in pairs}

    def get_zipfiles(
        self,
        dirpath: Path,
        timeframe: TimeFrame | None = None,
        pair: Pair | None = None,
    ) -> list[KlineZipFile]:
        if not timeframe and not pair:
            return os.listdir(dirpath)

        ptrn = ""
        if timeframe:
            ptrn += f"(?=.*{timeframe.value}-)"
        if pair:
            ptrn += f"(?=.*{pair.name.upper()}-)"
        r = re.compile(ptrn)
        paths = filter(r.match, os.listdir(dirpath))
        paths = map(partial(os.path.join, dirpath), paths)
        return [KlineZipFile(path, timeframe, pair) for path in map(PurePath, paths)]

    def zipfile2records(self, zipfile: KlineZipFile) -> list[KlineRecord]:
        try:
            with ZipFile(zipfile.fpath, mode="r") as zip:
                with zip.open(f"{zipfile.fpath.stem}.csv", mode="r") as f:
                    pid = zipfile.pair.id
                    pid = pid if pid else self.pairs[zipfile.pair.name]
                    reader = csv.reader(io.TextIOWrapper(f))
                    rows = [KlineRecord(pid, *row) for row in reader]
            return rows
        except BadZipFile:
            logger.info(f"{zipfile} is corrupted")
            return []

    def parse_files(
        self, dirpath: Path, timeframe: TimeFrame, pair: Pair
    ) -> list[KlineRecord]:
        fpaths = self.get_zipfiles(dirpath, timeframe, pair)
        logger.info(f"#{len(fpaths)} files are loaded")

        records = list(itertools.chain(*map(self.zipfile2records, fpaths)))
        logger.info(f"#{len(records)} records")
        return records


def main(dirpath: Path, max_jobs: int):

    pairs = get_pairs(dirpath)
    logger.info(f"#{len(pairs)} MarketPairs are loaded")

    with database.get_session() as sess:
        try:
            crud.MarketPair.create(sess, pairs)
            logger.info(f"#{len(pairs)} MarketPairs are inserted")
            sess.commit()
        except:
            sess.rollback()
            return

    with database.get_session() as sess:
        pairs = crud.MarketPair.read_all(sess)

    parser = KlineParser(pairs)

    by_time = lambda x: x.timeframe  # tables are managed by timeframe
    for timeframe, params in itertools.groupby(JobParam.get_params(dirpath), by_time):
        logger.info(f"handle `{timeframe.name}` timeframe data")

        funcs = [partial(parser.parse_files, dirpath, *param) for param in params]
        while funcs:
            num_jobs = min(max_jobs, len(funcs))
            jobs = [funcs.pop(0) for _ in range(num_jobs)]
            logger.info(f"#{len(jobs)} jobs are created")

            # with ProcessPoolExecutor(4) as executor:
            #     futures = [executor.submit(job) for job in jobs]
            # results = [future.result() for future in futures]

            results = [job() for job in jobs]
            records = list(itertools.chain(*results))
            logger.info(f"#{len(records)} will be inserted")

            table = timeframe.get_mapped_table()
            with database.get_engine().begin() as conn:
                try:
                    crud.KlineTable.insert_bulk(conn, table, records)
                    conn.commit()
                except Exception as e:
                    print_exc()
                    conn.rollback()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Historical Data Pusher")
    parser.add_argument("dirpath", help="Historical Data Dir Path")
    parser.add_argument(
        "--max_jobs",
        type=int,
        default=4,
        help="the number of maximum jobs of each loop",
    )
    parser.add_argument("--env", help="env filepath", default=".env")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    load_dotenv(args.env)

    database.on_startup(database.DBConfig.from_env(), future=True)
    datamodel.drop_tables()
    datamodel.create_tables()

    start = time.perf_counter()
    main(args.dirpath, args.max_jobs)
    finish = time.perf_counter()
    print(finish - start)

    database.on_shutdown()
