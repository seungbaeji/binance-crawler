import time
import os
import argparse
import platform
import asyncio
from pathlib import Path, PurePath
import logging
import logging.config

import aiofiles
import aiofiles.os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def split_files(dirpath: Path) -> tuple[set[str], set[str]]:
    files = set(os.listdir(dirpath))
    checksums = set([f for f in files if f.endswith('CHECKSUM')])
    zipfiles = files - checksums
    return checksums, zipfiles


async def read_checksum(fpath: Path) -> tuple[str, str]:
    async with aiofiles.open(fpath, mode='r') as f:
        resp = await f.read()
    return tuple(resp.split())


async def get_checksum(fpath: Path) -> str:
    cmd = 'shasum -a 256' if platform.mac_ver else 'sha256sum'
    cmd += f' {fpath}'
    proc = await asyncio.create_subprocess_shell(cmd,
                                                 stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    if stdout:
        return str(stdout.split()[0], 'utf-8')
    if stderr:
        return str(stderr, 'utf-8')


async def move_validfile(fpath: Path, valid_dirpath: Path):
    valid_fpath = Path(os.path.join(valid_dirpath, fpath.name))
    await aiofiles.os.replace(fpath, valid_fpath)


async def remove_failed(download_dirpath: Path,
                        failed: dict[str, int],
                        threshold: int = 3):
    for fname, cnt in failed.items():
        if cnt >= threshold:
            fpath = Path(os.path.join(download_dirpath, fname))
            await aiofiles.os.remove(fpath)
            logger.info(f'{fname} is invalid, removed')


async def validate(download_dir: Path, valid_dir: Path, failed: dict[str, int]):
    checksums, zipfiles = split_files(download_dir)
    valid_zipfiles = set(os.listdir(valid_dir))
    msg = f'#{len(checksums)} checksums / ' + \
          f'#{len(zipfiles)} zipfiles / ' + \
          f'#{len(valid_zipfiles)} valid zipfiles'
    logger.info(msg)
    if not zipfiles:
        return

    for checksum_fname in checksums:
        if PurePath(checksum_fname).stem in valid_zipfiles:
            continue

        checksum_fpath = Path(os.path.join(download_dir, checksum_fname))
        hash, zipfname = await read_checksum(checksum_fpath)
        if zipfname not in zipfiles:  # it's already validated
            continue

        zipfpath = Path(os.path.join(download_dir, zipfname))
        checksum = await get_checksum(zipfpath)
        if hash == checksum:
            await move_validfile(zipfpath, valid_dir)
            logger.info(f'{zipfname} is valid')
        else:
            cnt = failed[zipfname] if zipfname in failed else 0
            cnt += 1
            failed.update({zipfname: cnt})
            logger.info(f'{zipfname} is failed {hash}/{checksum}')


async def main(download_dir: Path, valid_dir: Path, threshold: int):
    failed = {}
    for _ in range(threshold):
        await validate(download_dir, valid_dir, failed)
        time.sleep(5)
    logger.info(f'Fails: {failed}')
    await remove_failed(download_dir, failed, threshold)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Historical Data Validator')
    parser.add_argument('--download_dir', '-d', type=Path, required=True)
    parser.add_argument('--valid_dir', '-v', type=Path, required=True)
    parser.add_argument('--threshold', '-t', type=int, default=3)
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    if not os.path.exists(args.download_dir) or \
       not os.path.exists(args.valid_dir):
        raise ValueError('Invalid Dirs')

    try:
        t1 = time.time()
        asyncio.run(main(args.download_dir, args.valid_dir, args.threshold))
        print(f'{time.time() - t1:.2f} sec')
    except:
        logger.info('===== FINISH =====')
