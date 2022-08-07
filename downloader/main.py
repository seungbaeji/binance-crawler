from __future__ import annotations
import argparse
import os
import asyncio
from pathlib import Path
from urllib.parse import urlparse
import time
import logging
import logging.config

import aiohttp
import aiofiles
import aiofiles.os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


async def asynchronous_download(sess: aiohttp.ClientSession,
                                url: str,
                                fpath: Path,
                                chunk_size: int = 1024) -> float:
    async with (sess.get(url) as resp, \
                aiofiles.open(fpath, mode='wb') as f):
        assert resp.status == 200
        fsize = resp.headers['Content-Length']
        async for data in resp.content.iter_chunked(chunk_size):
            await f.write(data)
    logger.debug(f'{fpath.name} is downloaded')
    return float(fsize)


async def main(urls: list[str], download_dir: Path, valid_dir: Path):
    total_size = 0
    async with (aiohttp.ClientSession() as sess):
        for i, url in enumerate(urls):
            url = urlparse(url)
            fname = url.path.split('/')[-1]
            
            fpath = Path(os.path.join(valid_dir, fname))
            if await aiofiles.os.path.exists(fpath):
                total_size += await aiofiles.os.path.getsize(fpath)
                continue
            
            fpath = Path(os.path.join(download_dir, fname))
            if await aiofiles.os.path.exists(fpath):
                total_size += await aiofiles.os.path.getsize(fpath)
                continue

            fsize = await asynchronous_download(sess, url.geturl(), fpath)
            total_size += fsize
            
            if (i % 100 == 0) or (i == len(urls) - 1):
                logger.info(f'===== {i/len(urls)*100:.2f}%, {total_size/10**6:.2f} MB is Downloaded =====')


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Historical Data Downloader')
    parser.add_argument('links', type=Path, help='file download links csv')
    parser.add_argument('--download_dir', '-d', type=Path)
    parser.add_argument('--valid_dir', '-v', type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    download_dir = args.download_dir
    valid_dir = args.valid_dir
    dirpath = os.path.dirname(Path(__file__))
    
    if not download_dir:
        download_dir = Path(os.path.join(dirpath, 'download'))
    if not valid_dir:
        valid_dir = Path(os.path.join(dirpath, 'valid'))
    
    for dirpath in [download_dir, valid_dir]:
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

    with open(args.links, 'r') as f:
        urls = f.readlines()
    if not urls:
        raise ValueError('No File Download Links')
    logger.info(f'#{len(urls)} Files will be downloaded')

    try:
        t1 = time.time()
        asyncio.run(main(urls, download_dir, valid_dir))
        print(f'{time.time() - t1:.2f} sec')
    except KeyboardInterrupt:
        logger.info('===== FINISH ======')
