from __future__ import annotations
import re
import dataclasses as dc
import argparse
from pathlib import Path
import logging
import logging.config
import sqlite3
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

BASE_URL = 'http://data.binance.vision/'


@dc.dataclass
class PageNode:
    url: str
    children: None | list[PageNode] = dc.field(default=None)

    @property
    def name(self) -> str:
        return self.url.split('/')[-2]

    @staticmethod
    def collect_urls(root_page: PageNode) -> list[str]:
        stack, output = [root_page], []
        while stack:
            node = stack.pop()
            output.append(node.url)
            if node.children:
                stack.extend(node.children[::-1])
        return output

    @staticmethod
    def is_filelink(url: str) -> bool:
        if re.search('.zip', url):
            return True
        return False


class Browser:
    max_wait_sec = 10

    def __init__(self, debug: bool = False) -> None:
        self.options = Options()
        if not debug:
            self.options.add_argument('headless')

        self.options.add_experimental_option('detach', True)
        self.service = Service(executable_path=ChromeDriverManager(
            chrome_type=ChromeType.CHROMIUM).install())
        self.driver = webdriver.Chrome(service=self.service,
                                       options=self.options)
        self.tabs = {}

    def open(self, url: str):
        self.driver.get(url)

    def create_tab(self, tabname: str):
        self.driver.execute_script(f'''window.open('', '{tabname}');''')
        tab_id = self.driver.window_handles[-1]
        self.tabs.update({tabname: tab_id})

    def open_tab(self, tabname: str):
        '''It opens a new tab and move to it
        '''
        if tabname not in self.tabs:
            self.create_tab(tabname)
        self.driver.switch_to.window(self.tabs[tabname])

    def crawl_links(self, url: str) -> list[str]:
        '''It crawls links at the page except the link to a prev page
        '''
        self.driver.get(url)

        wait(self.driver, self.max_wait_sec).until(\
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#listing a')))
        links = self.driver.find_elements(By.CSS_SELECTOR, '#listing a')
        links = list(map(lambda x: x.get_attribute('href'), links))
        return links[1:]  # remove a link for a prev page

    def crawl_pages(self, page: PageNode) -> PageNode:
        '''It crawls pages recursively until faces a file download link
        '''
        logger.info(f'Crawl {page.url}')
        links = self.crawl_links(page.url)
        page.children = [PageNode(link) for link in links]
        for child in page.children:
            if PageNode.is_filelink(child.url):
                continue
            self.crawl_pages(child)
        return page

    def close(self):
        self.driver.close()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Crawler for Binance Historical Data Links')
    parser.add_argument('--prefix',
                        '-p',
                        help='prefix url to crawl',
                        default='data/futures/um/monthly/klines')
    parser.add_argument('--link_fpath',
                        '-f',
                        help='filepath for save links',
                        type=Path,
                        default='links.csv')
    parser.add_argument('--debug', action='store_true')
    return parser.parse_args()


def main(args: argparse.Namespace):
    browser = Browser(debug=args.debug)
    url = f'{BASE_URL}?prefix={args.prefix}'
    root_page = PageNode(url)

    try:
        browser.open(url)
        browser.crawl_pages(root_page)
    finally:
        browser.close()

    urls = PageNode.collect_urls(root_page)
    urls = list(filter(PageNode.is_filelink, urls))

    with open(args.link_fpath, 'w') as f:
        for url in urls:
            f.write(url)
            f.write('\n')


if __name__ == '__main__':
    args = get_args()
    main(args)
