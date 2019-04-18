"""
use beautifulsoup to extract urls
"""
from bs4 import BeautifulSoup, SoupStrainer


def extract_links_iter(body):
    for link in BeautifulSoup(body, parseOnlyThese=SoupStrainer('a')):
        if "href" in link:
            yield link['href']
