"""
the crawler takes a url and retrieves the data, may need to be smart
about fetching by doing a head request first, or responding to a 302
"""
import unittest
from webob import Request, Response
import os
from mimetypes import guess_type
import md5
from feederengine.crawler import crawl
import logging

logging.basicConfig(level="DEBUG")

here = here = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(here, "data")
RSS_PATH = os.path.join(data_dir, "rss.xml")


def mime_type(filename):
    """
    use std lib to guess mimetype
    """
    return guess_type(filename)[0]


def calculate_etag(path):
    """
    etag = md5 hash pf (path + size + last-modified-time)
    """
    m = md5.new(path)
    stat = os.stat(path)
    size, mtime = stat[6], stat[8]
    m.update(str(size))
    m.update(str(mtime))
    return m.hexdigest()


def mock_rss_server(environ, start_response):
    """
    server a file with etag in header, if etag matches will return status=304
    """
    request = Request(environ)
    etag = calculate_etag(RSS_PATH)

    if etag in request.if_none_match:
        return Response(status_int=304)(environ, start_response)

    return Response(app_iter=open(RSS_PATH),
                    content_type=mime_type(RSS_PATH),
                    etag=etag)(environ, start_response)


class TestCrawler(unittest.TestCase):
    def testCrawlWorker(self):
        urls = ["http://www.reddit.com/r/Python/",
                "http://slashdot.org",
                "http://news.ycombinator.com/"]
        workers = {}
        for url in urls:
            workers[url] = crawl(url=url)

        results = [(k, str(w())) for k, w in workers.items()]

        print str(results)

    def testCrawlerFail(self):
        try:
            str(crawl(url="http://1.1.1.1")(.01))
            self.fail()
        except:
            pass
