"""
the crawler takes a url and retrieves the data, may need to be smart
about fetching by doing a head request first, or responding to a 302
"""
import unittest
import logging

logging.basicConfig(level="DEBUG")


class TestCrawler(unittest.TestCase):
    def testCrawlWorker(self):

        from feederengine import crawler

        urls = ["http://www.reddit.com/r/Python/",
                "http://slashdot.org",
                "http://news.ycombinator.com/"]
        workers = {}
        for url in urls:
            workers[url] = crawler.crawl(url=url)

        results = [(k, str(w())) for k, w in workers.items()]

        print str(results)

    def testCrawlerFail(self):
        from feederengine import crawler

        try:
            str(crawler.crawl(url="http://1.1.1.1")(.01))
            self.fail() #pragma no cover
        except:
            pass

    def testCrawlerFailGettingResponse(self):
        """
        strictly for test coverage
        """
        from feederengine import crawler

        def err_app(environ, start_response):
            raise Exception("fuuuuuuuu")
        crawler.proxy = err_app
        try:
            str(crawler.crawl(url="http://1.1.1.1")(.01))
            self.fail() #pragma no cover
        except:
            pass

    def testCrawlerCantCopyRequest(self):
        """
        strictly for test coverage
        """
        from feederengine import crawler
        t = crawler._CrawlerThread(url="")
        t._response = 1
        try:
            t.response
            self.fail()
        except:
            pass

