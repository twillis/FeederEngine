"""
the crawler takes a url and retrieves the data, may need to be smart
about fetching by doing a head request first, or responding to a 302
"""
import unittest
from webob import Request, Response
import os
from mimetypes import guess_type
here = here = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(here, "data")
RSS_PATH = os.path.join(data_dir, "rss.xml")


def mime_type(filename):
    return guess_type(filename)[0]


def mock_rss_server(environ, start_response):
    return Response(app_iter=open(RSS_PATH),
                    content_type=mime_type(RSS_PATH))(environ, start_response)


class TestCrawler(unittest.TestCase):
    def testCrawlURL(self):
        URL = "http://blog.sadphaeton.com/rss.xml"
        res = Request.blank(URL).get_response(mock_rss_server)
        self.assert_(res.status_int == 200)
        self.assert_(res.body.startswith("<?xml version=\"1.0\" encoding=\"utf-8\"?>"), res.body)
        print str(res)
