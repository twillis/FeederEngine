"""
scheduler tests
"""
import unittest
from feederengine import scheduler
from sqlalchemy import create_engine
import transaction
import datetime
import uuid
import time

def make_engine():
    engine = create_engine('sqlite:///:memory:', echo=True)
    return engine


def setUp(self):
    """need db setup"""
    self.engine = make_engine()
    scheduler.DBSession.configure(bind=self.engine)
    scheduler.Base.metadata.create_all(self.engine)


def tearDown(self):
    """need db teardown"""
    scheduler.Base.metadata.drop_all(self.engine)


class TestCrawlJobModel(unittest.TestCase):
    def setUp(self):
        setUp(self)

    def tearDown(self):
        tearDown(self)

    def testGetCrawlJobs(self):
        """
        does it run without error?
        """
        urls = [u"http://feeds.feedburner.com/43folders",
                u"http://advocacy.python.org/podcasts/littlebit.rss",
                u"http://friendfeed.com/alawrence?format=atom",
                u"http://feeds.feedburner.com/antiwar"]

        with transaction.manager:
            for url in urls:
                rec = scheduler.mark_job_scheduled(url)
                self.assert_(rec, "no rec for url %s" % url)

        recs = [r for r in scheduler.DBSession.query(scheduler.CrawlJobModel).all()]
        self.assert_(len(recs) == len(urls), (len(recs), len(urls)))

        # pretend we crawled the url and update the record
        with transaction.manager:
            etag = str(uuid.uuid4())
            last_modified = datetime.datetime.now()
            rec = scheduler.mark_job_checked(url,
                                             etag=etag,
                                             last_modified=last_modified)
            self.assert_(rec, "no rec for url %s" % url)
            self.assert_(etag == rec.etag)
            self.assert_(last_modified == rec.last_modified)
        time.sleep(2)
        recs = [r for r in scheduler.get_crawl_jobs(threshold_minutes=5)]
        data = [dict(url=r.url, last_scheduled=r.last_scheduled, last_checked=r.last_checked) for r in recs]
        self.assert_(len(recs) == len(urls) - 1, (len(recs), len(urls), data))
