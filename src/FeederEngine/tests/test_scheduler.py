"""
scheduler tests
"""
import unittest
from feederengine import scheduler
from sqlalchemy import create_engine
import transaction
import datetime
import uuid

engine = create_engine('sqlite:///:memory:', echo=True)


def setUp(self):
    """db setup"""

    scheduler.DBSession.configure(bind=engine)
    scheduler.Base.metadata.create_all(engine)


def tearDown(self):
    """db teardown"""
    scheduler.Base.metadata.drop_all(engine)


class TestCrawlJobModel(unittest.TestCase):
    def setUp(self):
        setUp(self)

    def tearDown(self):
        tearDown(self)

    def testCrawlJobsScheduledChecked(self):
        """
        tests out the mark_job_scheduled and mark_job_checked logic
        """
        urls = [u"http://feeds.feedburner.com/43folders",
                u"http://advocacy.python.org/podcasts/littlebit.rss",
                u"http://friendfeed.com/alawrence?format=atom",
                u"http://feeds.feedburner.com/antiwar"]

        # indicate url is scheduled to be crawled
        with transaction.manager:
            for url in urls:
                rec = scheduler.mark_job_scheduled(url)
                self.assert_(rec, "no rec for url %s" % url)

        recs = [r for r in \
                scheduler.DBSession.query(scheduler.CrawlJobModel).all()]
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


class TestScheduler(unittest.TestCase):
    def make_data(self):
        """
        setup data
        """
        urls = [u"http://feeds.feedburner.com/43folders",
                u"http://advocacy.python.org/podcasts/littlebit.rss",
                u"http://friendfeed.com/alawrence?format=atom",
                u"http://feeds.feedburner.com/antiwar"]
        recs = []
        # indicate urls are scheduled
        with transaction.manager:
            for url in urls:
                recs.append(scheduler.mark_job_scheduled(url))

        return recs

    def scheduled_backdate_recs(self, recs, ago_minutes=5):
        """helpers so we don't have to wait 5 minutes"""
        ago_date = datetime.datetime.now() - datetime.timedelta(minutes=ago_minutes)
        with transaction.manager:
            for rec in recs:
                rec.last_scheduled = ago_date
                scheduler.DBSession.add(rec)

    def checked_backdate_recs(self, recs, ago_minutes=5):
        """helpers so we don't have to wait 5 minutes"""
        ago_date = datetime.datetime.now() - datetime.timedelta(minutes=ago_minutes)
        with transaction.manager:
            for rec in recs:
                rec.last_checked = ago_date
                scheduler.DBSession.add(rec)

    def setUp(self):
        setUp(self)

    def tearDown(self):
        tearDown(self)

    def testGetCrawlJobs(self):
        """
        test get_crawl_jobs logic

        a query that returns a list of urls that should be crawled.

        the list is determined by the last time it was checked if at
        all and whether it is currently scheduled to be crawled

        the goal is to crawl a url at most 5 minutes
        """
        recs = self.make_data()

        #stuff is scheduled, backdate scheduled so we have the right
        #conditions
        self.scheduled_backdate_recs(recs, 10)

        # at this point we should get 0 results because no job has
        # been marked checked
        result = [r for r in scheduler.get_crawl_jobs()]
        self.assert_(len(result) == 0)

        # if we mark one checked and backdate it to 7 minutes ago we
        # should get results of 1
        self.checked_backdate_recs([recs[0], ], 7)
        result = [r for r in scheduler.get_crawl_jobs()]
        self.assert_(len(result) == 1)

    def testScheduleExisting(self):
        """
        coverage
        schedule a url that already exists in the db as if it's
        already been scheduled/checked once
        """
        recs = self.make_data()
        self.scheduled_backdate_recs(recs, 60)
        self.checked_backdate_recs(recs, 50)

        with transaction.manager:
            rec = scheduler.DBSession.merge(recs[0])
            url = rec.url
            newrec = scheduler.mark_job_scheduled(url)
            scheduler.DBSession.add(newrec)

    def testAssumeChecked(self):
        """
        coverage
        if url comes in that hasn't been scheduled, assume it was
        checked and just persist/return the record
        """
        with transaction.manager:
            newrec = scheduler.mark_job_checked("http://gooogle.com")
            scheduler.DBSession.add(newrec)
            self.assert_(newrec)
