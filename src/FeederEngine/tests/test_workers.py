"""
trying to test these things
"""
import unittest
from feederengine.workers import SchedulerWorker, CrawlWorker, pull_socket
from feederengine.process.base import KillableProcess
from feederengine.scheduler import CrawlJobModel, get_crawl_jobs
from feederengine import meta, scheduler
from sqlalchemy import create_engine
import time
import datetime
import transaction
import uuid
import os
import logging
import zmq
__here__ = os.path.abspath(os.path.dirname(__name__))
logging.basicConfig(level="INFO")
log = logging.getLogger(__name__)


class TestKillableProcess(unittest.TestCase):
    def test_start(self):

        w = KillableProcess()
        w.start()
        time.sleep(1)
        self.assert_(w.is_alive())
        time.sleep(1)
        w.terminate()
        time.sleep(1)
        self.assert_(not w.is_alive())


class TestSchedulerWorker(unittest.TestCase):
    def setUp(self):
        db_url = "sqlite:///%s/%s.db" % (__here__, str(uuid.uuid4()))
        self.engine = create_engine(db_url,
                                    echo=False)
        meta.Session = meta.session_factory(self.engine)
        scheduler.Base.metadata.create_all(self.engine)
        self.db_url = db_url

    def tearDown(self):
        scheduler.Base.metadata.drop_all(self.engine)

    def test_start(self):
        urls = [u"http://feeds.feedburner.com/43folders",
                u"http://advocacy.python.org/podcasts/littlebit.rss",
                u"http://friendfeed.com/alawrence?format=atom",
                u"http://feeds.feedburner.com/antiwar"]
        with transaction.manager:
            for url in urls:
                meta.Session().add(CrawlJobModel(url=url))


        self.assert_(len(list(meta.Session().query(CrawlJobModel).all())))
        self.assert_(len(list(get_crawl_jobs())))
        time.sleep(1)

        # subscription = "http://feeds.feedburner.com/antiwar"
        log.info("telling worker to use database %s" % self.db_url)
        scheduler_bind = "tcp://127.0.0.1:10000"
        crawl_bind = "tcp://127.0.0.1:10001"
        w = SchedulerWorker(self.db_url, scheduler_bind)
        c = CrawlWorker(scheduler_bind, crawl_bind)
        [w.start(), c.start()]
        self.assert_(w.is_alive())
        self.assert_(c.is_alive())
        time.sleep(2)
        context = zmq.Context()
        
        with pull_socket(context, crawl_bind) as subscription:

            for i in xrange(1, 5):
                try:
                    url, data = subscription.recv_multipart(zmq.NOBLOCK)
                except zmq.ZMQError:
                    pass
                else:
                    log.info(data)

        [w.terminate(), c.terminate()]
        time.sleep(.1)
        self.assert_(not w.is_alive())
        self.assert_(not c.is_alive())
