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
import transaction
import uuid
import os
import logging
import zmq
from utils import mock, mock_rss_server

__here__ = os.path.abspath(os.path.dirname(__name__))
logging.basicConfig(level="INFO")
log = logging.getLogger(__name__)


class TestKillableProcess(unittest.TestCase):
    def test_start(self):

        w = KillableProcess()
        w.start()
        self.assert_(w.is_alive())
        w.terminate()
        time.sleep(.01)
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

        # subscription = "http://feeds.feedburner.com/antiwar"
        log.info("telling worker to use database %s" % self.db_url)
        scheduler_bind = "tcp://127.0.0.1:10000"
        crawl_bind = "tcp://127.0.0.1:10001"
        from feederengine import crawler
        with mock(crawler, "proxy", mock_rss_server):
            w = SchedulerWorker(self.db_url, scheduler_bind)
            c = CrawlWorker(scheduler_bind, crawl_bind)

            w.start()
            c.start()

            self.assert_(w.is_alive())
            self.assert_(c.is_alive())

            context = zmq.Context()

            with pull_socket(context, crawl_bind) as subscription:
                count = 0
                tries = 0
                poller = zmq.Poller()
                poller.register(subscription, zmq.POLLIN)
                while count < len(urls) and tries < 100:
                    polled = dict(poller.poll(timeout=100))
                    if subscription in polled and polled[subscription] == zmq.POLLIN:
                        try:
                            url, data = subscription.recv_multipart(zmq.NOBLOCK)
                            count += 1
                        except zmq.ZMQError:
                            log.error("timeout", exc_info=True)
                            time.sleep(.1)
                        else:
                            log.info(data)
                    tries += 1
                    log.info("tries %s and results %s" % (tries, count))

            [w.terminate(), c.terminate()]
            time.sleep(1)
            self.assert_(not w.is_alive())
            self.assert_(not c.is_alive())
            self.assert_(count == len(urls), "didn't get all expected messages")
