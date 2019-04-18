"""
implementations of the various worker processes
"""
import logging
from .process.base import KillableProcess
import contextlib
import zmq
import json
import time
from .crawler import crawl_url
from . import indexer
import datetime
log = logging.getLogger(__name__)


@contextlib.contextmanager
def push_socket(context, bind):
    """
    push instead of publish because we are building up and potentially
    distributing work not distributing messages (a queue in other
    words)
    """
    socket = context.socket(zmq.PUSH)
    socket.bind(bind)
    yield socket
    socket.close()


@contextlib.contextmanager
def pull_socket(context, bind):
    """
    pull instead of subscribe, because we are grabbing work to do
    """
    socket = context.socket(zmq.PULL)
    socket.connect(bind)
    yield socket
    socket.close()


class SchedulerWorker(KillableProcess):
    """
    periodically call get_crawl_jobs

    for each job in get_crawl_jobs:
        msg = json.dumps(job)
        socket.send(msg)
        mark_job_scheduled(job.url)
    """
    def __init__(self, db_url, send_bind):
        KillableProcess.__init__(self)
        self._db_url = db_url
        self._bind = send_bind
        self.log = logging.getLogger(self.__class__.__name__)

    def work(self, should_continue):
        if not should_continue():
            return

        from . import meta
        from . import scheduler
        meta.db_url = self._db_url
        context = zmq.Context()
        poller = zmq.Poller()

        with push_socket(context, self._bind) as publish:
            poller.register(publish, zmq.POLLOUT)
            while should_continue():
                self.log.debug("running...")
                polled = dict(poller.poll(timeout=100))
                if publish in polled and polled[publish] == zmq.POLLOUT:
                    for r in scheduler.get_crawl_jobs():
                        url, msg = str(r.url), json.dumps(dict(url=r.url,
                                              etag=r.etag,
                                              last_modified=r.last_modified))
                        publish.send_multipart([url, msg])
                        self.log.info((url, msg))
                        scheduler.mark_job_scheduled(r.url)
                    else:
                        self.log.info("nothing to do, waiting......")

            else:
                self.log.debug("cleaning up....")


class CrawlWorker(KillableProcess):
    """
    pulls msgs off a queue
    job = json.loads(in_socket.recv())
    res = crawl_url(job.url, job.etag, job.last_modified)
    out_socket.send(str(res), job)
    """
    def __init__(self, from_bind, to_bind):
        KillableProcess.__init__(self)
        self._from_bind = from_bind
        self._to_bind = to_bind
        self.log = logging.getLogger(self.__class__.__name__)

    def work(self, should_continue):
        if not should_continue():
            return

        context = zmq.Context()
        with pull_socket(context, self._from_bind) as source, \
                 push_socket(context, self._to_bind) as destination:
            poller = zmq.Poller()
            poller.register(source, zmq.POLLIN)
            poller.register(destination, zmq.POLLOUT)

            while should_continue():
                url, data = None, None
                presults = dict(poller.poll(timeout=10))
                if len(presults) == 2:
                    try:
                        url, data = source.recv_multipart(zmq.NOBLOCK)
                        self.log.info([url, data])
                    except zmq.ZMQError as ze:
                        self.log.error("something bad happened maybe\n\n %s" % str(ze))

                    if url and data:
                        response = None
                        try:
                            data = json.loads(data)
                            response = crawl_url(url=url,
                                                 etag=data["etag"],
                                                 last_modified=data["last_modified"])
                            if response:
                                self.log.info("got response for %s" % url)
                        except Exception:
                            self.log.error("could not crawl %s" % url)

                        if response:
                            try:
                                self.log.info("sending to destination....")
                                destination.send_multipart([url, str(response)])
                            except zmq.ZMQError as ze:
                                self.log.error("could not send result of crawl of %s \n\n %s" % (url, str(ze)))
                    self.log.info("did something...")
                    self.log.info([presults, source in presults, destination in presults])


class IndexWorker(KillableProcess):
    """
    pulls job off queue
    indexes response body[tokenize, stem, other]
    saves to db
    """
    def __init__(self, from_bind, db_url):
        KillableProcess.__init__(self)
        self._from = from_bind
        self._db_url = db_url
        self.log = logging.getLogger(self.__class__.__name__)

    def work(self, should_continue):
        self.log.debug("ready to work")
        if not should_continue():
            return

        from . import meta
        import transaction
        meta.db_url = self._db_url
        context = zmq.Context()
        poller = zmq.Poller()

        with pull_socket(context, self._from) as source:
            self.log.debug("has socket")
            poller = zmq.Poller()
            poller.register(source, zmq.POLLIN)

            while should_continue():
                self.log.debug("polling socket")
                presults = dict(poller.poll(timeout=10))
                if source in presults:
                    self.log.debug("got work to do")
                    url, response = source.recv_multipart(zmq.NOBLOCK)
                    if indexer.needs_indexed(url):
                        self.log.debug("indexing url %s" % url)
                        index_list = list(indexer.index_page_iter(url, response))
                        # persist to database
                        if self.log.isEnabledFor("DEBUG"):
                            self.log.debug("indexing complete for url %s" % url)
                            for entry in index_list:
                                self.log.debug("url: %(url)s\n\nstem: %(stem)s (%(frequency)s)" % entry)
                        with transaction.manager:
                            indexer.add_entries(index_list)


class UpdateWorker(object):
    """
    pulls job off queue
    calls mark_job_checked()
    """
    pass
