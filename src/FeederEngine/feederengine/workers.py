"""
implementations of the various worker processes
"""
import logging
from process.base import KillableProcess
import contextlib
import zmq
import json
import time
from crawler import crawl_url

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
    def __init__(self, db_url, send_bind):  #, unit_test=False):
        KillableProcess.__init__(self)
        self._db_url = db_url
        self._bind = send_bind
        self.log = logging.getLogger(self.__class__.__name__)
        # self._unit_test = unit_test

    def work(self, should_continue):
        import meta
        import scheduler
        meta.db_url = self._db_url
        context = zmq.Context()
        with push_socket(context, self._bind) as publish:
            # if self._unit_test:
            #     time.sleep(5) # wait for client to connect... FIX
            while should_continue():
                self.log.debug("running...")
                for r in scheduler.get_crawl_jobs():
                    url, msg = str(r.url), json.dumps(dict(url=r.url,
                                          etag=r.etag,
                                          last_modified=r.last_modified))
                    publish.send_multipart([url, msg])
                    scheduler.mark_job_scheduled(r.url)
                else:
                    self.log.info("nothing to do, waiting......")
                    time.sleep(1)  # debug
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
        with pull_socket(context, self._from_bind) as source:
            with push_socket(context, self._to_bind) as destination:
                while should_continue():
                    url, data = None, None
                    try:
                        url, data = source.recv_multipart(zmq.NOBLOCK)
                    except zmq.ZMQError as ze:
                        log.error("something bad happened maybe\n\n %s" % str(ze), exc_info=True)
                        time.sleep(1)

                    if url and data:
                        response = None
                        try:
                            data = json.loads(data)
                            response = crawl_url(url=url,
                                                 etag=data["etag"],
                                                 last_modified=data["last_modified"])
                        except Exception:
                            log.error("could not crawl %s" % url, exc_info=True)

                        if response:
                            try:
                                destination.send_multipart([url, str(response)],
                                                           flags=zmq.NOBLOCK)
                            except zmq.ZMQError as ze:
                                log.error("could not send result of crawl of %s \n\n %s" % (url, str(ze)),
                                          exc_info=True)


class IndexWorker(object):
    """
    pulls job off queue
    indexes response body[tokenize, stem, other]
    """
    pass


class UpdateWorker(object):
    """
    pulls job off queue
    calls mark_job_checked()
    """
    pass
