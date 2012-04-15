"""
the crawler represents a unit of work to be completed.

the unit of work is "go get this document if it has been updated"
"""
from paste.proxy import TransparentProxy
from webob import Request
import threading
import logging
log = logging.getLogger(__name__)


# forwards request to specified url
proxy = TransparentProxy()


class _CrawlerThread(threading.Thread):
    """
    thread to fetch a response, when completed, the response is
    accesible via the response property
    """
    _response = None

    def __init__(self, url, etag=None, last_modified=None):
        self._url = url
        self._etag = etag
        self._last_modified = last_modified
        super(_CrawlerThread, self).__init__()
        self.daemon = True

    def run(self):
        # filter out params equal to None
        extra_args = {k: v for k, v in dict(etag=self._etag,
                                            if_modified_since=self._last_modified).items() if v}
        request = Request.blank(self._url, **extra_args)


        self._response = request.get_response(proxy)
        log.debug("response recieved from %s (%s)" \
                  % (self._url, self._response.status_int))

    @property
    def response(self):
        if self._response:
            response = None
            with threading.Lock() as lock:  # not sure whether this is needed but it's neat
                response = self._response.copy()  # necessary??
            return response
        else:
            return None


def crawl(url, etag=None, last_modified=None):
    """
    kicks off a thread to get a response for the url based on etag and
    last_modified returns a method to call in the future to get the
    response.

    >>> get_response = crawl(url="http://google.com")
    >>> # later on
    >>> response = get_response()
    """
    #create thread
    t = _CrawlerThread(url=url, etag=etag, last_modified=last_modified)

    #closure to return to caller
    def complete(timeout=5.0):
        """
        callback that is returned which in turn can be called to get
        the response
        """
        t.join(timeout)

        if not t.response:
            raise Exception("ERROR: didn't get a response from %s" % url)  # need
                                                                    # a
                                                                    # more
                                                                    # specific
                                                                    # exception
        else:
            return t.response

    # start thread
    t.start()
    return complete  # future
