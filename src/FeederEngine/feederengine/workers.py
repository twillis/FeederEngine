"""
implementations of the various worker processes
"""


class SchedulerWorker(object):
    """
    periodically call get_crawl_jobs

    for each job in get_crawl_jobs:
        msg = json.dumps(job)
        socket.send(msg)
        mark_job_scheduled(job.url)
    """
    pass


class CrawlWorker(object):
    """
    pulls msgs off a queue
    job = json.loads(in_socket.recv())
    res = crawl_url(job.url, job.etag, job.last_modified)
    out_socket.send(str(res), job)
    """
    pass


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
