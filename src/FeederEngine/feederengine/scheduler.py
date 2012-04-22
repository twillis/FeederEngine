"""
given a table of urls and other information, decides when to schedule
a crawler to crawl for updates
"""
from sqlalchemy import Column, UnicodeText, String, DateTime, or_, and_
import meta
import datetime

DEFAULT_JOB_COUNT = 10


class CrawlJobModel(meta.Base):
    """
    let's start with models just being the representation of a row and
    not too much behavior and see how that goes.
    """
    __tablename__ = "crawl_jobs"
    url = Column(UnicodeText, unique=True, primary_key=True,
                 nullable=False)
    etag = Column(String(1024))
    last_modified = Column(DateTime, index=True)
    last_scheduled = Column(DateTime, index=True)
    last_checked = Column(DateTime, index=True)

    # describe the various states for sql
    state_never_scheduled = last_scheduled == None
    state_scheduled = last_scheduled != None
    state_in_process = last_scheduled > last_checked
    state_not_in_process = and_(state_scheduled,
                                last_scheduled < last_checked)
    state_checked = last_checked != None
    state_never_checked = last_checked == None

    @classmethod
    def state_not_just_checked(cls, ago=5):
        threshold = datetime.datetime.now() - datetime.timedelta(minutes=ago)
        return cls.last_checked < threshold


def get_for_url(url):
    DBSession = meta.Session()
    return DBSession.query(CrawlJobModel)\
           .filter(CrawlJobModel.url == url).first()


def mark_job_scheduled(url):
    DBSession = meta.Session()
    rec = get_for_url(url)
    if rec:
        rec.last_scheduled = datetime.datetime.now()
        return rec
    else:
        rec = CrawlJobModel(url=url,
                            last_scheduled=datetime.datetime.now())
        DBSession.add(rec)
        return rec


def mark_job_checked(url, etag=None, last_modified=None):
    rec = get_for_url(url)
    if rec:
        rec.last_checked = datetime.datetime.now()
        rec.etag = etag
        rec.last_modified = last_modified
        return rec
    else:
        # or to do imports, just accept the data, and set
        # last_scheduled = last_checked
        now = datetime.datetime.now()
        return CrawlJobModel(url=url,
                             last_scheduled=now,
                             last_checked=now,
                             etag=etag,
                             last_modified=last_modified)


def get_crawl_jobs(count=DEFAULT_JOB_COUNT, threshold_minutes=5):
    """
    get list of urls to crawl based on last_scheduled and last_checked

    count: number of records to return threshold_minutes: # of
    minutes threshold to determine whether it's time to schedule
    the job
    """
    # select * from
    DBSession = meta.Session()
    q = DBSession.query(CrawlJobModel)
    return q.filter(or_(CrawlJobModel.state_not_just_checked(threshold_minutes),
                        CrawlJobModel.state_never_scheduled,
                        CrawlJobModel.state_not_in_process))\
        .order_by(CrawlJobModel.last_checked)
