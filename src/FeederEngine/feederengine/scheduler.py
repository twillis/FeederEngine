"""
given a table of urls and other information, decides when to schedule
a crawler to crawl for updates
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, UnicodeText, String, DateTime, or_, and_
from sqlalchemy.orm import scoped_session, sessionmaker
from zope.sqlalchemy import ZopeTransactionExtension
import datetime

DEFAULT_JOB_COUNT = 10

Base = declarative_base()

# engine gets set elsewhere
DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))


class CrawlJobModel(Base):
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
        return or_(cls.state_never_checked,
                   and_(cls.state_checked,
                        cls.last_checked < threshold))


def get_for_url(url):
    return DBSession.query(CrawlJobModel)\
           .filter(CrawlJobModel.url == url).first()


def mark_job_scheduled(url):
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
        return None


def get_crawl_jobs(count=DEFAULT_JOB_COUNT, threshold_minutes=5):
    """
    get list of urls to crawl based on last_scheduled and last_checked

    count: number of records to return threshold_minutes: # of
    minutes threshold to determine whether it's time to schedule
    the job
    """
    # select * from
    q = DBSession.query(CrawlJobModel)
    q = q.filter(CrawlJobModel.state_not_just_checked(threshold_minutes)).order_by(CrawlJobModel.last_checked)

    return q
