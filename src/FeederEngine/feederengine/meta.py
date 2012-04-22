"""
stuff for initializing the database
"""
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from zope.sqlalchemy import ZopeTransactionExtension
from sqlalchemy.ext.declarative import declarative_base

import logging
log = logging.getLogger(__name__)

# set this for the engine factory to produce something
db_url = None

# set this with the result of session_factory to initialize
Session = None

Base = declarative_base()


def engine_factory():
    global db_url
    if db_url:
        log.info("engine being created with url %s" % db_url)
        return create_engine(db_url, echo=True)
    else:
        log.warn("db_url is not set")
        return None


def session_factory(engine=None):
    e = engine or engine_factory()
    if not e:
        log.warn("Session being created without an engine, will need to be configured before using")
    return scoped_session(sessionmaker(bind=e,
                                       extension=ZopeTransactionExtension()))
