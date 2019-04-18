"""
very very crude page indexer

the goal is to get stems and the frequency on the page to result in a
db row that looks like

[stem] | [url] | domain | frequency | index_date
"""
import nltk
from nltk.stem import snowball
from urllib import parse as urlparse
from . import meta
from sqlalchemy import PrimaryKeyConstraint, Table, Column, Unicode, UnicodeText, String, DateTime, Integer, or_, and_
import datetime


def index_page_iter(url, body):
    body = nltk.clean_html(body)
    tokens = nltk.word_tokenize(body)
    domain = urlparse.urlparse(url).hostname
    stems = [get_stemmer(body).stem(w.decode("utf-8").lower()) for w in tokens]
    stem_freq = {}
    for stem in stems:
        if not stem in stem_freq:
            stem_freq[stem] = 1
        else:
            stem_freq[stem] += 1

    for k, v in stem_freq.items():
        yield dict(stem=k, url=url, domain=domain, frequency=v)


def get_stemmer(body):
    return snowball.EnglishStemmer(ignore_stopwords=True)


class IndexEntry(meta.Base):
    __tablename__ = "index_entries"
    domain = Column(Unicode)
    frequency = Column(Integer)
    index_date = Column(DateTime, default=datetime.datetime.now)
    stem = Column(Unicode)
    url = Column(Unicode)
    __table_args__ = (PrimaryKeyConstraint("stem", "url"), {})


def add_entries(entries):
    ENTRY_COLUMNS = ["domain", "frequency", "stem", "url"]
    DBSession = meta.Session()

    for entry in entries:
        args = {k.lower(): v for k, v in entry.items() \
                if k.lower() in ENTRY_COLUMNS}
        DBSession.add(IndexEntry(**args))


def needs_indexed(url, threshold=10):
    dt_threshold = datetime.datetime.now() - datetime.timedelta(minutes=threshold)
    DBSession = meta.Session()
    return DBSession.query(IndexEntry)\
           .filter(IndexEntry.url == url)\
           .filter(IndexEntry.index_date >= dt_threshold).count() == 0
