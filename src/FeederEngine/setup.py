from setuptools import setup, find_packages
import os

here = os.path.dirname(os.path.abspath(__file__))

README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = ["feedparser", "webob"]
version = '0.0'

setup(name='FeederEngine',
      version=version,
      description="Engine for fetching rss feeds",
      long_description=README + "\n\n" + CHANGES,
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Tom Willis',
      author_email='tom.willis@gmail.com',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
