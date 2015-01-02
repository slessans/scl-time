from distutils.core import setup

__author__ = 'Scott Lessans'


setup(
    name='scl_time',
    py_modules=['scl_time'],
    version='0.9',
    description='A library of utils for working with time intervals and sequences.',
    author='Scott Lessans',
    author_email='scott@scottlessans.com',
    url='https://github.com/slessans/scl-time.git',
    download_url='https://github.com/slessans/scl-time/tarball/0.9',
    requires=['pytz'],
    keywords=['time', 'date', 'datetime', 'intervals'],
)