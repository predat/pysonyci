# coding: utf8

import logging
from .version import __version__


__title__ = 'pysonyci'
__author__ = 'Sylvain Maziere'
__license__ = 'MIT'
__copyright__ = 'Copyright 2016 Sylvain Maziere'

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

from .sonyci import SonyCi
