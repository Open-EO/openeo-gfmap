"""OpenEO GFMAP Manager submodule. Implements the logic of splitting the jobs into subjobs and
managing the subjobs.
"""

import logging

_log = logging.getLogger(__name__)

_log.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
_log.addHandler(stream_handler)

formatter = logging.Formatter("%(asctime)s|%(name)s|%(levelname)s:  %(message)s")
stream_handler.setFormatter(formatter)


# Exclude the other loggers from other libraries
class ManagerLoggerFilter(logging.Filter):
    """Filter to only accept the OpenEO-GFMAP manager logs."""

    def filter(self, record):
        return record.name in [_log.name]


stream_handler.addFilter(ManagerLoggerFilter())


def set_log_level(level):
    """Set the log level of the OpenEO-GFMAP manager logger."""
    _log.setLevel(level)
