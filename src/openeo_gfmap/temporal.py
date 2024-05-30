""" Definitions of temporal context"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class TemporalContext:
    """Temporal context is defined by a `start_date` and `end_date` values.

    The value must be encoded on a YYYY-mm-dd format, e.g. 2020-01-01
    """

    start_date: str
    end_date: str

    def to_datetime(self):
        """Converts the temporal context to a tuple of datetime objects."""
        return (
            datetime.strptime(self.start_date, "%Y-%m-%d"),
            datetime.strptime(self.end_date, "%Y-%m-%d"),
        )
