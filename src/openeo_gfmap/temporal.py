""" Definitions of temporal context"""
from dataclasses import dataclass
from typing import Tuple


@dataclass
class TemporalContext:
    """Temporal context is defined by a `start_date` and `end_date` values.

    The value must be encoded on a YYYY-mm-dd format, e.g. 2020-01-01
    """

    start_date: str
    end_date: str