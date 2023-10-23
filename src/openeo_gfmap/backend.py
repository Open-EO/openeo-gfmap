"""Backend Contct.

Defines on which backend the pipeline is being currently used.
"""
from dataclasses import dataclass
from enum import Enum


class Backend(Enum):
    """Enumerating the backends supported by the Mapping Framework."""

    TERRASCOPE = ("terrascope",)
    CREODIAS = ("creodias",)
    EODC = ("eodc",)
    LOCAL = "local"


@dataclass
class BackendContext:
    """Backend context and information.

    Containing backend related information useful for the framework to
    adapt the process graph.
    """

    backend: Backend
