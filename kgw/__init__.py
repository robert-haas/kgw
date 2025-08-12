"""kgw - Knowledge Graph Workflows

A Python package for downloading, converting, and analyzing
a selection of knowledge graphs.

Currently five projects from the domain of biomedicine
are covered. In future, more projects from the same or other
domains might get added. Contributions are welcome and encouraged!

"""

__version__ = "0.2.0"

__all__ = [
    "biomedicine",
    "run",
]


from . import biomedicine
from ._shared.base import run
