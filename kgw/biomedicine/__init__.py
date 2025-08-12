"""Knowledge graph projects from the domain of biomedicine."""

from ._ckg import Ckg
from ._hald import Hald
from ._hetionet import Hetionet
from ._monarchkg import MonarchKg
from ._oregano import Oregano
from ._primekg import PrimeKg


__all__ = [
    "Ckg",
    "Hald",
    "Hetionet",
    "MonarchKg",
    "Oregano",
    "PrimeKg",
]
