"""Shared downstream client primitives."""

from .limits import AdmissionOverloaded, AdmissionSnapshot, BoundedAdmission
from .ndjson import iter_ndjson

__all__ = ["AdmissionOverloaded", "AdmissionSnapshot", "BoundedAdmission", "iter_ndjson"]
