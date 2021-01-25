from collections import UserDict

from typing import Any, Dict


from pyproven.response import ProvenResponse


class CompactResponse(ProvenResponse):
    """A dict-like object holding the amount of proofs and versions deleted after a compact operation."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.nProofsDeleted = self["nProofsDeleted"]
        self.nVersionsDeleted = self["nVersionsDeleted"]


class ListVersionDocument(UserDict):
    """A dict-like object holding the individual version data retrieved from a list_version command."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.version: float = document["version"]
        self.status: str = document["status"]
        self.effective_date: str = document["effectiveDate"]


class ListVersionsResponse(UserDict):
    """A dict-like object containing the versions matching the search parameters in a list_version command."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["versions"] = [ListVersionDocument(doc) for doc in document["versions"]]


class VersionResponse(ProvenResponse):
    """ABC for get_version and set_version response documents."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.response: str = document["response"]
        self.version: float = document["version"]
        self.status: str = document["status"]


class GetVersionResponse(VersionResponse):
    """Dict-like object holding the data for a get_version response.
    See :class:`VersionResponse`"""


class SetVersionResponse(VersionResponse):
    """Dict-like object holding the data for a set_version response.
    See :class:`VersionResponse`"""
