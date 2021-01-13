from typing import Any, Dict
from collections import UserDict


class ProvenResponse(UserDict):
    """ABC for ProvenDB document object."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.ok = document["ok"]
