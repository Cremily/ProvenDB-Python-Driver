from collections import UserDict
from typing import Any, Dict, List, Optional, Union
from pyproven.response import ProvenResponse
from bson import BSON


class GetDocumentProofResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["proofs"] = [_process_document_proof(proof) for proof in self["proofs"]]
        self.proofs: List[DocumentProof] = self["proofs"]


class DocumentProof(UserDict):
    def __init__(self, document: Dict[str, Any]) -> None:
        super().__init__(document)


class FailedDocumentProof(DocumentProof):
    def __init__(self, document: Dict[str, Any]) -> None:
        super().__init__(document)
        self.errmsg = self["errmsg"]


class SuccessfulDocumentProof(DocumentProof):
    def __init__(self, document: Dict[str, Any]) -> None:
        super().__init__(document)
        self.collection: str = self["collection"]
        self.scope: str = self["scope"]
        self.provenDbId: str = self["ProvenDbId"]
        self.documentId: str = self["documentId"]
        self.version: int = self["version"]
        self.status: str = self["status"]
        self.btcTransaction: str = self["btcTransaction"]
        self.btcBlockNumber: str = self["btcBlockNumber"]
        self.versionProofId: str = self["versionProofId"]
        self.documentHash: str = self["documentHash"]
        self.versionHash: str = self["versionHash"]
        self.proof: Union[BSON, Dict[str, Any]] = self["proof"]


def _process_document_proof(document: Dict[str, any]) -> DocumentProof:
    if "errmsg" in document.keys():
        return FailedDocumentProof(document)
    else:
        return SuccessfulDocumentProof(document)
