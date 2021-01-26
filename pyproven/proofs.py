import datetime
from typing import Any, Dict, List, Union

from bson.objectid import ObjectId
from pyproven.response import ProvenDocument, ProvenResponse
from bson import BSON


class GetDocumentProofResponse(ProvenResponse):
    """Dict-like object representing the list of proofs retrieved for a given version."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["proofs"] = [_process_document_proof(proof) for proof in self["proofs"]]
        self.proofs: List[DocumentProof] = self["proofs"]


class DocumentProof(ProvenDocument):
    """ABC for an individual document proof."""

    def __init__(self, document: Dict[str, Any]) -> None:
        super().__init__(document)


class FailedDocumentProof(DocumentProof):
    """Dict-like object holding the failed proof for a specific document."""

    def __init__(self, document: Dict[str, Any]) -> None:
        super().__init__(document)
        self.errmsg = self["errmsg"]


class SuccessfulDocumentProof(DocumentProof):
    """Dict-like object holding details of a valid proof for a specific document. """

    def __init__(self, document: Dict[str, Any]) -> None:
        super().__init__(document)
        self.collection: str = self["collection"]
        self.scope: str = self["scope"]
        self.provenDbId: ObjectId = self["provenDbId"]
        self.documentId: str = self["documentId"]
        self.version: int = self["version"]
        self.status: str = self["status"]
        self.btcTransaction: str = self["btcTransaction"]
        self.btcBlockNumber: str = self["btcBlockNumber"]
        self.versionProofId: str = self["versionProofId"]
        self.documentHash: str = self["documentHash"]
        self.versionHash: str = self["versionHash"]
        self.proof: Union[BSON, Dict[str, Any]] = self["proof"]


def _process_document_proof(document: Dict[str, Any]) -> DocumentProof:
    """Helper factory function to determine if a specific document proof is a failure or not."""
    if "errmsg" in document.keys():
        return FailedDocumentProof(document)
    else:
        return SuccessfulDocumentProof(document)


class GetVersionProofResponse(ProvenResponse):
    """Dict-like object holding the list of proofs for a specific version."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.proofs = [VersionProof(proof) for proof in self["proofs"]]


class VersionProof(ProvenDocument):
    """Dict-like object representing a specific proof for a version."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self._id: ObjectId = self["_id"]
        self.proofId: str = self["proofId"]


class SubmitProofResponse(ProvenResponse):
    """Dict-like object holding data for the submitted proof."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.version: int = document["version"]
        self.dateTime: datetime.datetime = document["dateTime"]
        self.hash: str = document["hash"]
        self.proofId: str = document["proofId"]
        self.status: str = document["status"]


class VerifyProofResponse(ProvenResponse):
    """Dict-like object containing verification data for a specific proof."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        # proof json documents are highly dependent on the underlying proof type and collection and thus can't be statically defined.
        self.proof: Union[Dict[str, Any], bytes] = self["proof"]
        self.proofId: str = self["proofId"]
        self.proofStatus: str = self["proofStatus"]
        self.version: int = self["version"]
