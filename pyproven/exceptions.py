from typing import List

from pymongo.errors import PyMongoError


def extract_error_info(err: PyMongoError):
    """Attempts to extract error fields from the Pymongo document."""
    error_doc: List[str] = err._message.split("{")[1].split("}")[0].split(",")
    ok: int = int(error_doc[0].split(": ")[1])
    errmsg: str = error_doc[1].split(": ")[1].lstrip('"').rstrip('"')
    code: int = int(error_doc[2].split(": ")[1])
    codeName: str = error_doc[3].split(": ")[1].lstrip('"').rstrip('"')
    return {"ok": ok, "errmsg": errmsg, "code": code, "codeName": codeName}


class PyProvenError(PyMongoError):
    """Base class for all pyproven exceptions."""

    def __init__(self, err: PyMongoError):
        super().__init__(message=err._message, error_labels=err._error_labels)


class BulkLoadAlreadyStartedError(PyProvenError):
    """Exception raised when a command to start bulk loading is sent, but bulk loading has already started."""


class CompactError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to compact between two versions."""


class CompactValueError(CompactError):
    """Error raised when versions given for compacting are invalid."""


class CompactProofError(CompactError):
    """Error raised when a proof doesn't exist above the compact range."""
