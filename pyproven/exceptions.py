from typing import Any, Dict, Optional
import pymongo

from pymongo.errors import PyMongoError


class PyProvenError(Exception):
    """Base class for all pyproven exceptions."""

    def __init__(
        self,
        message: str = "",
        pymongo_exception: Optional[PyMongoError] = None,
    ):
        self.message = message
        self.mongo_excep = pymongo_exception
        self.explain = self.message 
        if self.mongo_excep:
            self.explain += (
                f" \n pymongo also raised an exception: {str(self.mongo_excep)}. \n"
            )
        super().__init__(self.explain)


class DocumentHistoryError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to gather document history."""


class SetVersionError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` is unable to set db version."""


class GetVersionError(PyProvenError):
    """Exception raised when :class:pyproven.database.ProvenDB' is unable to retrieve current version of database."""


class ListVersionError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` is unable to retrieve the version list of database."""


class BulkLoadError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to set or get bulk load status."""


class BulkLoadStartError(BulkLoadError):
    """Generic exception when failing to set the db to start bulk loading. """


class BulkLoadAlreadyStartedError(BulkLoadError):
    """Exception raised when a command to start bulk loading is sent, but bulk loading has already started."""


class BulkLoadStopError(BulkLoadError):
    """Generic exception raised when failing to set db to stop bulk loading."""


class BulkLoadNotStartedError(BulkLoadError):
    """Exception raised when attempting to stop/kill bulk loading on a db, but the db is not currently bulk loading."""


class CompactError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to compact between two versions."""


class CreateIgnoredError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to set a collection to be ignored."""


class PrepareForgetError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to prepare a set of documents to be forgotten."""


class ExecuteForgetError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to forget a set of documents."""


class GetDocumentProofError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to get the proofs of a certain set of documents.
    This is only raised when the command fails, if individual documents are invalid,
    the errors will be contained within the response proofs."""


class GetVersionProofError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to get the proof document for a specific version. """


class RollbackError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to rollback the database to the last valid version."""


class ListStorageError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB`fails to get the list of storage sizes for each collection in the db. """


class SubmitProofError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails at submitting a proof. """


class VerifyProofError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails at varifying a proof. """


class ShowMetadataError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails at displaying metadata."""


class HideMetadataError(PyProvenError):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails at hiding metadata."""
