from typing import Any, Dict
import pymongo

from pymongo.errors import PyMongoError


class PyProvenException(Exception):
    """Base class for all pyproven exceptions."""

    def __init__(
        self,
        message: str = None,
        pymongo_exception: PyMongoError = None,
        proven_error: Dict[str, Any] = None,
    ):
        self.message = message
        self.mongo_excep = pymongo_exception
        self.proven_error = proven_error
        self.explain = self.message + "\n"
        if self.mongo_excep:
            self.explain += (
                f"pymongo also raised an exception: {str(self.mongo_excep)}. \n"
            )
        if self.proven_error:
            self.explain += (
                f"The ProvenDB instance also raised an error {str(proven_error)}."
            )
        super().__init__(self.explain)


class DocumentHistoryException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to gather document history."""


class SetVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` is unable to set db version."""


class GetVersionException(PyProvenException):
    """Exception raised when :class:pyproven.database.ProvenDB' is unable to retrieve current version of database."""


class ListVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` is unable to retrieve the version list of database."""


class BulkLoadException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to set or get bulk load status."""


class BulkLoadStartException(BulkLoadException):
    """Generic exception when failing to set the db to start bulk loading. """


class BulkLoadAlreadyStartedException(BulkLoadStartException):
    """Exception raised when a command to start bulk loading is sent, but bulk loading has already started."""


class BulkLoadStopException(BulkLoadException):
    """Generic exception raised when failing to set db to stop bulk loading."""


class BulkLoadNotStartedException(BulkLoadException):
    """Exception raised when attempting to stop/kill bulk loading on a db, but the db is not currently bulk loading."""


class CompactException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to compact between two versions."""


class CreateIgnoredException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to set a collection to be ignored."""


class PrepareForgetException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to prepare a set of documents to be forgotten."""


class ExecuteForgetException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to forget a set of documents."""


class GetDocumentProofException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB`
    fails to get the proofs of a certain set of documents.
    This is only raised when the command fails, if individual documents are invalid,
    the errors will be contained within the response proofs."""


class GetVersionProofException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to get the proof document for a specific version. """


class RollbackException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to rollback the database to the last valid version."""


class ListStorageException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB`fails to get the list of storage sizes for each collection in the db. """

