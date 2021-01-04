from typing import Any, Dict
import pymongo

from pymongo.errors import PyMongoError

class PyProvenException(Exception):
    """Base class for all pyproven exceptions."""
    def __init__(self,message:str=None, pymongo_exception:PyMongoError=None,
                proven_error:Dict[str,Any]=None):
        self.message = message
        self.mongo_excep = pymongo_exception
        self.proven_error = proven_error
        self.explain = self.message + "\n"
        if self.mongo_excep:
            self.explain += f"pymongo also raised an exception: {str(self.mongo_excep)}. \n"
        if self.proven_error:
            self.explain += f"The ProvenDB instance also raised an error {str(proven_error)}."
        super().__init__(self.explain)   
class DocumentHistoryException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` 
    fails to gather document history."""
class SetVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` 
    is unable to set db version."""
class GetVersionException(PyProvenException):
    """Exception raised when :class:pyproven.database.ProvenDB' 
    is unable to retrieve current version of database."""
class ListVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` 
    is unable to retrieve the version list of database."""
class BulkLoadException(PyProvenException):
    """Exception raised when :class:`pyproven.database.ProvenDB` 
    fails to set or get bulk load status."""
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