from typing import Any, Dict

from pymongo.errors import PyMongoError

class PyProvenException(Exception):
    """Base class for all PyProven exceptions."""
    def __init__(self,
        message:str=None, pymongo_exception:PyMongoError=None,proven_error:Dict[str,any]=None):
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
    """Exception raised when :class:`pyproven.database.ProvenDB` fails to gather document history."""
class SetVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.database.ProvenDB` is unable to set db version."""
class GetVersionException(PyProvenException):
    """Exception raised when :class:pyproven.`database.database.ProvenDB' is unable to retrieve current version of database."""
class ListVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.database.ProvenDB` is unable to retrieve the version list of database."""

