from typing import Any, Dict

from pymongo.errors import PyMongoError

class PyProvenException(Exception):
    """Base class for all PyProven exceptions."""
    def __init__(self,
        message:str=None, pymongo_exception:PyMongoError=None):
        self.message = message
        self.mongo_excep = pymongo_exception
        self.explain = self.message + "\n"
        if self.mongo_excep:
            self.explain += f"pymongo raised the following exception: {str(self.mongo_excep)}. \n"
        super().__init__(self.explain)
        
        
class VersionException(PyProvenException):
    """Base class for setVersion and getVersion exceptions."""
class SetVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.database.ProvenDB`
    is given an invalid version argument."""
class GetVersionException(PyProvenException):
    """Exception raised when :class:pyproven.`database.database.ProvenDB' is unable 
    to retrieve current version of database."""
class ListVersionException(PyProvenException):
    """Exception raised when :class:`pyproven.database.database.ProvenDB` is unable
    to retrieve the version list of database."""