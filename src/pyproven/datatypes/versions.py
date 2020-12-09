from collections import UserDict
from typing import Any, Dict, Optional, Union
from datetime import datetime
from pyproven import ProvenDB
from pyproven.datatypes import exceptions
class VersionData(UserDict):
    """ABC for setVersion and getVersion data classes."""
    def __init__(self,db:ProvenDB,command:str,command_arg:Union[int,str,datetime]):
        super().__init__(db.command(command,command_arg))
        self.data:Dict[str,Any]
        """raw ProvenDB document returned"""
        self.ok: Optional[str] = None
        """Return code from document"""
        self.response: Optional[str] = None
        """Response message"""
        self.version: Optional[int] = None
        """Version number of db"""
        self.status: Optional[str] = None
        """Status of version, if user set or current""" 
        if 'executeFailed' in self.data.keys():
            raise exceptions.VersionException(proven_err_doc=self.data['executeFailed'])
        elif 'ok' in self.data.keys():
            self.ok: int = self.data['ok']
            if self.ok:
                self.response = self.data["results"]
                self.version = self.data["version"]
                self.status = self.data["status"]
        else:
            raise exceptions.PyProvenException(proven_err_doc=self.data)

class GetVersionData(VersionData):
    """Dict-like object that fetches and holds
    the database respone to the ProvenDB getVersion command.
    :param db: :class:`pyproven.ProvenDB` object to set version on.
    :type db: :class:`pyproven.ProvenDB`
    :see https://provendb.readme.io/docs/getversion"""
    def __init__(self,db:ProvenDB):
        
        try:
            super().__init__(db,"getVersion",1)
        except exceptions.VersionException as e:
            raise exceptions.VersionGetException(  
                f"""ProvenDB returned executeFailed when trying to get db {str(db)} version).""",
                proven_err_doc=e.err_doc)
        except exceptions.PyProvenException as e:
            raise exceptions.VersionSetException(
                f"""General error occured when trying to get db {str(db)} version.""",
                proven_err_doc=e.err_doc)


        
class SetVersionData(VersionData):
    """Dict-like object that fetches and holds
    the database response to the ProvenDB setVersion command. 

    :param db: :class:`pyproven.ProvenDB` object to set version on.
    :type db: :class:`pyproven.ProvenDB`
    :param date: Sets db version to the version number or date provided.
    :type date: Union[int,str,datetime]
    :see https://provendb.readme.io/docs/setversion """
    def __init__(self,db:ProvenDB,date:Union[int,str,datetime]):
        """Constructor method for setVersion command results. """
        try:
            super().__init__(db,"setVersion",date)
        except exceptions.VersionException as e:
            raise exceptions.VersionSetException(  
                f"""ProvenDB returned executeFailed when trying 
                to set db {str(db)} to version
                {str(date)}).""",proven_err_doc=e.err_doc)
        except exceptions.PyProvenException as e:
            raise exceptions.VersionSetException(
                f"""General error occured when trying to set db {str(db)}
                to version {str(date)}.""",proven_err_doc=e.err_doc)
           

    
        