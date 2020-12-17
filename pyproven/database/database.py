from datetime import datetime
from typing import Any, Set, Union
import pymongo.database
from pyproven.datatypes.versions import GetVersionData,SetVersionData

class ProvenDB():
    """Proven DB Database object that wraps the original pymongo Database object. 
    See :class:`database.ProvenDB` """
    def __init__(self,database:pymongo.database.Database,*args,**kwargs):
        """Constructor method"""
        self.db: pymongo.database.Database = database
    def __getattr__(self,name: str) -> Any:
        """Calls :class:`pymongo.database.Database` object attribute or method when none could be found in self.

        :param name: Name of :class:`pymongo.database.Database` attribute or method to be called.
        :type name: str
        :return: Value from :class:`pymongo.database.Database` attribute or method.
        :rtype: Any
        """
        return getattr(self.db,name)
    def set_version(self,date:Union[str,int,datetime]) -> SetVersionData:
        """

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :return: A dict-like object representing the provenDB return document. 
        :rtype: SetVersionData
        """
        return SetVersionData(date)
    def get_version(self) -> GetVersionData:
        """Gets the version the db is set to.
        :see https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document. 
        :rtype: GetVersionData
        """
        return GetVersionData(self)
