from datetime import datetime

from typing import Any, Optional, Set, Union, Dict

import pymongo
from pymongo.errors import OperationFailure, PyMongoError
import pymongo.message
import pymongo.database

from pyproven.versions import GetVersionData, ListVersionsData
from pyproven.versions import SetVersionData
from pyproven.exceptions import SetVersionException,ListVersionException,GetVersionException

def _fix_op_msg(flags, command, dbname, read_preference, slave_ok, check_keys,
            opts, ctx=None):
    """Get a OP_MSG message."""
    command['$db'] = dbname
    name = next(iter(command))
    try:
        identifier = pymongo.message._FIELD_MAP.get(name)
        docs = command.pop(identifier)
    except KeyError:
        identifier = ""
        docs = None
    try:
        if ctx:
            return pymongo.message._op_msg_compressed(
                flags, command, identifier, docs, check_keys, opts, ctx)
        return pymongo.message._op_msg_uncompressed(
            flags, command, identifier, docs, check_keys, opts)
    finally:
        # Add the field back to the command.
        if identifier:
            command[identifier] = docs
class ProvenDB():
    """Proven DB Database object that wraps the original pymongo Database object. 
    See :class:`database.ProvenDB` """
    def __init__(self,database: pymongo.database.Database,*args,**kwargs):
        """Constructor method"""
        self.db: pymongo.database.Database = database
        pymongo.message._op_msg = _fix_op_msg
    def __getattr__(self,name: str) -> Any:
        """Calls :class:`pymongo.database.Database` object attribute or method when none could be found in self.

        :param name: Name of :class:`pymongo.database.Database` attribute or method to be called.
        :type name: str
        :return: Value from :class:`pymongo.database.Database` attribute or method.
        :rtype: Any
        """
        return getattr(self.db,name)
    def set_version(self,date: Union[str,int,datetime]) -> SetVersionData:
        """

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :return: A dict-like object representing the provenDB return document. 
        :rtype: SetVersionData
        """
        try:
            document = self.db.command("setVersion",date)
            return SetVersionData(document)
        except PyMongoError as e:
            raise SetVersionException(f"Failure to set version {date} on db {self.db.name}.",e) from None
    def get_version(self) -> GetVersionData:
        """Gets the version the db is set to.
        :see https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document. 
        :rtype: GetVersionData
        """
        try:
            document = self.db.command("getVersion",1)
            if document['ok']:
                return GetVersionData(document)
            else:
                raise GetVersionException(proven_err_doc=document[''])
        except PyMongoError as e:
            raise GetVersionException(f"Failure to get version from db {self.db.name}.",e) from None
    def list_versions(self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        sort_direction: Optional[int] = None
        ) -> ListVersionsData:
        """[summary]

        :param start_date: [description], defaults to None
        :type start_date: Optional[datetime], optional
        :param end_date: [description], defaults to None
        :type end_date: Optional[datetime], optional
        :param limit: [description], defaults to None
        :type limit: Optional[int], optional
        :param sort_direction: [description], defaults to None
        :type sort_direction: Optional[int], optional
        :return: A list-like container of :class:`pyproven.versions.VersionDocument`
        holding all retrieved versions of database. 
        :rtype: ListVersionsDocument
        """
        command_args: Dict[str,Union[datetime,int]] = {}
        if start_date:
            command_args.update({"startDate":start_date})
        if end_date:
            command_args.update({"endDate":end_date})
        if limit:
            command_args.update({"limit":limit})
        if sort_direction:
            command_args.update({"sortDirection":sort_direction})
        try:
            document: Dict[str,Any] = self.db.command({"listVersions":command_args})
            if document['ok']:
                return ListVersionsData(document['versions'])
        except PyMongoError as e:
            raise ListVersionException(f"Unable to get version list from {self.db.name}.",e) from None
