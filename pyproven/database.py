from datetime import datetime
from typing import Any, Optional, Union, Dict
import pymongo
import pymongo.message
from pyproven import versions
from pyproven.versions import ListVersionsDocument, VersionDocument
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
    def set_version(self,date: Union[str,int,datetime]) -> VersionDocument:
        """

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :return: A dict-like object representing the provenDB return document. 
        :rtype: SetVersionData
        """
        document = self.db.command("setVersion",date)
        return VersionDocument(document)
    def get_version(self) -> VersionDocument:
        """Gets the version the db is set to.
        :see https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document. 
        :rtype: GetVersionData
        """
        document = self.db.command("getVersion",1)
        if document['ok']:
            return VersionDocument(document)
    def list_versions(self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        sort_direction: Optional[int] = None
        ) -> ListVersionsDocument:
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
        document: Dict[str,Any] = self.db.command({"listVersions":command_args})
        if document['ok']:
            return ListVersionsDocument(document['versions'])
