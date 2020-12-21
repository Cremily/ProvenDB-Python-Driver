
import datetime

import unittest

import os

from pymongo import MongoClient

try:
    from pyproven.exceptions import ListVersionException, SetVersionException
    from pyproven import ProvenDB
except ImportError:
    import sys
    sys.path.append("../pyproven")
    from pyproven.exceptions import ListVersionException, SetVersionException
    from pyproven import ProvenDB
PROVENDB_URI = os.getenv("PROVENDB_URI")
PROVENDB_DATABASE = "python-test"
class ProvenDBTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = MongoClient(PROVENDB_URI)
        self.db = self.client[PROVENDB_DATABASE]
        self.pdb = ProvenDB(self.db)
    
    def test_proven_constructor(self):
        """Ensures that a ProvenDB object is created."""
        self.assertIsInstance(self.pdb,ProvenDB)
    
    def test_get_version(self):
        """ProvenDB instance can get the version the DB is set to."""
        version = self.pdb.get_version()
        self.assertTrue("The version is set to: " in version.response)
    
    def test_set_version_first(self):
        """ProvenDB can set version to the first version the DB."""
        version = self.pdb.set_version(1)
        self.assertTrue(version.version == 1)
    
    def test_set_version_current(self):
        """ProvenDB can set version to the most current DB version."""
        version = self.pdb.set_version('current')
        self.assertTrue(version.response == "The version has been set to: 'current'")
    
    def test_set_version_impossible(self):
        """ProvenDB will raise the correct exception when given an impossible version number."""
        version = self.pdb.set_version('current')
        impossible_version = version.version + 1000
        with self.assertRaises(SetVersionException):
            self.pdb.set_version(impossible_version)
   
    def test_list_versions_noargs(self):
        """list_versions can be called with no arguments and always presents at least the current version."""
        versions = self.pdb.list_versions()
        self.assertTrue(versions)
    
    def test_list_versions_limit(self):
        """list_versions correctly limits the number of returned versions."""
        versions = self.pdb.list_versions(limit=1)
        self.assertTrue(len(versions)==1)

    def test_doc_history(self):
        history = self.pdb.doc_history("unit-test",{"a":1})
        self.assertTrue(history.histories)
if __name__ == "__main__":
    unittest.main()