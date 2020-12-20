
import unittest
import sys
import os
from pymongo import MongoClient
sys.path.append("../pyproven")
from pyproven import ProvenDB
PROVENDB_URI = os.getenv("PROVENDB_URI")
PROVENDB_DATABASE = "python-test"
class ProvenDBTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = MongoClient(PROVENDB_URI)
        self.db = self.client[PROVENDB_DATABASE]
        self.pdb = ProvenDB(self.db)
    def test_proven_constructor(self):
        self.assertIsInstance(self.pdb,ProvenDB)
    def test_get_version(self):
        version = self.pdb.get_version()
        self.assertTrue("The version is set to: " in version.response)
    def test_set_version(self):
        version = self.pdb.set_version(1)
        self.assertTrue(version.version == 1)
    def test_list_versions(self):
        versions = self.pdb.list_versions()

if __name__ == "__main__":
    unittest.main()