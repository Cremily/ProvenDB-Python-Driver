import os,sys,pyproven,pymongo
client = pymongo.MongoClient(os.getenv("PROVENDB_URI"))
print("got client")
db = client['python-test']
pdb = pyproven.ProvenDB(db,provendb_hack=True)
pdb.create_ignored("ignored_test")