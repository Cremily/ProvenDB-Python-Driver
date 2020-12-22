import os,sys,pyproven,pymongo
client = pymongo.MongoClient(os.getenv("PROVENDB_URI"))
db = client['python-test']
pdb = pyproven.ProvenDB(db,provendb_hack=True)
pdb['unit-test'].insert_one({'a':1})

pdb.doc_history("unit-test",{"a":1})