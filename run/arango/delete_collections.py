import argparse
from os import environ

from arango import ArangoClient

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--collection", default="all", help="test setting")
parser.add_argument(
    "--db",
    # default="_system",
    default="wos_csv",
    help="db for arangodb connection",
)

client = ArangoClient()
cred_name = environ["ARANGO_UNAME"]
cred_pass = environ["ARANGO_PASS"]
args = parser.parse_args()
mode = args.collection
sys_db = client.db(args.db, username=cred_name, password=cred_pass)

if mode == "all":
    print([c["name"] for c in sys_db.collections() if c["name"][0] != "_"])
    cnames = [c["name"] for c in sys_db.collections() if c["name"][0] != "_"]
    for gn in cnames:
        sys_db.delete_collection(gn)
    print([c["name"] for c in sys_db.collections() if c["name"][0] != "_"])

    print("graphs:")
    print([c["name"] for c in sys_db.graphs()])
    gnames = [c["name"] for c in sys_db.graphs()]
    for gn in gnames:
        sys_db.delete_graph(gn)
    print([c["name"] for c in sys_db.graphs()])
else:
    sys_db.delete_collection(mode)
