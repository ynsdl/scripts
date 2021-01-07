# @author yunus dal
# this script; if mongodb msgPack collection count is zero truncate cassandra msg_pack_user table

from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import datetime
import os
import subprocess
import sys
import logging

# ********* remote server infos and cassandra installation path for clear snapshots after truncate table *********
host1 = 'ip1'
host2 = 'ip2'
host3 = 'ip'
command = 'cd /opt/apache-cassandra-3.10/bin/ ; ./nodetool clearsnapshot msg msg_pack_user'

# ********* mongodb connection *********
client = MongoClient()
client = MongoClient('mongodb://mongo_username:mongopassword@ip:port,ip:port,ip:port/?replicaSet=replica_set_name')

# ********* cassandra connection *********
auth_provider = PlainTextAuthProvider(username='', password='')
cluster = Cluster(["ip"], port=9042, auth_provider = auth_provider)

# ********* logging *********
log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# ********* main class *********
def main():

    db = client.nm 
    ISODate = datetime.datetime.today().strftime('%Y/%m/%d')
    count = db.msgPacks.find({"send":{"$gt":ISODate}}).count()

    if count == 0:

        log.info("msgPack collection count is zero...")
        session = cluster.connect()
        log.info("truncating msg_pack_user table...")
        session.execute("truncate msg.msg_pack_user")
        log.info("closing cassandra connection")
        session.close()

        log.info("clear snaphosts on remote hosts...")
        subprocess.Popen(["ssh", "%s" % host1, command])
        subprocess.Popen(["ssh", "%s" % host2, command])
        subprocess.Popen(["ssh", "%s" % host3, command])
    else:
        log.info("msgPack collection count is not zero...")
    
if __name__ == "__main__":
    main()