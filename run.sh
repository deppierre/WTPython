mongod  --dbpath "data/db" --logpath "data/mongod.log" --port 27017 --wiredTigerCacheSizeGB "0.5" --setParameter logLevel=5

python -m pip install wiredtiger==3.2.1