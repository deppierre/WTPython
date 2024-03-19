### Build wiredtiger image
docker build -t wiredtiger .

### Init a new mongodb instance
cd /tmp && mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017

### Run the new container
docker run --rm --name wiredtiger -v /tmp/data/db:/data wiredtiger ./wt_dump.py -m mydb.mycollection /data