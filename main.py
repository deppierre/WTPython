#!/usr/bin/env python
# mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

import bson, uuid, sys, subprocess, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class WTable(object):
    def __init__(self, conn, coll = None, db = None, ident = None):

        self.collection = coll
        self.ident = ident
        self.indexes = []

        if coll and db:
            self.namespace = db + "." + coll

        self.__session = conn.open_session()

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        
        print("Checkpoint done")
        return self.__session.checkpoint()

    def get_new_cursor(self, statistics=False):
        """Function to create a new cursor"""

        if statistics:
            return self.__session.open_cursor(f"statistics:table:{self.ident}", None, "append")

        return self.__session.open_cursor(f"table:{self.ident}", None, "append")
    
    def get_k_v(self, idx_key = None):
        """Function to return keys and values in a table"""

        k_v = {}
        cursor = self.get_new_cursor()

        while cursor.next() == 0:
            if self.collection:
                k_v[cursor.get_key()] = bson.decode(cursor.get_value())
            else:
                key = cursor.get_key().hex()
                value = cursor.get_value().hex()


                if(idx_key == "{'_id': 1}"):
                    key += value[:4]
                    value = value[-2:]

                ksdecode = subprocess.run(
                    [
                        os.path.join(os.path.dirname(__file__) or '.',"ksdecode"),
                        "-o",
                        "bson",
                        "-p",
                        idx_key,
                        "-t",
                        value,
                        "-r",
                        "long",
                        key,
                    ],
                    capture_output=True, check=True
                )
                keystring, record_id = ksdecode.stdout.decode("utf-8").strip().split(",")

                k_v[keystring] = record_id

        cursor.close()

        return k_v
    
    def get_new_k(self):
        """Function to generate a new key in a table"""
        new_key = 0
        for key in self.get_k_v():
            new_key = int(key) + 1

        return new_key

    def get_stats(self):
        """Function to get stats of a table"""
        cursor = self.get_new_cursor(statistics=True)

        stat_output = []
        stat_filter = [
            "cache: modified pages evicted",
            "cache: pages evicted",
            "cache: pages read into cache",
            "cache: pages written from cache"
        ]

        while cursor.next() == 0:
            stat_ = cursor.get_value()
            if stat_[0] in stat_filter:
                stat_output.append(stat_)

        return stat_output
    
    def create_table(self):
        """Function to create a new table"""

        if not self.ident:
            self.ident = self.collection + "_" + str(uuid.uuid4())
        return self.__session.create(f"table:{self.ident}", "key_format=q,value_format=u,type=file,memory_page_max=4096B,split_pct=50,leaf_page_max=4096B,checksum=on,block_compressor=snappy,app_metadata=(formatVersion=1)")

    def drop_table(self):
        """Function to drop a table"""
        return self.__session.drop(f"table:{self.ident}")

    def close_session(self):
        """Function to close a WT session"""

        print(f"-- Session closed ({self.collection})")
        return self.__session.close()

    def get_values(self, key=None):
        """Function to get all values in a Table"""

        if key is not None:
            cursor = self.get_new_cursor()
            result = bson.decode(cursor[key])

            cursor.close()
        else:
            result = self.get_k_v()

        return result
        

    #Ex for records: {"id1":{ "key1":"value1" }, "id2": { "key2":"value2" }, ...}
    def insert_records(self, records):
        """Function to insert a list of records in a Table"""

        cursor = self.get_new_cursor()

        for key in records.keys():
            value = bson.encode(records[key])
            cursor[key] = value

        cursor.close()

        return len(records)
                

    def delete_record(self, key):
        """Function to delete one record in a table"""
        cursor = self.get_new_cursor()

        cursor.set_key(key)
        cursor.remove()

        return cursor.close()

def main():
    """Run function"""
    print(WIREDTIGER_VERSION_STRING)

    argvs = sys.argv
    db_path = "data/db"
    my_values = []
    create = None
    drop = None

    #Debug connection
    # conn = wiredtiger_open(db_path, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4), config_base=false, statistics=(fast), log=(enabled=true,archive=true,path=journal,compressor=snappy), file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250), statistics_log=(wait=0), verbose=(version), compatibility=(release="3.3", require_min="3.2.0")')
    conn = wiredtiger_open(db_path, "create, cache_size=512M, session_max=33000, log=(enabled,recover=on), eviction=(threads_min=4,threads_max=4), verbose=(), statistics=(all)")
    catalog_table  = WTable(conn, ident = "_mdb_catalog", coll = "_mdb_catalog")

    #collect arguments
    try:
        if argvs[1]:
            namespace = argvs[1].split('.')

            database = namespace[0]
            collection = namespace[1]
        else:
            print("\nNo namespace provided (database.collection)")
            sys.exit(1)
    except IndexError:
        print("\nNo namespace provided (database.collection)")
        sys.exit(1)
    finally:
        catalog = catalog_table.get_k_v()
        print("\nList of collections:")

        for k,v in catalog.items():
            if 'md' in v:
                print(f"-- namespace: {v['md']['ns']} - ident: {v['ident']}")

    #collect values
    try:
        for argv in argvs[2:]:
            my_values.append(argv)
    except IndexError:
        print("No values set")

    coll_table  = WTable(conn, coll = collection, db = database)

    catalog_cursor = catalog_table.get_k_v()
    for k,v in catalog_cursor.items():
        if "md" in v:
            if v["md"]["ns"] == coll_table.namespace:
                coll_table.ident = v["ident"]

                if "indexes" in v["md"]:
                    for index in v["md"]["indexes"]:
                        name = index["spec"]["name"]
                        coll_table.indexes.append({
                            "key": index["spec"]["key"],
                            "name": name,
                            "ident": v["idxIdent"][name]
                        })

                catalog_key = k

    if coll_table.ident:
        print(f"\nident: {coll_table.ident}\n-- key: RecordID, value: document")

        coll_documents = coll_table.get_values()
        if coll_documents:
            for k, v in coll_documents.items():
                print(f"-- key: {k}, value: {v}")
        else:
            print("-- 0 Documents")
    else:
        print("\nDo you want to create this collection ? (y/n)")
        create = input().lower()

        if create == "y":
            coll_table.create_table()
            print(f"-- Collection {collection} created")
            new_key = catalog_table.get_new_k()
            uuid_binary = Binary(uuid.uuid4().bytes, 4)
            
            #new entry in catalogue
            catalog_table.insert_records(
                {
                    new_key:
                    {
                        'md': {
                            'ns': coll_table.namespace, 
                            'options': {
                                'uuid': uuid_binary
                                }
                            },
                            'ns': coll_table.namespace, 
                            'ident': coll_table.ident
                    }
                }
            )
            catalog_table.checkpoint_session()
            print("-- Catalog updated with the new collection")

    if coll_table.indexes:
        for index in coll_table.indexes:
            print(f"\nident: {index['ident']}\n-- key: KeyString, value: RecordID")
            index_table = WTable(conn, ident = index["ident"])

            for k, v in index_table.get_k_v(idx_key = str(index["key"])).items():
                print(f"-- key: {{ {k[1:].strip()} }}, value: {v.split(':')[1][:-1].strip()}")

    if not my_values and not create:
        print("\nDo you want to drop this collection ? (y/n)")
        drop = input().lower()

        if drop == "y":
            catalog_table.delete_record(catalog_key)
            coll_table.drop_table()
            catalog_table.checkpoint_session()

            if coll_table.indexes:
                for index in coll_table.indexes:
                    index_table = WTable(conn, ident = index["ident"])
                    index_table.drop_table()
                    print(f"-- Table: {index_table.ident} is dropped")


            print(f"-- Table: {coll_table.ident} is dropped")

    #Check if value is set
    if my_values and (coll_table.ident or create == "y"):
        nb_insert = 0
        for value in my_values:
            new_key = coll_table.get_new_k()
            nb_insert += coll_table.insert_records(
                {
                    new_key:
                    {
                        'field': value
                    }
                }
            )
        print(f"\nInsert new value(s): {nb_insert}")

    #Checkpoint
    if my_values:
        print("\nDo you want to Checkpoint these modifications ? (y/n)")
        checkpoint = input().lower()

        if checkpoint == "y":
            new_checkpoint = WTable(conn)
            new_checkpoint.checkpoint_session()

    if not drop and not create:
        print(f"\nStatistics for {coll_table.ident}:")

        for collstat in coll_table.get_stats():
            print(f"-- {collstat[0]} :: {collstat[1]}")

    #Close all sessions
    print("\nClosing sessions")
    coll_table.close_session()
    catalog_table.close_session()

if __name__ == "__main__":
    main()