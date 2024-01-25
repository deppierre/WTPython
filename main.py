#!/usr/bin/env python
# mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

import bson, uuid, sys, subprocess, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary

class WTable(object):
    def __init__(self, conn, name = None, ident = None, type = None):

        self.name = name
        self.ident = ident
        self.type = type

        self.__session = conn.open_session()

        print(f"\nNew Session ({name})")

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
            if self.type == "c":
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
            self.ident = self.name + "_" + str(uuid.uuid4())
        return self.__session.create(f"table:{self.ident}", "key_format=q,value_format=u,type=file,memory_page_max=4096B,split_pct=50,leaf_page_max=4096B,checksum=on,block_compressor=snappy")

    def drop_table(self):
        """Function to drop a table"""

        print(f"\nTable dropped ({self.name})")

        return self.__session.drop(f"table:{self.ident}")

    def close_session(self):
        """Function to close a WT session"""

        if(self.ident):
            print(f"\nSession closed ({self.name})")
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
    coll_indexes = []
    drop = None
    coll_table = None
    ident = None
    database, collection = "", ""


    #Debug connection
    # conn = wiredtiger_open(db_path, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4), config_base=false, statistics=(fast), log=(enabled=true,archive=true,path=journal,compressor=snappy), file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250), statistics_log=(wait=0), verbose=(version), compatibility=(release="3.3", require_min="3.2.0")')
    
    try:
        conn = wiredtiger_open(db_path, "create, cache_size=512M, session_max=33000, log=(enabled,recover=on), eviction=(threads_min=4,threads_max=4), verbose=(), statistics=(all)")
    except _wiredtiger.WiredTigerError as e:
        print(f"Connection error ({e})")
    else:
        catalog_table  = WTable(conn, ident = "_mdb_catalog", name = "_mdb_catalog", type = "c")

        #collect arguments
        try:
            namespace = argvs[1].split('.')
            database = namespace[0]
            collection = namespace[1]
        except IndexError:
            print("\nNo namespace provided (database.collection)")
            sys.exit(1)
        finally:
            catalog = catalog_table.get_k_v()

            print("\nMongoDB catalog content (_mdb_catalog):")

            for k,v in catalog.items():
                if 'md' in v:
                    print(f"-- namespace: {v['md']['ns']} - ident: {v['ident']}")

                    if v["md"]["ns"] == database + "." + collection:
                            ident = v["ident"]

                            if "indexes" in v["md"]:
                                for index in v["md"]["indexes"]:
                                    name = index["spec"]["name"]
                                    coll_indexes.append({
                                        "key": index["spec"]["key"],
                                        "name": name,
                                        "ident": v["idxIdent"][name]
                                    })

                            catalog_key = k

            catalog_table.close_session()

        #collect values
        try:
            for argv in argvs[2:]:
                my_values.append(argv)
        except IndexError:
            print("No values set")

        if ident:
            coll_table  = WTable(conn, name = collection, ident = ident, type = "c")

            print(f"\nident: {coll_table.ident}\n-- key: RecordID || value: document")

            coll_documents = coll_table.get_values()
            if coll_documents:
                for k, v in coll_documents.items():
                    print(f"-- key: {k} || value: {v}")
            else:
                print("-- 0 Documents")

            coll_table.close_session()
            
            if coll_indexes:
                for index in coll_indexes:
                    index_table = WTable(conn, name = index['name'], ident = index["ident"], type = "i")
                    print(f"\nident: {index['ident']}\n-- key: KeyString || value: RecordID")

                    for k, v in index_table.get_k_v(idx_key = str(index["key"])).items():
                        print(f"-- key: {{ {k[1:].strip()} }} || value: {v.split(':')[1][:-1].strip()}")

                    index_table.close_session()

        else:
            print(f"\nNo collection found ({collection})")

        if ident:
            print("\nDo you want to drop this collection ? (y/n)")
            drop = input().lower()

            if drop == "y":
                catalog_table  = WTable(conn, ident = "_mdb_catalog", name = "_mdb_catalog", type = "c")
                coll_table  = WTable(conn, name = collection, ident = ident, type = "c")

                catalog_table.delete_record(catalog_key)
                coll_table.drop_table()

                if coll_indexes:
                    for index in coll_indexes:
                        index_table = WTable(conn, name = index['name'], ident = index["ident"], type = "i")
                        index_table.drop_table()
                        index_table.close_session()

                catalog_table.checkpoint_session()

                catalog_table.close_session()
                

        #Check if value is set
        if my_values:
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

        if ident and drop is None:
            print(f"\nStatistics for {coll_table.ident}:")

            for collstat in coll_table.get_stats():
                print(f"-- {collstat[0]} :: {collstat[1]}")

if __name__ == "__main__":
    main()