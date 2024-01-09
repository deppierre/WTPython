#!/usr/bin/env python
# mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

import bson, uuid, sys, re, os, json
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class WTable(object):
    def __init__(self, conn, coll = None, db = None, ident = None):

        self.collection = coll
        self.ident = ident

        if coll and db:
            self.namespace = db + "." + coll

        self.__session = conn.open_session()
        print(f"-- New session created ({self.collection})")

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        
        print("Checkpoint done")
        return self.__session.checkpoint()

    def get_new_cursor(self):
        """Function to create a new cursor"""

        return self.__session.open_cursor(f'table:{self.ident}', None, "append")
    
    def get_k_v(self):
        """Function to return keys and values in a table"""

        k_v = {}
        cursor = self.get_new_cursor()

        while cursor.next() == 0:
            # print(cursor.get_key())
            # print(cursor.get_value())
            if self.collection:
                k_v[cursor.get_key()] = bson.decode(cursor.get_value())
            else:
                key = cursor.get_key()
                value = cursor.get_value()

                k_v[key] = value

                # print(f"key: {key}, value: {value}")
                # k_v[cursor.get_key()] = cursor.get_value().decode(json.detect_encoding(cursor.get_value()))

        cursor.close()

        return k_v
    
    def get_new_k(self):
        """Function to generate a new key in a table"""
        new_key = 0
        for key in self.get_k_v():
            new_key = int(key) + 1

        return new_key

    # def get_stats(self):
    #     """Function to get stats of a table"""
    #     if not table:
    #         cursor = self.__session.open_cursor("statistics:", None)
    #     else:
    #         cursor = self.__session.open_cursor(f"statistics:table:{table}", None)

    #     stats = []
    #     while cursor.next() == 0:
    #         stats.append(cursor.get_value())
        
    #     return stats
    
    def create_table(self):
        """Function to create a new table"""

        if not self.ident:
            self.ident = self.collection + "_" + str(uuid.uuid4())
        return self.__session.create(f"table:{self.ident}", "key_format=q,value_format=u,type=file,memory_page_max=10m,split_pct=90,leaf_value_max=64MB,checksum=on,block_compressor=snappy,app_metadata=(formatVersion=1),log=(enabled=false)")

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
        for key in records.keys():
            value = bson.encode(records[key])

            cursor = self.get_new_cursor()

            cursor[key] = value

            cursor.close()

            if self.ident != "_mdb_catalog":
                print(f"-- Record {key} inserted")
                
        return

    def delete_record(self, key):
        """Function to delete one record in a table"""
        cursor = self.get_new_cursor()

        cursor.set_key(key)
        cursor.remove()

        cursor.close()
        print(f"-- Record {key} deleted")

        return


def main():
    """Run function"""
    print(WIREDTIGER_VERSION_STRING)

    argvs = sys.argv
    db_path = "data/db"
    my_values = []

    #Debug connection
    # conn = wiredtiger_open(db_path, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4), config_base=false, statistics=(fast), log=(enabled=true,archive=true,path=journal,compressor=snappy), file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250), statistics_log=(wait=0), verbose=(version), compatibility=(release="3.3", require_min="3.2.0")')
    conn = wiredtiger_open(db_path, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4)')
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
        print("\nList all collections:")

        for k,v in catalog.items():
            if 'md' in v:
                print(f"-- namespace: {v['md']['ns']} - ident: {v['ident']}")

    #collect values
    try:
        for argv in argvs[2:]:
            my_values.append(argv)
    except IndexError:
        print("No values set")

    print("\nOpening sessions")
    coll_table  = WTable(conn, coll = collection, db = database)

    catalog_cursor = catalog_table.get_k_v()
    for k,v in catalog_cursor.items():
        if 'md' in v:
            if v['md']['ns'] == coll_table.namespace:
                coll_table.ident = v['ident']
                catalog_key = k

                if 'idxIdent' in v:
                    for k in v['idxIdent']:
                        if k == "_id_":
                            coll_table_idx_ident = v['idxIdent'][k]

                            index_table = WTable(conn, ident = coll_table_idx_ident)
                            print(index_table.get_k_v())

    if coll_table.ident:
        print(f"\nDocuments in the {collection} collection:")

        coll_documents = coll_table.get_values()
        if coll_documents:
            for k, v in coll_documents.items():
                print(f"-- key (RecordID): {k}, value (doc): {v}")
        else:
            print(f"-- No Documents in this collection ({collection})")

        if not my_values:
            print("\nDo you want to drop this collection ? (y/n)")
            drop = input().lower()

            if drop == "y":
                catalog_table.delete_record(catalog_key)
                coll_table.drop_table()
                catalog_table.checkpoint_session()

                print(f"-- Table: {coll_table.ident} is dropped")
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

    #Check if value is set
    if my_values and (coll_table.ident or create == "y"):
        print("\nInsert value(s):")
        for value in my_values:   
            new_key = coll_table.get_new_k()
            coll_table.insert_records(
                {
                    new_key:
                    {
                        'field': value
                    }
                }
            )

    #Checkpoint
    if my_values:
        print("\nDo you want to Checkpoint these modifications ? (y/n)")
        checkpoint = input().lower()

        if checkpoint == "y":
            new_checkpoint = WTable(conn)
            new_checkpoint.checkpoint_session()

    #Close all sessions
    print("\nClosing sessions")
    coll_table.close_session()
    catalog_table.close_session()

if __name__ == "__main__":
    main()