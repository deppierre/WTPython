#!/usr/bin/env python
# mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

import bson, uuid, sys, re, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class MyTable(object):
    def __init__(self, conn, my_collection, my_database = None):

        #MongoDB
        self.database = "_" + self.__class__.__name__
        self.table = my_collection
        self.ident = None

        if my_database:
            self.namespace = my_database + "." + my_collection

        self.__session = conn.open_session()
        print(f"New session created ({self.table})")

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        
        print(f"Checkpoint ({self.table})")
        return self.__session.checkpoint()

    def get_new_cursor(self):
        """Function to create a new cursor"""

        if self.ident is None:
            self.ident = self.table
            
        return self.__session.open_cursor(f'table:{self.ident}', None, "append")
    
    def get_k_v(self):
        """Function to return keys and values in a table"""

        k_v = {}
        cursor = self.get_new_cursor()

        while cursor.next() == 0:
            k_v[cursor.get_key()] = bson.decode(cursor.get_value())
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
        self.__session.create(f"table:{self.table}", "key_format=q,value_format=u,type=file,memory_page_max=10m,split_pct=90,leaf_value_max=64MB,checksum=on,block_compressor=snappy,app_metadata=(formatVersion=1),log=(enabled=false)")

    def drop_table(self):
        """Function to drop a table"""
        self.__session.drop(f"table:{self.ident}")

    def close_session(self):
        """Function to close a WT session"""

        print(f"Session closed ({self.table})")
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
            return print(f"-- Record {key} inserted")


    def delete_record(self, key):
        """Function to delete one record in a table"""
        cursor = self.get_new_cursor()

        cursor.set_key(key)
        cursor.remove()

        cursor.close()
        return print(f"-- Record {key} deleted")


def main():
    """Run function"""
    print(WIREDTIGER_VERSION_STRING)

    argvs = sys.argv
    db_path = "data/db"
    my_values = []

    #Debug connection
    # conn = wiredtiger_open(db_path, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4), config_base=false, statistics=(fast), log=(enabled=true,archive=true,path=journal,compressor=snappy), file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250), statistics_log=(wait=0), verbose=(version), compatibility=(release="3.3", require_min="3.2.0")')
    conn = wiredtiger_open(db_path, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4)')

    #collect arguments
    try:
        if argvs[1]:
            namespace = argvs[1].split('.')

            database = namespace[0]
            collection = namespace[1]
        else:
            print("No namespace provided")
            sys.exit(1)
    except IndexError:
        print("Namespace is incorrect (database.collection)")
        sys.exit(1)

    #collect values
    try:
        for argv in argvs[2:]:
            my_values.append(argv)
    except IndexError:
        print("No values set")

    coll_table  = MyTable(conn, my_collection = collection, my_database = database)
    catalog_table  = MyTable(conn, my_collection = "_mdb_catalog")

    catalog_cursor = catalog_table.get_k_v()
    for k,v in catalog_cursor.items():
        if 'md' in v:
            if v['md']['ns'] == coll_table.namespace:
                coll_table.ident = v['ident']
                catalog_key = k

    if coll_table.ident:
        coll_documents = coll_table.get_values()
        if coll_documents:
            print(f"Documents in the collection ({collection}): {coll_documents}")

        print("Do you wanna drop this collection ? (y/n)")
        drop = input().lower()

        if drop == "y":
            catalog_table.delete_record(catalog_key)
            coll_table.drop_table()

            print(f"Table: {coll_table.ident} is dropped")
    else:
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
                        'ident': coll_table.table
                }
            }
        )
        print("-- Catalog updated with the new collection")

    #Check if value is set
    if my_values:
        print("Insert value(s):")
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

    #Drop all user collections
    else:
        catalog = catalog_table.get_k_v()
        print("List all collections:")

        for k,v in catalog.items():
            if 'md' in v:
                print(f"-- namespace: {v['md']['ns']} - ident: {v['ident']}")

    coll_table.checkpoint_session()

    #Close all sessions
    coll_table.close_session()
    catalog_table.close_session()

if __name__ == "__main__":
    main()