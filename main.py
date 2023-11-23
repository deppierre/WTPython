#!/usr/bin/env python

import bson, uuid, sys, re, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class PyHack(object):
    def __init__(self, db_path):
        conn = wiredtiger_open(db_path, 'create,cache_size=512M,session_max=33000,eviction=(threads_min=4,threads_max=4),config_base=false,statistics=(fast),log=(enabled=true,archive=true,path=journal,compressor=snappy),file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250),statistics_log=(wait=0),verbose=(version),compatibility=(release="3.3", require_min="3.2.0")')
        self.db_path = db_path

        #MongoDB
        self.database = "_" + self.__class__.__name__
        self.collection = None
        self.key = None
        self.values = []

        #WiredTiger
        self.mdb_catalog = '_mdb_catalog'

        self.__session = conn.open_session()
        print("Connection opened")

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        return self.__session.checkpoint()

    def get_new_cursor(self, table):
        """Function to create a new cursor"""
        return self.__session.open_cursor(f'table:{table}', None, "append")
    
    def get_k_v(self, table):
        """Function to return keys and values in a table"""
        k_v = {}
        cursor = self.get_new_cursor(table)

        while cursor.next() == 0:
            k_v[cursor.get_key()] = bson.decode(cursor.get_value())

        cursor.close()

        return k_v
    
    def get_new_k(self, table):
        """Function to generate a new key in a table"""
        new_key = 0
        for key in self.get_k_v(table):
            new_key = int(key) + 1

        return new_key
    
    def get_ident(self, namespace):
        """Function to get ident of a file"""
        catalog = self.get_k_v(self.mdb_catalog)

        for k,v in catalog.items():
            if 'md' in v:
                if(v['md']['ns'] == namespace):
                    return v['ident']

        return None


    def get_stats(self, table=None):
        """Function to get stats of a table"""
        if not table:
            cursor = self.__session.open_cursor("statistics:", None)
        else:
            cursor = self.__session.open_cursor(f"statistics:table:{table}", None)

        stats = []
        while cursor.next() == 0:
            stats.append(cursor.get_value())
        
        return stats
    
    def create_table(self, table):
        """Function to create a new table"""
        self.__session.create(f"table:{table}", "key_format=q,value_format=u,type=file,memory_page_max=10m,split_pct=90,leaf_value_max=64MB,checksum=on,block_compressor=snappy,app_metadata=(formatVersion=1),log=(enabled=false)")

    def drop_table(self, table):
        """Function to drop a table"""
        self.__session.drop(f"table:{table}")

    def close_session(self):
        """Function to close a WT session"""
        print("Connection closed")
        return self.__session.close()

    def get_values(self, table, key=None):
        """Function to get all values in a Table"""
        if key is not None:
            cursor = self.get_new_cursor(table)
            result = bson.decode(cursor[key])

            cursor.close()
        else:
            result = self.get_k_v(table)

        return result
        

    #Ex for records: {"id1":{ "key1":"value1" }, "id2": { "key2":"value2" }, ...}
    def insert_records(self, table, records):
        """Function to insert a list of records in a Table"""
        for key in records.keys():
            value = bson.encode(records[key])

            cursor = self.get_new_cursor(table)

            cursor[key] = value

            cursor.close()
            return print(f"-- Record {key} inserted")


    def delete_record(self, table, key):
        """Function to delete one record in a table"""
        cursor = self.get_new_cursor(table)

        cursor.set_key(key)
        cursor.remove()

        cursor.close()
        return print(f"-- Record {key} deleted")


def main():
    """Run function"""
    print(WIREDTIGER_VERSION_STRING)

    wt = PyHack("data/db")
    wt_catalog = wt.mdb_catalog
    namespace = None

    #collect arguments
    try: 
        argvs = sys.argv
        namespace = argvs[1].split('.')
        for argv in argvs[2:]:
            wt.values.append(argv)
    except IndexError:
        if not namespace :
            print("No namespace set")
        elif not wt.values:
            print("No value set")

    #Check if collection is set, if not create it
    if namespace is not None:
        if len(namespace) == 1:
            wt.collection = namespace[0]
        elif len(namespace) == 2:
            wt.database = namespace[0]
            wt.collection = namespace[1]

        namespace = wt.database + "." + wt.collection
        ident = wt.get_ident(namespace)
        
        if ident:
            coll_documents = wt.get_values(ident)
            if coll_documents:
                print(f"Documents in the collection ({wt.collection}): {coll_documents}")

            print("Do you wanna drop this collection ? (y/n)")
            drop = input().lower()

            if drop == "y":
                catalog = wt.get_k_v(wt_catalog)

                for k, v in catalog.items():
                    if 'md' in v:
                        if v['ident'] == ident:
                            wt.delete_record(
                                wt.mdb_catalog, 
                                k
                            )

                print(f"Table: {ident} is dropped")
                wt.drop_table(ident)
        else:
            ident = wt.collection
            wt.create_table(ident)
            print(f"-- Collection {wt.collection} created")
            new_key = wt.get_new_k(wt.mdb_catalog)
            uuid_binary = Binary(uuid.uuid4().bytes, 4)
            
            #new entry in catalogue
            wt.insert_records(
                wt.mdb_catalog,
                {
                    new_key:
                    {
                        'md': {
                            'ns': namespace, 
                            'options': {
                                'uuid': uuid_binary
                                }
                            },
                            'ns': namespace, 
                            'ident': ident
                    }
                }
            )
            print("-- Catalog updated with the new collection")

        #Check if value is set
        if wt.values:
            print("Insert value(s):")
            for value in wt.values:   
                new_key = wt.get_new_k(ident)
                wt.insert_records(
                    ident,
                    {
                        new_key:
                        {
                            'field': value
                        }
                    }
                )

    #Drop all user collections
    else:
        catalog = wt.get_k_v(wt_catalog)
        print("List all collections:")

        for k,v in catalog.items():
            if 'md' in v:
                print(f"-- namespace: {v['md']['ns']} - ident: {v['ident']}")

    print("Checkpoint ...")
    wt.checkpoint_session()

    wt.close_session()

if __name__ == "__main__":
    main()