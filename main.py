#!/usr/bin/env python

import bson, uuid, sys, re, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class PyHack(object):
    def __init__(self, dbPath):
        conn = wiredtiger_open(dbPath, 'create,cache_size=512M,session_max=33000,eviction=(threads_min=4,threads_max=4),config_base=false,statistics=(fast),log=(enabled=true,archive=true,path=journal,compressor=snappy),file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250),statistics_log=(wait=0),verbose=(),compatibility=(release="3.3", require_min="3.2.0")')
        self.dbPath = dbPath

        #MongoDB
        self.database = "_" + self.__class__.__name__
        self.collection = None
        self.key = None
        self.values = []

        #WiredTiger
        self.mdbCatalog = '_mdb_catalog'

        self.__session = conn.open_session()
        print("Connection opened")

    def checkpoint_session(self):
        return self.__session.checkpoint()

    def get_new_cursor(self, table):
        return self.__session.open_cursor(f'table:{table}', None, "append")
    
    def get_k_v(self, table):
        k_v = {}
        cursor = self.get_new_cursor(table)

        while cursor.next() == 0:
            k_v[cursor.get_key()] = bson.decode(cursor.get_value())

        cursor.close()

        return k_v
    
    def get_new_k(self, table):
        newKey = 0
        for key in self.get_k_v(table):
            newKey = int(key) + 1

        return newKey
    
    def get_ident(self, namespace):
        catalog = self.get_k_v(self.mdbCatalog)

        for k,v in catalog.items():
            if 'md' in v:
                if(v['md']['ns'] == namespace):
                    return v['ident']
        else:
            return None


    def get_stats(self, table=None):
        if not table:
            cursor = self.__session.open_cursor(f'statistics:', None)
        else:
            cursor = self.__session.open_cursor(f'statistics:table:{table}', None)

        stats = []
        while cursor.next() == 0:
            stats.append(cursor.get_value())
        
        return stats

    def get_catalog(self):
        return self.mdbCatalog
    
    def create_table(self, table):
        self.__session.create(f"table:{table}", "key_format=q,value_format=u,type=file,memory_page_max=10m,split_pct=90,leaf_value_max=64MB,checksum=on,block_compressor=snappy,app_metadata=(formatVersion=1),log=(enabled=false)")

    def drop_table(self, table):
        self.__session.drop(f"table:{table}")

    def close_session(self):
        print("Connection closed")
        return self.__session.close()

    def get_values(self, table, key=None):
        if key is not None:
            cursor = self.get_new_cursor(table)
            result = bson.decode(cursor[key])

            cursor.close()
        else:
            result = self.get_k_v(table)

        return result
        

    #Ex for records: {"id1":{ "key1":"value1" }, "id2": { "key2":"value2" }, ...}
    def insert_records(self, table, records):
        for key in records.keys():
            value = bson.encode(records[key])

            cursor = self.get_new_cursor(table)

            cursor[key] = value

            cursor.close()
            return print(f"-- Record {key} inserted")


    def delete_record(self, table, key):
        cursor = self.get_new_cursor(table)

        cursor.set_key(key)
        cursor.remove()

        cursor.close()
        return print(f"-- Record {key} deleted")


def main():
    print(WIREDTIGER_VERSION_STRING)

    wt = PyHack("data/db")
    wtCatalogName = wt.get_catalog()
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
            collDocuments = wt.get_values(ident)
            if collDocuments:
                print(f"Documents in the collection ({wt.collection}): {collDocuments}")

            print("Do you wanna drop this collection ? (y/n)")
            drop = input().lower()

            if drop == "y":
                catalog = wt.get_k_v(wtCatalogName)

                for k, v in catalog.items():
                    if 'md' in v:
                        if v['ident'] == ident:
                            wt.delete_record(
                                wt.mdbCatalog, 
                                k
                            )

                print(f"Table: {ident} is dropped")
                wt.drop_table(ident)
        else:
            ident = wt.collection
            wt.create_table(ident)
            print(f"-- Collection {wt.collection} created")
            newKey = wt.get_new_k(wt.mdbCatalog)
            uuid_binary = Binary(uuid.uuid4().bytes, 4)
            
            #new entry in catalogue
            wt.insert_records(
                wt.mdbCatalog, 
                { 
                    newKey:
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
            print(f"-- Catalog updated with the new collection")

        #Check if value is set
        if wt.values:
            print("Insert value(s):")
            for value in wt.values:   
                newKey = wt.get_new_k(ident)
                wt.insert_records(
                    ident, 
                    {
                        newKey: 
                        {
                            'field': value
                        }
                    }
                )

    #Drop all user collections
    else:
        catalog = wt.get_k_v(wtCatalogName)
        print("List all collections:")

        for k,v in catalog.items():
            if 'md' in v:
                print(f"-- namespace: {v['md']['ns']} - ident: {v['ident']}")

    print("Checkpoint ...")
    wt.checkpoint_session()

    wt.close_session()

if __name__ == "__main__":
    main()