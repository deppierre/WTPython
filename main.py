#!/usr/bin/env python

import bson, uuid, sys, re, os
from wiredtiger import wiredtiger_version, wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class PyHack(object):

    def __init__(self, dbPath):
        conn = wiredtiger_open(dbPath, 'create,statistics=(all),verbose=(version)')
        self.dbPath = dbPath
        self.prefix = "_" + self.__class__.__name__

        #MongoDB
        self.database = self.prefix
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

        return k_v
    
    def get_new_k(self, table):
        newKey = 0
        for key in self.get_k_v(table):
            newKey = int(key) + 1

        return newKey

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
        self.__session.create(f"table:{table}", "key_format=q,value_format=u, type=file,memory_page_max=10m,split_pct=90,leaf_value_max=64MB,checksum=on,block_compressor=snappy, app_metadata=(formatVersion=1),log=(enabled=false)")

    def drop_table(self, table):
        self.__session.drop(f"table:{table}")

    def close_session(self):
        print("Connection closed")
        return self.__session.close()

    def get_values(self, table, key=None):
        if key is not None:
            cursor = self.get_new_cursor(table)
            return bson.decode(cursor[key])
        else:
            return self.get_k_v(table)

    #Ex for records: {"id1":{ "key1":"value1" }, "id2": { "key2":"value2" }, ...}
    def insert_records(self, table, records):
        for key in records.keys():
            print(records[key])
            value = bson.encode(records[key])

            cursor = self.get_new_cursor(table)

            cursor[key] = value

            print(f"--Record {key} inserted")
            cursor.close()


    def delete_record(self, table, key):
        cursor = self.get_new_cursor(table)

        cursor.set_key(key)
        cursor.remove()

        print(f"--Record {key} deleted")
        cursor.close()


def main():
    print(wiredtiger_version()[0])

    wt = PyHack("data/db")
    wtCatalogName = wt.get_catalog()

    #collect arguments
    try: 
        argvs = sys.argv
        wt.collection = argvs[1]
        for argv in argvs[2:]:
            wt.values.append(argv)
    except IndexError:
        if not wt.collection :
            print("No collection set")
        elif not wt.values:
            print("No value set")

    #Check if collection is set, if not create it
    if wt.collection is not None:
        try:
            ident = wt.prefix + "_" + wt.collection

            collDocuments = wt.get_values(ident)
            print(f"Documents in the collection ({wt.collection}): {collDocuments}")

        except _wiredtiger.WiredTigerError:
            wt.create_table(ident)
            print(f"--Collection {wt.collection} created")

            newNs = wt.database + "." + wt.collection 
            newKey = wt.get_new_k(wt.mdbCatalog)
            uuid_binary = Binary(uuid.uuid4().bytes, 4)
            #new entry in catalogue

            wt.insert_records(
                wt.mdbCatalog, 
                { 
                    newKey:
                    {
                        'md': {
                            'ns': newNs, 
                            'options': {'uuid': uuid_binary}}, 
                            'ns': newNs, 
                            'ident': ident
                    }
                }
            )
            print(f"--Catalog updated with the new collection")

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
        print("Drop all collections")

        catalog = wt.get_k_v(wtCatalogName)
        last_item = list(catalog.keys())[-1]

        #update catalog
        try: 
            while catalog[last_item]["md"]["ns"].startswith(wt.prefix):
                print(f"Last item (id: {last_item}) to delete: {wt.get_values(wtCatalogName, last_item)}")
                wt.delete_record(
                    wt.mdbCatalog, 
                    last_item
                )
                last_item -= 1
        except KeyError:
            print("Nothing to update in catalog")
        except TypeError:
            print(f"Error to remove item {last_item}: {catalog[last_item]}")
        else:
            #Drop wt tables
            for file in os.listdir(wt.dbPath):
                if file.startswith(wt.prefix):
                    wt.drop_table(file.replace(".wt", ""))
                    print(f"Table: {file} is dropped")

    print("Checkpoint ...")
    wt.checkpoint_session()

    wt.close_session()

if __name__ == "__main__":
    main()