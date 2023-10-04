#!/usr/bin/env python

import bson, uuid, sys, re, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary


class PyHack(object):

    def __init__(self, dbPath):
        conn = wiredtiger_open(dbPath, 'create,statistics=(all)')
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
            try:
                k_v[cursor.get_key()] = bson.decode(cursor.get_value())
            except TypeError:
                k_v[cursor.get_key()] = cursor.get_value()

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
        self.__session.create(f"table:{table}", "key_format=S,value_format=S")

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

            # if value[1] != '0':
            #     print(f"Key={key}, Value={value}")

    #Ex for records: {"id1":{ "key1":"value1" }, "id2": { "key2":"value2" }, ...}
    def insert_records(self, table, records):
        for record in records.keys():
            key = record
            value = bson.encode(records[record])

            cursor = self.get_new_cursor(table)

            cursor.set_key(key)
            cursor.set_value("lol")

            cursor.insert()
            
            print(f"Record {key} inserted")
            cursor.close()


    def delete_record(self, table, key):
        cursor = self.get_new_cursor(table)

        cursor.set_key(key)
        cursor.remove()

        print(f"Record {key} deleted")
        cursor.close()


def main():
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
            print(f"Documents in the collection ({ident}): {collDocuments}")

        except _wiredtiger.WiredTigerError:
            wt.create_table(ident)
            print(f"Collection {wt.collection} created")

            newNs = wt.database + "." + wt.collection, 
            newKey = wt.get_new_k(wt.mdbCatalog)
            #new entry in catalogue

            wt.insert_records(
                wt.mdbCatalog, 
                { 
                    newKey:
                    {
                        'md': {
                            'ns': newNs, 
                            'options': {'uuid': Binary(uuid.uuid4().bytes, 4)}
                        }, 
                        'ns': newNs, 
                        'ident': ident
                    }
                }
            )
            print(f"Catalog updated with the new collection")

    #Check if value is set
        if wt.values:
            print("Insert value")

            for value in wt.values:   
                newKey = str(wt.get_new_k(ident))

                wt.insert_records(
                    ident, 
                    {
                        newKey: 
                        {
                            'test': 'test'
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
            while catalog[last_item]["ident"].startswith(wt.prefix):
                print(f"Last item (id: {last_item}) to delete: {wt.get_values(wtCatalogName, last_item)}")
                wt.delete_record(
                    wt.mdbCatalog, 
                    last_item
                )
                last_item -= 1
        except KeyError:
            print("Nothing to update in catalog")
        #Drop wt tables
        for file in os.listdir(wt.dbPath):
            if file.startswith(wt.prefix):
                wt.drop_table(file.replace(".wt", ""))
                print(f"Table: {file} is dropped")

    print("Checkpoint ...")
    wt.checkpoint_session()

    wt.close_session()








    # Open a cursor and insert a record
    # cursor = session.get_new_cursor('table:access', None)

    # keys, values = [ (k,v) for k,v in mdbCatalog ]

    # newKey = max(keys) + 1

    # 

    # print(print_cursor(mdbCatalog))



    # collCursor[1] = bson.encode({'name':'TestUpdate'})

    # collCursor.set_value("")


    
    # print_database_stats(session)
    # print_file_stats(session)
    # print_overflow_pages(session)
    # print_derived_stats(session)


def print_database_stats(session):
    statcursor = session.get_new_cursor("statistics:")
    print_cursor(statcursor)
    statcursor.close()

def print_file_stats(session):
    fstatcursor = session.get_new_cursor("statistics:table:access")
    print_cursor(fstatcursor)
    fstatcursor.close()

def print_overflow_pages(session):
    ostatcursor = session.get_new_cursor("statistics:table:access")
    val = ostatcursor[stat.dsrc.btree_overflow]
    if val != 0:
        print('%s=%s' % (str(val[0]), str(val[1])))
    ostatcursor.close()

def print_derived_stats(session):
    dstatcursor = session.get_new_cursor("statistics:table:access")
    ckpt_size = dstatcursor[stat.dsrc.block_checkpoint_size][1]
    file_size = dstatcursor[stat.dsrc.block_size][1]
    percent = 0
    if file_size != 0:
        percent = 100 * ((float(file_size) - float(ckpt_size)) / float(file_size))
    print("Table is %%%s fragmented" % str(percent))

    app_insert = int(dstatcursor[stat.dsrc.cursor_insert_bytes][1])
    app_remove = int(dstatcursor[stat.dsrc.cursor_remove_bytes][1])
    app_update = int(dstatcursor[stat.dsrc.cursor_update_bytes][1])
    fs_writes = int(dstatcursor[stat.dsrc.cache_bytes_write][1])

    if app_insert + app_remove + app_update != 0:
        print("Write amplification is " + '{:.2f}'.format(fs_writes / (app_insert + app_remove + app_update)))
    dstatcursor.close()



if __name__ == "__main__":
    main()