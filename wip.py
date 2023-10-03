#!/usr/bin/env python

import bson, uuid, sys, random, re, os
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary
import traceback


class PyHack(object):

    def __init__(self, dbPath):
        conn = wiredtiger_open(dbPath, 'create,statistics=(all)')
        self.mdbCatalog = '_mdb_catalog'
        self.dbPath = dbPath

        self.collection = None
        self.value = None

        self.__session = conn.open_session()
        self.print_message("connection", f"connection opened", "info")

    def checkpoint_session(self):
        return self.__session.checkpoint()

    def get_new_cursor(self, table):
        return self.__session.open_cursor(f'table:{table}', None)
    
    def get_k_v(self, table):
        k_v = {}
        cursor = self.get_new_cursor(table)

        while cursor.next() == 0:
            k_v[cursor.get_key()] = bson.decode(cursor.get_value())

        return k_v
    
    def get_new_k(self, table):
        for k in self.get_k_v(table):
            newKey = k + 1

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
        self.print_message("connection", f"connection closed", "info")
        return self.__session.close()

    def get_values(self, table, key=None):
        return self.get_k_v(table)

            # if value[1] != '0':
            #     print(f"Key={key}, Value={value}")

    def insert_record(self, table, record):
        cursor = self.get_new_cursor(table)
        if len(record) == 2:
            k, v = record


            cursor.set_key(k)
            cursor.set_value(bson.encode(v))

            # cursor.set_key('key'+ str(k))
            # cursor.set_value('value' + str(bson.encode(v)))
            cursor.insert()
        
            self.print_message("insert", f"record {k} inserted", "info")
            cursor.close()


    def delete_record(self, table, key):
        cursor = self.get_new_cursor(table)

        cursor.set_key(key)
        cursor.remove()

        self.print_message("delete", f"record {key} deleted", "info")
        cursor.close()

    def print_message(self, module, trace, level="info"):
        print(f"{module} - {trace}")



def main():
    wt = PyHack("data/db")
    wtCatalogName = wt.get_catalog()

    #collect arguments
    try: 
        argvs = sys.argv
        wt.collection = argvs[1]
        wt.value = argvs[2]
    except IndexError:
        if not wt.collection :
            print("No collection set")
        elif not wt.value:
            print("No value set")
            

    #Check if collection is set, if not create it
    if wt.collection  is not None:
        try:
            collDocuments = wt.get_values(wt.collection)
            print(f"Documents in the collection: {collDocuments}")

            # if documents:
            #     print(f"Document in the {wt.collection} collection:\n{documents}")
        except _wiredtiger.WiredTigerError:
            wt.create_table(wt.collection)
            print(f"Collection {wt.collection} created")

            newNs = f"myDbFromWT.{wt.collection }"
            newKey = wt.get_new_k(wt.mdbCatalog)
            #new entry in catalogue
            wt.insert_record(
                wt.mdbCatalog, 
                [ newKey, {
                    'md': {
                        'ns': newNs, 
                        'options': {'uuid': Binary(uuid.uuid4().bytes, 4)}}, 
                        'ns': newNs, 
                        'ident': wt.collection 
                    } 
                ]
            )

            print(wt.get_values(wt.mdbCatalog))
    #Check if value is set
        if wt.value is not None:
            print("Insert value")
    #Drop all user collections
    else:
        print("Mode: drop collection(s)")

        catalog = wt.get_k_v(wtCatalogName)
        last_item = list(catalog.keys())[-1]
        #update catalog
        try: 
            while re.match(r'^(?!collection|index|WiredTiger|_mdb|sizeStorer)\w+$', catalog[last_item]["ident"]) is not None:
                #add last_item
                print(f"Last item to delete: {wt.get_values(wtCatalogName, last_item)}")
                wt.delete_record(
                    wt.mdbCatalog, 
                    last_item
                )
                last_item -= 1
        except KeyError:
            print("Nothing to update in catalog")
        #Drop wt tables
        for file in os.listdir(wt.dbPath):
            match = re.match(r'^(?!collection|index|WiredTiger|_mdb|sizeStorer)(\w+)\.wt$', file)

            if match is not None:
                tableName = match.group(1)
                wt.drop_table(tableName)
                print(f"Table: {tableName} is dropped")

        # else:
        #     maxKey = value
        #     print(maxKey)
        #     wt.drop_table()
        # wt.update_catalog(myNewColl, "drop")

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