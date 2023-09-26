#!/usr/bin/env python


import os, bson, uuid, sys
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat

class PyHack(object):

    def __init__(self):
        # Connect to the database and open a session
        conn = wiredtiger_open('data/db', 'create,statistics=(all)')
        self.mdbCatalog = '_mdb_catalog'
        self.session = conn.open_session()

    def get_catalog(self):
        return self.mdbCatalog
    
    def close_session(self):
        return self.session.close()

    def print_table(self, table):
        cursor = self.session.open_cursor(f'table:{table}', None)
        while cursor.next() == 0:
            key = str(cursor.get_key())
            value = bson.decode(cursor.get_value())

            print(value)

            # if value[1] != '0':
            #     print(f"Key={key}, Value={value}")

    def insert_record(self, table, record):
        cursor = self.session.open_cursor(f'table:{table}', None, "append")
        k, v = record
        cursor[k] = bson.encode(v)
        
        print(f"Record {k} inserted")
        cursor.close()


    def delete_record(self, table, key):
        cursor = self.session.open_cursor(f'table:{table}', None)

        cursor.set_key(key)
        cursor.remove()

        print(f"Record {key} deleted")
        cursor.close()

    def update_catalog(self, collection, command):
        newNs = f'myDbFromWT.{collection}'
        cursor = self.session.open_cursor(f'table:{self.mdbCatalog}', None)

        if command == "insert":
            while cursor.next() == 0:
                maxKey = cursor.get_key()
                doc = bson.decode(cursor.get_value())

                if ('ns' in doc) and (doc['ns'] == newNs):
                    print(f"Error: This collection already exist ({doc['ns']})")
                    break
                else:
                    newKey = maxKey + 1
                    self.insert_record(
                        self.mdbCatalog, 
                        [ newKey, {'md': {'ns': newNs, 'options': {'uuid': uuid.uuid1()}, 'indexes': [], 'prefix': -1}, 'ns': newNs, 'ident': 'collection-4--4331703230610760751'} ]
                        # [ {'md': {'ns': 'myDbFromWT.myCollFromWT', 'options': {'uuid': UUID('5500e784-7a23-4c8e-9341-4b402c7b7e6f')}, 'indexes': [], 'prefix': -1}, 'ns': 'myDbFromWT.myCollFromWT', 'ident': 'collection-4--4331703230610760751'} ]
                    )

        elif command == "drop":
            while cursor.next() == 0:
                key = cursor.get_key()
                doc = bson.decode(cursor.get_value())

                if ('ns' in doc) and (doc['ns'] == newNs):
                    self.delete_record(
                        self.mdbCatalog, 
                        key
                    )
                    break
        cursor.close()



def main():
    argvs = sys.argv
    command = argvs[1]

    myNewColl = "myCollFromWT"


    wt = PyHack()
    wt.update_catalog(myNewColl,"insert")

    wtCatalog = wt.get_catalog()
    print(wt.print_table(wtCatalog))

    wt.close_session()




    # Open a cursor and insert a record
    # cursor = session.open_cursor('table:access', None)

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
    statcursor = session.open_cursor("statistics:")
    print_cursor(statcursor)
    statcursor.close()

def print_file_stats(session):
    fstatcursor = session.open_cursor("statistics:table:access")
    print_cursor(fstatcursor)
    fstatcursor.close()

def print_overflow_pages(session):
    ostatcursor = session.open_cursor("statistics:table:access")
    val = ostatcursor[stat.dsrc.btree_overflow]
    if val != 0:
        print('%s=%s' % (str(val[0]), str(val[1])))
    ostatcursor.close()

def print_derived_stats(session):
    dstatcursor = session.open_cursor("statistics:table:access")
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