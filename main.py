#!/usr/bin/env python

import bson, uuid, sys, random
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat
from bson.binary import Binary

class PyHack(object):

    def __init__(self, dbPath):
        conn = wiredtiger_open(dbPath, 'create,statistics=(all)')
        self.mdbCatalog = '_mdb_catalog'

        self.__session = conn.open_session()
        self.print_message("connection", f"connection opened", "info")

    def checkpoint_session(self):
        return self.__session.checkpoint()

    def get_new_cursor(self, table):
        return self.__session.open_cursor(f'table:{table}', None)

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
    
    def close_session(self):
        self.print_message("connection", f"connection closed", "info")
        return self.__session.close()

    def print_table(self, table):
        cursor = self.get_new_cursor(table)
        while cursor.next() == 0:
            key = str(cursor.get_key())
            value = bson.decode(cursor.get_value())

            print(value)

            # if value[1] != '0':
            #     print(f"Key={key}, Value={value}")

    def insert_record(self, table, record):
        cursor = self.get_new_cursor(table)
        if len(record) == 2:
            k, v = record
            cursor[k] = bson.encode(v)
        
            self.print_message("insert", f"record {k} inserted", "info")
            cursor.close()


    def delete_record(self, table, key):
        cursor = self.get_new_cursor(table)

        cursor.set_key(key)
        cursor.remove()

        self.print_message("delete", f"record {key} deleted", "info")
        cursor.close()

    def update_catalog(self, collection, command):
        newNs = f'myDbFromWT.{collection}'
        cursor = self.get_new_cursor(self.mdbCatalog)


        if command == "create":
            while cursor.next() == 0:
                maxKey = cursor.get_key()
                doc = bson.decode(cursor.get_value())

                if ('ns' in doc):
                    if (doc['ns'] == newNs):
                        self.print_message("catalog", f"Insert error. This collection already exist ({doc['ns']})", "warning")
                        break
            else:
                newKey = maxKey + 1
                uuid_binary = Binary(uuid.uuid4().bytes, 4)

                self.insert_record(
                    self.mdbCatalog, 
                    [ newKey, {
                        'md': {
                            'ns': newNs, 
                            'options': {'uuid': uuid_binary}}, 
                            'ns': newNs, 
                            'ident': 'collection-4--4331703230610760751'
                        } 
                    ]
                )

        elif command == "drop":
            while cursor.next() == 0:
                key = cursor.get_key()
                doc = bson.decode(cursor.get_value())

                if ('ns' in doc):
                    if (doc['ns'] == newNs):
                        print(doc['ns'] )
                        self.delete_record(
                            self.mdbCatalog, 
                            key
                        )
                        break
            else:
                raise Exception(f"update_catalog: namespace doesn't exist ({newNs})")
        else:
            raise Exception("update_catalog: command uknown ('create', 'drop')")

        cursor.close()

    def print_message(self, module, trace, level="info"):
        print(f"{module} - {trace}")



def main():
    argvs = sys.argv

    myNewColl = "myCollFromWT"

    wt = PyHack("data/db")
    wtCatalogName = wt.get_catalog()

    try:
        wt.update_catalog(myNewColl, "create")
        try:
            print(wt.print_table(wtCatalogName))
        except:
            print("Error to read the table")
    except Exception as Cat:
        wt.print_message("catalog",Cat, "warning")
    finally:
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