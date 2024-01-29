#!/usr/bin/env python
# mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

import bson, sys, subprocess, os, pprint
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary
import re

class WTable(object):
    def __init__(self, conn, ident = None, type = None):

        self.ident = ident
        self.type = type

        self.__session = conn.open_session()

        if self.ident != "_mdb_catalog":
            print(f"\nNew Session ({ident})")

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        
        print("Checkpoint done")
        return self.__session.checkpoint()

    def get_new_cursor(self, statistics=False, log=False):
        """Function to create a new cursor"""

        if statistics:
            return self.__session.open_cursor(f"statistics:table:{self.ident}", None, "append")
        
        elif log:
            return self.__session.open_cursor("log:")

        else:
            return self.__session.open_cursor(f"table:{self.ident}", None, "append")
    
    def get_ks_vs(self, idx_key = None):
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

    def drop_table(self):
        """Function to drop a table"""

        print(f"\nTable dropped ({self.ident})")

        return self.__session.drop(f"table:{self.ident}")

    def close_session(self):
        """Function to close a WT session"""

        if(self.ident):
            print(f"Session closed ({self.ident})")
            return self.__session.close()


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
        conn = wiredtiger_open(db_path, "log=(enabled=true,path=journal,compressor=snappy),readonly=true,builtin_extension_config=(zstd=(compression_level=6))")
    except _wiredtiger.WiredTigerError as e:
        print(f"Connection error ({e})")
    else:
        catalog_table  = WTable(conn, ident = "_mdb_catalog", type = "c")

        #collect arguments
        try:
            namespace = argvs[1].split('.')
            database = namespace[0]
            collection = namespace[1]
        except IndexError:
            print("\nNo namespace provided (database.collection)")
            sys.exit(1)
        finally:
            catalog = catalog_table.get_ks_vs()
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
            catalog_table.close_session()
            
        #collect values
        try:
            for argv in argvs[2:]:
                my_values.append(argv)
        except IndexError:
            print("No values set")

        if ident:
            coll_table  = WTable(conn, ident = ident, type = "c")
            coll_documents = coll_table.get_ks_vs()

            if coll_documents:
                for k, v in coll_documents.items():
                    print(f"- RecordID: {k}, document(bson): {v}")
            else:
                print("-- 0 Documents")
            cursor = coll_table.get_new_cursor(log=True)
            
            print("\nJournal content:")
            while cursor.next() == 0:
                log_file, log_offset, opcount = cursor.get_key()
                txnid, rectype, optype, fileid, logrec_key, logrec_value = cursor.get_value()

                if rectype == 1:  # Assuming WT_LOGREC_MESSAGE corresponds to 1
                    print(f"LSN [{log_file}][{log_offset}].{opcount}: record type {rectype} optype {optype} txnid {txnid} fileid {fileid}")
                    print(f"key size {len(logrec_key)} value size {len(logrec_value)}")
                    
                    try:
                        bson_obj = bson.decode_all(logrec_value)
                        bson_obj = pprint.pformat(bson_obj, indent=1).replace('\n', '\n\t  ')
                        print('\t\"value-bson\":%s' % (bson_obj),)
                    except Exception as e:
                        # If bsons don't appear to be printing uncomment this line for the error reason.
                        #logging.error('Error at %s', 'division', exc_info=e)
                        print(logrec_value)
                        pass
            cursor.close()
            coll_table.close_session()
            
            if coll_indexes:
                for index in coll_indexes:
                    index_table = WTable(conn, ident = index["ident"], type = "i")

                    for k, v in index_table.get_ks_vs(idx_key = str(index["key"])).items():
                        print(f"-- KeyString: {{ {k[1:].strip()} }}, RecordID: {v.split(':')[1][:-1].strip()}")

                    index_table.close_session()

        else:
            print(f"\nNo collection found ({collection})")

if __name__ == "__main__":
    main()