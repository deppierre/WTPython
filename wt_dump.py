#!/usr/bin/env python
# Example of commands:
#   ./wt_dump.py -m mytestdb2.colleyction /data/db
#   ./wt_dump.py -m log /data/db
# To setup a lab:
#   mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# Import data:
#   mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

import bson, sys, subprocess, os, pprint, re
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger
from bson.binary import Binary

class WTable(object):
    """Class to dump a WTable"""
    def __init__(self, conn, ident = None, ttype = None):

        self.ident = ident
        self.type = ttype

        self.__session = conn.open_session()

        if self.ident != "_mdb_catalog":
            print(f"\nNew Session ({ident})")

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        
        print("Checkpoint done")
        return self.__session.checkpoint()

    def get_new_cursor(self, uri_mode = None):
        """Function to create a new cursor"""

        if uri_mode == "statistics":
            return self.__session.open_cursor(f"statistics:table:{self.ident}", None, "append")
        elif uri_mode == "log":
            return self.__session.open_cursor("log:")
        elif uri_mode == "metadata":
            return self.__session.open_cursor("metadata:")
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
        cursor = self.get_new_cursor(uri_mode="statistics")

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
    
def util_usage():
    print("Usage: wt_dump -m {<ns name>|catalog|log} [uri]")
    print("Example: wt_dump -m mydb.mycollection data/db")
    print("\t-m the intended mode or a namespace to dump a collection.")
    print("\turi is the dbPath folder in MongoDB (data/db ...).")
    sys.exit(1)

def main():
    if len(sys.argv) < 3:
        util_usage()
        exit()

    if sys.argv[1] != '-m':
        print('A mode must be specified with -m.')
        util_usage()

    mode_str = sys.argv[2]
    uri = "data/db" if len(sys.argv) < 4 else sys.argv[3]

    if mode_str == "log":
        mode = "log"
    elif mode_str == "catalog":
        mode = "metadata"
    elif '.' in mode_str:
        mode = "coll_dump"
        try:
            namespace = mode_str.split('.')
            database = namespace[0]
            collection = namespace[1]
            ident = None
            coll_indexes = []
        except IndexError:
            util_usage()
    else:
        util_usage()

    #Debug connection
    # conn = wiredtiger_open(uri, 'create, cache_size=512M, session_max=33000, eviction=(threads_min=4,threads_max=4), config_base=false, statistics=(fast), log=(enabled=true,archive=true,path=journal,compressor=snappy), file_manager=(close_idle_time=100000,close_scan_interval=10,close_handle_minimum=250), statistics_log=(wait=0), verbose=(version), compatibility=(release="3.3", require_min="3.2.0")')
    
    try:
        conn = wiredtiger_open(uri, "log=(enabled=true,path=journal,compressor=snappy),readonly=true,builtin_extension_config=(zstd=(compression_level=6))")
    except _wiredtiger.WiredTigerError as e:
        print(f"Connection error ({e})")
    else:
        catalog_table  = WTable(conn, ident = "_mdb_catalog", ttype = "c")
        catalog = catalog_table.get_ks_vs()


        if mode == "metadata":
            print("\n__MongoDB catalog content (_mdb_catalog.wt):\n")
            for k,v in catalog.items():
                if 'md' in v:
                    print(f"ident: {v['ident']}\n\tnamespace: {v['md']['ns']}")
                    if "indexes" in v["md"]:
                            for i,j in enumerate(v["md"]["indexes"],1):
                                name = j["spec"]["name"]
                                print(f"\tindex {i}: \n\t\tname: {j['spec']['name']}\n\t\tkey: {j['spec']['key']}\n\t\tready: {j['ready']}\n\t\tident: {v['idxIdent'][name]}")

            cursor = catalog_table.get_new_cursor(uri_mode="metadata")

            print("\n__WiredTiger catalog content (WiredTiger.wt):\n")
            while cursor.next() == 0:
                if "file:" in cursor.get_key():

                    #Key
                    key = cursor.get_key()
                    ident = re.sub(r"^file:|\.wt$", "", key)

                    #Values
                    value = cursor.get_value()

                    fileid = re.search(r'id=(\d+)', value)
                    log = re.search(r'enabled=([\w\d]+)', value)
                    prefix_compression = re.search(r'prefix_compression=([\w\d]+)', value)
                    memory_page_max = re.search(r'memory_page_max=([\w\d]+)', value)
                    leaf_page_max = re.search(r'leaf_page_max=([\w\d]+)', value)
                    leaf_value_max = re.search(r'leaf_value_max=([\w\d]+)', value)

                    compressor_match = re.search(r'block_compressor=(\w*)', value)
                    compressor = "none" if len(compressor_match.group(1)) == 0 else compressor_match.group(1)

                    print(f"ident: {ident}\n\tfileid: {fileid.group(1)}\n\tlog: {log.group(1)}\n\tcompressor: {compressor}\n\tprefix compression: {prefix_compression.group(1)}\n\tmemory max page: {memory_page_max.group(1)}\n\tleaf max page: {leaf_page_max.group(1)}\n\tleaf max value: {leaf_value_max.group(1)}")

            catalog_table.close_session()

        if mode == "log":
            cursor = catalog_table.get_new_cursor(uri_mode="log")

            print("\nJournal content:")
            while cursor.next() == 0:
                log_file, log_offset, opcount = cursor.get_key()
                txnid, rectype, optype, fileid, logrec_key, logrec_value = cursor.get_value()

                    # if optype == 4:  # Assuming WT_LOGREC_MESSAGE corresponds to 1
                if fileid != 0:
                    try:
                        bson_obj = bson.decode_all(logrec_value)
                        bson_obj = pprint.pformat(bson_obj, indent=1).replace('\n', '\n\t  ')
                        
                        print(f"LSN:[{log_file}][{log_offset}].{opcount}:\n\trecord type: {rectype}\n\toptype: {optype}\n\ttxnid: {txnid}\n\tfileid: {fileid}\n\tkey-hex: {logrec_key.hex()}\n\tvalue-hex: {logrec_value}\n\tvalue-bson: {bson_obj}")
                    except Exception as e:
                        key = logrec_key.hex()
                        value = logrec_value.hex()

                        key += value[:4]
                        value = value[-2:]

                        ksdecode = subprocess.run(
                            [
                                os.path.join(os.path.dirname(__file__) or '.',"ksdecode"),
                                "-o",
                                "bson",
                                "-t",
                                value,
                                "-r",
                                "long",
                                key,
                            ],
                            capture_output=True, check=True
                        )
                        key, value = ksdecode.stdout.decode("utf-8").strip().split(",")

                        print(f"LSN:[{log_file}][{log_offset}].{opcount}:\n\trecord type: {rectype}\n\toptype: {optype}\n\ttxnid: {txnid}\n\tfileid: {fileid}\n\tkey-hex: {logrec_key.hex()}\n\tkey-decode: {key}\n\tvalue-hex: {logrec_value}\n\tvalue-decode: {value}")
                        
            cursor.close()

        if mode == "coll_dump":
            for k,v in catalog.items():
                if 'md' in v and v["md"]["ns"] == database + "." + collection:
                        ident = v["ident"]
                        if "indexes" in v["md"]:
                            for index in v["md"]["indexes"]:
                                name = index["spec"]["name"]
                                coll_indexes.append({
                                    "key": index["spec"]["key"],
                                    "name": name,
                                    "ident": v["idxIdent"][name]
                                })
            if ident:
                coll_table  = WTable(conn, ident = ident, ttype = "c")
                coll_documents = coll_table.get_ks_vs()

                if coll_documents:
                    for k, v in coll_documents.items():
                        print(f"-- RecordID: {k}, document: {v}")
                else:
                    print("-- 0 Documents")

                coll_table.close_session()
                
            if coll_indexes:
                for index in coll_indexes:
                    index_table = WTable(conn, ident = index["ident"], ttype = "i")

                    for k, v in index_table.get_ks_vs(idx_key = str(index["key"])).items():
                        print(f"-- KeyString: {{ {k[1:].strip()} }}, RecordID: {v.split(':')[1][:-1].strip()}")

                    index_table.close_session()
            
        if mode == "coll_dump" and not ident:
            print(f"\nNo collection found ({collection})")

if __name__ == "__main__":
    main()