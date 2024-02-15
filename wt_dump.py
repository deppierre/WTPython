#!/usr/bin/env python
# Example of commands:
#   ./wt_dump.py -m mytestdb2.colleyction /data/db
#   ./wt_dump.py -m log /data/db
# To setup a lab:
#   mlaunch init --single --wiredTigerCacheSizeGB 0.5 --host localhost --port 27017
# Import data:
#   mongoimport "mongodb://localhost:27017" -d test -c collection /Users/pdepretz/0_m/tests/people.json

# Demo:
# 1- catalog is the central piece: WT only knows an object called ident, a table. So first MDB has to go through the catalog to identify the table supporting your index and collection. The catalog is updated everytime you create a new collection or index. MDB choose the ident name by itself, so WT will just create it using the name provided by MDB.
# 2- dump ns. thats how we can map our indexes with collection. MDB will test the indexes in the catalog to select the best plan
# 3- you can pass a bunch of options to WT, and some of them can be applied at the table level like compression, page size etc ... notice different between coll and index.

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

                keystring, record_id = decode_keystring(key, value, idx_key)
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

def decode_keystring(key, value, idx_key):
    """Function to decode a keystring"""
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

    return keystring, record_id
    
def util_usage():
    print("Usage: wt_dump -m {<ns>|<wtcatalog>|<mdbcatalog>|<log>} <uri>")
    print("Example: wt_dump -m mydb.mycollection data/db")
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
    elif mode_str == "wtcatalog":
        mode = "wtmetadata"
    elif mode_str == "mdbcatalog":
        mode = "mdbmetadata"
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
        mdb_catalog = {}
        wt_catalog = {}
        logs = []

        #METADATA MDB
        catalog_table  = WTable(conn, ident = "_mdb_catalog", ttype = "c")
        catalog = catalog_table.get_ks_vs()

        for k,v in catalog.items():
            if 'md' in v:
                mdb_catalog[v['ident']] = {
                    "ns": v['md']['ns'],
                    "indexes": {}
                }

                if "indexes" in v["md"]:
                    for i in v["md"]["indexes"]:
                        name = i["spec"]["name"]
                        
                        mdb_catalog[v['ident']]["indexes"][name] = {
                            "ready": i['ready'],
                            "key": i["spec"]["key"],
                            "ident": v['idxIdent'][name]
                        }

        #METADATA WT
        wt_cursor = catalog_table.get_new_cursor(uri_mode="metadata")
        
        while wt_cursor.next() == 0:
            if "file:" in wt_cursor.get_key():
                #Key
                key = wt_cursor.get_key()
                ident = re.sub(r"^file:|\.wt$", "", key)

                #Values
                value = wt_cursor.get_value()
                fileid = re.search(r'id=(\d+)', value)
                compressor_match = re.search(r'block_compressor=(\w*)', value)
                compressor = "none" if len(compressor_match.group(1)) == 0 else compressor_match.group(1)


                wt_catalog[ident] = {
                    "fileid": fileid.group(1),
                    "log": re.search(r'enabled=([\w\d]+)', value).group(1),
                    "prefix_compression": re.search(r'prefix_compression=([\w\d]+)', value).group(1),
                    "memory_page_max": re.search(r'memory_page_max=([\w\d]+)', value).group(1),
                    "leaf_page_max": re.search(r'leaf_page_max=([\w\d]+)', value).group(1),
                    "leaf_value_max": re.search(r'leaf_value_max=([\w\d]+)', value).group(1),
                    "compressor": compressor
                }

        #LOG
        log_cursor = catalog_table.get_new_cursor(uri_mode="log")
        while log_cursor.next() == 0:
            log_file, log_offset, opcount = log_cursor.get_key()
            txnid, rectype, optype, fileid, logrec_key, logrec_value = log_cursor.get_value()

            logs.append([log_file, log_offset, opcount, txnid, rectype, optype, fileid, logrec_key, logrec_value])
        
        log_cursor.close()

        catalog_table.close_session()

        if mode == "mdbmetadata":
            for k,v in mdb_catalog.items():
                print(f"namespace: {v['ns']}\n\tident: {k}")

                if "indexes" in v:
                    for i,j in v["indexes"].items():
                        name = i
                        print(f"\tindex {i}: \n\t\tname: {name}\n\t\tkey: {j['key']}\n\t\tready: {j['ready']}\n\t\tident: {j['ident']}")

        if mode == "wtmetadata":
            for k,v in wt_catalog.items():
                print(f"ident: {k}\n\tfileid: {v['fileid']}\n\tlog: {v['log']}\n\tcompressor: {v['compressor']}\n\tprefix compression: {v['prefix_compression']}\n\tmemory max page: {v['memory_page_max']}\n\tleaf max page: {v['leaf_page_max']}\n\tleaf max value: {v['leaf_value_max']}")

        if mode == "log":
            for log in logs:
                # if optype == 4:  # Assuming WT_LOGREC_MESSAGE corresponds to 1
                if log[6] != 0:
                    try:
                        bson_obj = bson.decode_all(log[8])
                        bson_obj = pprint.pformat(bson_obj, indent=1).replace('\n', '\n\t  ')
                        
                        print(f"LSN:[{log[0]}][{log[1]}].{log[2]}:\n\trecord type: {log[4]}\n\toptype: {log[5]}\n\ttxnid: {log[3]}\n\tfileid: {log[6]}\n\tkey-hex: {log[7].hex()}\n\tvalue-hex: {log[8]}\n\tvalue-bson: {bson_obj}")
                    except Exception as e:
                        key = log[7].hex()
                        value = log[8].hex()

                        key += value[:4]
                        value = value[-2:]

                        print({log[6]})

                        key, value = decode_keystring(
                            key,
                            value,
                            "{'_id': 1}"
                        )

                        print(f"LSN:[{log[0]}][{log[1]}].{log[2]}:\n\trecord type: {log[4]}\n\toptype: {log[5]}\n\ttxnid: {log[3]}\n\tfileid: {log[6]}\n\tkey-hex: {log[7].hex()}\n\tkey-decode: {key}\n\tvalue-hex: {log[8]}\n\tvalue-decode: {value}")

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