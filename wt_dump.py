#!/usr/local/bin/python

import sys, subprocess, os, pprint, re
import pymongo, bson
from regex import P
from wiredtiger import wiredtiger_open,WIREDTIGER_VERSION_STRING,stat,_wiredtiger

class WTable(object):
    """Class to dump a WTable"""
    def __init__(self, conn, ident = None, ttype = None):

        self.ident = ident
        self.type = ttype

        self.__session = conn.open_session()

        if self.ident != "_mdb_catalog" and ttype is not None:
            print(f"\nNew Session ({ident}):")

    def checkpoint_session(self):
        """Function to Checkpoint a session"""
        
        print("Checkpoint done")
        return self.__session.checkpoint()

    def get_new_cursor(self, uri = None):
        """Function to create a new cursor"""

        if uri == "statistics":
            return self.__session.open_cursor(f"statistics:table:{self.ident}", None, "append")
        elif uri == "log":
            return self.__session.open_cursor("log:")
        elif uri == "metadata":
            return self.__session.open_cursor("metadata:")
        elif uri == "hs":
            return self.__session.open_cursor("file:WiredTigerHS.wt", None, 'checkpoint=WiredTigerCheckpoint')
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

                if keystring in k_v:
                    k_v[keystring].append(record_id)
                else:
                    k_v[keystring] = [ record_id ]

        cursor.close()
        return k_v

    def get_stats(self):
        """Function to get stats of a table"""
        
        cursor = self.get_new_cursor(uri="statistics")

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

    def verify(self):
        return self.__session.verify(f"table:{self.ident}", "dump_address")

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

    keystring, record_id = None, None


    try:
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
        if ksdecode.returncode == 0:
            output = ksdecode.stdout.decode("utf-8").strip().split(",")

            record_id = output[-1].replace("}", "").strip()
            keystring = ",".join(output[0:-1]) + " }"

    except subprocess.CalledProcessError:
        pass
        # print(f"Error Keystring decoding {key, value, idx_key}")

    return keystring, record_id
    
def util_usage():
    """Function to print help"""
    print("Usage: ./wt_dump.py -m {<ns>|<wc>|<mc>|<l>} <uri>")
    print("\t -ns: a namespace name")
    print("\t -wc: WiredTiger catalog")
    print("\t -mc: MongoDB catalog")
    print("\t -l: WiredTiger log file")
    print("\t -uri: Local directory (.) by default")
    print("Example: ./wt_dump.py -m mydb.mycollection /data")
    sys.exit(1)

def main():
    """Main function"""
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        util_usage()
        exit()

    if sys.argv[1] != '-m':
        print('A mode must be specified with -m.')
        util_usage()

    mode_str = sys.argv[2]
    uri = "data/db" if len(sys.argv) < 4 else sys.argv[3]

    if mode_str == "l":
        mode = "log"
    elif mode_str == "wc":
        mode = "wtmetadata"
    elif mode_str == "mc":
        mode = "mdbmetadata"
    elif mode_str == "t":
        mode = "test"
    elif '.' in mode_str:
        mode = "coll_dump"
        try:
            namespace = mode_str
            ident = None
            coll_indexes = []
        except IndexError:
            util_usage()
    else:
        util_usage()

    try:
        conn = wiredtiger_open(uri, "log=(enabled=true,path=journal,compressor=snappy),readonly=true,statistics=(all)")
    except _wiredtiger.WiredTigerError as e:
        print(f"Connection error ({e})")
    else:
        mdb_catalog = {}
        wt_catalog = {}
        wt_hs = {}
        logs = []

        #METADATA MDB
        new_catalog_  = WTable(conn, ident = "_mdb_catalog", ttype = "c")
        catalog = new_catalog_.get_ks_vs()

        for k,v in catalog.items():
            if 'md' in v:
                mdb_catalog[v['ident']] = {
                    "ns": v['md']['ns'],
                    "UUID": v['md']['options']['uuid'],
                    "indexes": {}
                }

                if "indexes" in v["md"]:
                    for i in v["md"]["indexes"]:
                        name = i["spec"]["name"]
                        try:
                            unique = i["spec"]["unique"]
                        except KeyError:
                            unique = False
                        
                        mdb_catalog[v['ident']]["indexes"][name] = {
                            "ready": i['ready'],
                            "key": i["spec"]["key"],
                            "unique": unique,
                            "ident": v['idxIdent'][name],
                            "multikeyPaths": i["multikeyPaths"]
                        }

        #METADATA WT
        wt_cursor = new_catalog_.get_new_cursor(uri="metadata")
        
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
        log_cursor = new_catalog_.get_new_cursor(uri="log")
        while log_cursor.next() == 0:
            log_file, log_offset, opcount = log_cursor.get_key()
            txnid, rectype, optype, fileid, logrec_key, logrec_value = log_cursor.get_value()

            logs.append([log_file, log_offset, opcount, txnid, rectype, optype, fileid, logrec_key, logrec_value])
        log_cursor.close()


        #HS
        try:
            wt_hs_cursor = new_catalog_.get_new_cursor(uri="hs")

            if len(list(wt_hs_cursor)) > 0:
                for _, _, hs_start_ts, _, hs_stop_ts, _, type, value in wt_hs_cursor:
                    print(_, _, hs_start_ts, _, hs_stop_ts, _, type, value)
            else:
                print("History store is empty")
        except _wiredtiger.WiredTigerError as e:
            print(f"Catalog error\n{e}")

        new_catalog_.close_session()

        if mode == "test":
            print("Test mode")
            pass

        if mode == "mdbmetadata":
            for k,v in mdb_catalog.items():
                print(f"namespace: {v['ns']}\n\tident: {k}\n\tuuid: {v['UUID'].hex()}")

                if "indexes" in v:
                    for i,j in v["indexes"].items():
                        name = i
                        print(f"\tindex {i}:\
                              \n\t\tname: {name}\
                              \n\t\tkey: {j['key']}\
                              \n\t\tready: {j['ready']}\
                              \n\t\tunique: {j['unique']}\
                              \n\t\tmultikeyPaths: {j['multikeyPaths']}\
                              \n\t\tident: {j['ident']}")

        if mode == "wtmetadata":
            for k,v in wt_catalog.items():
                print(f"ident: {k}\
                      \n\tfileid: {v['fileid']}\
                      \n\tlog: {v['log']}\
                      \n\tcompressor: {v['compressor']}\
                      \n\tprefix compression: {v['prefix_compression']}\
                      \n\tmemory max page: {v['memory_page_max']}\
                      \n\tleaf max page: {v['leaf_page_max']}\
                      \n\tleaf max value: {v['leaf_value_max']}")

        if mode == "log":
            ident = None
            print("Last Transaction (txnid):\n")

            for log in logs:
                fileid = log[6]
                current_LSN = log[1]
                bson_hex = log[8]

                if fileid != 0:
                    if bson.is_valid(bson_hex):
                    
                        bson_obj = bson.decode_all(bson_hex)
                        bson_obj = pprint.pformat(bson_obj, indent=1).replace('\n', '\n\t  ')
                        
                        print(f"LSN:[{log[0]}][{current_LSN}].{log[2]}:\
                              \n\trecord type: {log[4]}\
                              \n\toptype: {log[5]}\
                              \n\ttxnid: {log[3]}\
                              \n\tfileid: {fileid}\
                              \n\tkey-hex: {log[7].hex()}\
                              \n\tvalue-hex: {bson_hex.hex()}\
                              \n\tvalue-bson: {bson_obj}")
                    else:
                        try:
                            key = log[7].hex()
                            value = bson_hex.hex()

                            key += value[:4]
                            value = value[-2:]

                            ident = [ k for k,v in wt_catalog.items() if int(v["fileid"].strip()) == fileid ][0]

                            for k,v in mdb_catalog.items():
                                for i,j in v["indexes"].items():
                                    if j["ident"] == ident:
                                        idx_key = str(j["key"])
                                        key, value = decode_keystring(
                                            key,
                                            value,
                                            idx_key
                                        )

                                        print(f"LSN:[{log[0]}][{current_LSN}].{log[2]}:\
                                            \n\trecord type: {log[4]}\
                                            \n\toptype: {log[5]}\
                                            \n\ttxnid: {log[3]}\
                                            \n\tfileid: {fileid}\
                                            \n\tkey-hex: {log[7].hex()}\
                                            \n\tkey-decode: {key}\
                                            \n\tvalue-hex: {bson_hex.hex()}\
                                            \n\tvalue-decode: {value}")
                        except:
                            print(f"LSN:[{log[0]}][{current_LSN}].{log[2]}: Unknown format")

        if mode == "coll_dump":
            ident = None
            for k,v in catalog.items():
                if 'md' in v and v["md"]["ns"] == namespace:
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
                        print(f"-- $recordId: {k}, {v}")
                else:
                    print("-- 0 Documents")

                coll_table.close_session()
                
            if coll_indexes:
                for index in coll_indexes:
                    index_table = WTable(conn, ident = index["ident"], ttype = "i")

                    for k, v in index_table.get_ks_vs(idx_key = str(index["key"])).items():
                        if k or v is not None:
                            if len(v) == 1:
                                print(f"-- KeyString: {k.strip()}, {v[0].strip()}")
                            elif len(v) > 1:
                                for recordid in v:
                                    print(f"-- KeyString: {k.strip()}, {recordid.strip()}")


                    index_table.close_session()
            
        if mode == "coll_dump" and not ident:
            print(f"\nNo collection found ({namespace})")

if __name__ == "__main__":
    DEBUG = False
    main()
    sys.exit(0)
