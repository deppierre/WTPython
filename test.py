def insert_records(table, records):
    for record in records.keys():
        key = record
        value = records[record]

        print(key,value)

items = {"key1":"value1","key2":"value2"}


items = { 
    "1":
    {
        'md': {
            'ns': 'newNs', 
            'options': {'uuid': 'binary' }
        }, 
        'ns': 'newNs', 
        'ident': 'wt.collection'
    }
}

insert_records("test", items)