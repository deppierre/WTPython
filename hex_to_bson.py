#!/usr/bin/env python

import bson
import sys
import pprint

while True:
    line = sys.stdin.readline()
    if not line:
        print("No data section")
        sys.exit(1)
    line = line.strip()
    if line == 'Data':
        break
while True:
    key = sys.stdin.readline()
    if not key:
        break
    key = key.strip()
    value = sys.stdin.readline().strip()
    print('Key:\t%s' % (key,))
    byt = bytes.fromhex(value)
    obj = bson.decode_all(byt)[0]
    print('Value:\n\t%s' % (pprint.pformat(obj, indent=1).replace('\n', '\n\t'),))
