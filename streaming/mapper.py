#!/usr/bin/env python3

import sys

for line in sys.stdin:
    key, replica = line.split('\t')
    words = replica.split()
    print("%s\t%d %d" %(key, 1, len(words)))