#!/usr/bin/env python3

import sys


cur_key, cur_replicas, cur_words = "", 0, 0
for line in sys.stdin:
    key, value = line.split('\t', 1)
    replicas, words = value.split(maxsplit=1)
    replicas = int(replicas)
    words = int(words)
    if key == cur_key:
        cur_replicas += replicas
        cur_words += words
    else:
        if cur_replicas > 0:
            print("%s\tREP:%d, WORDS:%d, AVG:%f" % (cur_key, cur_replicas, cur_words, cur_words * 1.0 / cur_replicas))
        cur_key, cur_replicas, cur_words = key, replicas, words
print("%s\t REP: %d, WORDS: %d, AVG: %f" % (cur_key, cur_replicas, cur_words, cur_words * 1.0 / cur_replicas))
