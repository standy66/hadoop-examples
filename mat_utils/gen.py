#! /usr/bin/env python3

import sys
import random

width = int(sys.argv[1])
height = int(sys.argv[2])

for i in range(0, width):
    for j in range(0, height):
        val = random.choice([0, 1])
        print("%d %d\t%d" % (i, j, val))
