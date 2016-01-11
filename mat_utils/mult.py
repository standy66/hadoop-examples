#! /usr/bin/env python3

import sys

def read_matrix(path):
    A = []
    with open(path) as f:
        content = f.readlines()
        for line in content:
            idx1, idx2, other = line.split()
            i = int(idx1)
            j = int(idx2)
            val = int(other)
            if (i >= len(A)):
                A.append([])
            A[i].append(val)
    return A

A = read_matrix(sys.argv[1])
B = read_matrix(sys.argv[2])
if (len(A[0]) != len(B)):
    raise RuntimeError("matrices can't be multiplied")

n = len(A)
m = len(B)
k = len(B[0])

C = [[0 for j in range(0, k)] for i in range(0, n)]

for i in range(0, n):
    for j in range(0, k):
        for p in range(0, m):
            C[i][j] += A[i][p] * B[p][j]

for i in range(0, n):
    for j in range(0, k):
        print("%d %d\t%d" % (i, j, C[i][j]))
