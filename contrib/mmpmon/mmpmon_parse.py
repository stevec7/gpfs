#!/usr/bin/env python
import gpfs
import sys

f = open(sys.argv[1], 'r')

for line in f:
    print line
