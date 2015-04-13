#!/usr/bin/env python

import sys

with open(sys.argv[1], 'r') as inp:
    vals = [int(v.strip()) for v in inp.readlines()]
    assert len(vals) > 0

    last = vals[0]
    for v in vals[1:]:
        if v != last+1:
            sys.stderr.write("Found sequence gap: {0} to {1}\n".format(last, v))
        last = v
