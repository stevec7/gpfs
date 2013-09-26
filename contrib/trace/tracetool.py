#!/usr/bin/env python
#
#
import argparse
import datetime
import gzip
import json
import sys
import zlib
from collections import defaultdict
from gpfs.analyze import TraceParser
from IPython import embed

def tree():
    return defaultdict(tree)

def main(args):

    filters = args.filters

    if args.traceinput:     # this currently crashes on my vm...
        j = gzip.open(args.traceinput, 'r')
        tracelog = json.load(j)
        parser = TraceParser(tracelog, args.verbose)
    else:
        #tracelog = lambda: defaultdict(tracelog)   # look how cool I am
        tracelog = tree()
        parser = TraceParser(tracelog, args.verbose)
        parser.parse_trace(args.filename, filters)

    # write the io dictionary to a compressed file in json format
    if args.tojson:
        gzipout = gzip.open(args.tojson, 'wb')
        json.dump(tracelog, gzipout)
        gzipout.close()

    if args.printsum:
        parser.print_disk_summary()
        parser.print_network_summary()

    if args.interactive:
        embed()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--filename',
                        dest='filename',
                        required=False,
                        help='filename of the trace to open.')
    parser.add_argument('-i', '--interactive',
                        dest='interactive',
                        required=False,
                        action='store_true',
                        default=False,
                        help='interactive mode. open an ipython shell at the end...')
    parser.add_argument('-t', '--traceinput',
                        dest='traceinput',
                        required=False,
                        help='instead of reading a tracefile, pass in a json dump.')
    parser.add_argument('--filters',
                        dest='filters',
                        required=False,
                        default='io',
                        help='command sep list of filters. Valid values: ' + \
                            'io,ts,rdma,brl')
    parser.add_argument('--print',
                        dest='printsum',
                        required=False,
                        help='Print summaries in a comma separated list. Valid values: ' + \
                            'io,ts,rdma,brl')
    parser.add_argument('--tojson',
                        dest='tojson',
                        required=False,
                        help='write dictionary to a json file')
    parser.add_argument('-v', '--verbose',
                        dest='verbose',
                        default=False,
                        required=False,
                        action='store_true',
                        help='show verbose stats (can be spammy)')
    args = parser.parse_args()

    main(args)
