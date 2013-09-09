#!/usr/bin/env python
import argparse
import datetime
import os
import subprocess
import sys
from collections import defaultdict
from gpfs import mmpmon as mmp

def tree():
    return defaultdict(tree)

def main(args):

    # from cmdline args
    inputfile = args.inputfile

    if args.hostfile:
        try:
            h = open(args.hostfile, 'r')
        except IOError as ioe:
            print "Error opening hosts file: {0}".format(ioe)
            sys.exit(1)
            
        hostfilter = [x.strip('\n') for x in h.readlines()]

    else:
        hostfilter = [] # blank

    # figure out which day of the week it is to pass the snapshot name
    #dow = days[datetime.datetime.today().weekday()] # total clown

    p = mmp.mmpmon()
    data = tree()
    iop = 0

    try:
        with open(inputfile, 'r') as f:
            for line in f:
                fields = line.split()

                if fields[0] != '_io_s_':
                    continue
                elif fields[4] not in hostfilter:
                    continue

                #print "Writing data for '{0}'".format(fields[4])
                data[iop] = dict(zip(fields[1::2], fields[2::2]))          
                iop += 1

        print data
            
    except IOError as ioe:
        print "Error opening input file: {0}".format(ioe)
        sys.exit(1)
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-H','--hostfile',
                        dest='hostfile',
                        required=False,
                        help='*file* is a list of hosts (1 per line) \
                        of the hosts you\'d like to capture. All other hosts \
                        are filtered out.')
    parser.add_argument('-i', '--input',
                        dest='inputfile',
                        required=True,
                        help='path to input file containing an mmpmon trace.')
    parser.add_argument('-e','--end',
                        dest='endtime',
                        required=False,
                        help='Dont collect data after YYYY-MM-DD_HH:MM:SS')
    parser.add_argument('-s','--start',
                        dest='starttime',
                        required=False,
                        help='Dont collect data before YYYY-MM-DD_HH:MM:SS')
    args = parser.parse_args()

    main(args)


