#!/usr/bin/env python
import argparse
import datetime
import matplotlib
import matplotlib.pyplot as pyp
import os
import subprocess
import sys
from collections import defaultdict

def yodict():
    return defaultdict(yodict)

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

    data = yodict()
    mmpmon = yodict()
    _INTERVAL = args.interval   # seconds
    _DATA_FIELDS = ['_br_', '_bw_', '_oc_', '_cc_', '_rdc_', 
        '_wc_', '_dir_', '_iu_']

    if args.starttime:
        bucket_start = int(args.starttime)
    else:
        # get timestamp from beginning of file (messy, I know)
        with open(inputfile, 'r') as g:
            first_line = g.readline()
        bucket_start = int(first_line.split()[10])

    ticker = 1
    current_bucket = 0
    num_hosts = len(hostfilter)

    try:
        with open(inputfile, 'r') as f:
            for line in f:
                fields = line.split()

                if fields[0] != '_io_s_':
                    continue
                elif fields[4] not in hostfilter:
                    continue

                # create temporary dictionary
                cdata = dict(zip(fields[1::2], fields[2::2]))   # "current data"
                host = cdata['_nn_']
                t = int(cdata['_t_'])

                # compute the buckets
                #current_bucket = (t - bucket_start) / _INTERVAL
                previous_bucket = current_bucket - 1

                # create a filtered dictionary of attributes we want to store
                cdc = dict((k,int(v)) for k, v in cdata.iteritems() if k in _DATA_FIELDS)
                # first entry for every host in data defaultdict
                if current_bucket == 0:
                    data[current_bucket][host] = cdc
                    mmpmon[current_bucket][host] = cdc

                else:
                    try:
                        prev = data[previous_bucket][host]
                        #print current_bucket, line
                        #print cdc
                        #print prev
                        delta = dict((k,int(cdc[k]) - int(prev[k])) for k in cdc)
                   
                        data[current_bucket][host] = cdc

                        # now set the data in the mmpmon_d dictionary
                        mmpmon[current_bucket][host] = delta
                    except TypeError as te:
                        continue

                # properly enumarate the bucket numbers
                ticker += 1
                if ticker > num_hosts:
                    ticker = 1
                    current_bucket += 1

        #from IPython import embed; embed()
    except IOError as ioe:
        print "Error opening input file: {0}".format(ioe)
        sys.exit(1)

    if args.topng:
        # make a tuple of two lists, x and y axises
        br = ([], [])
        bw = ([], [])
        tbr = ([], [])
        tbw = ([], [])

        for k, v in mmpmon.iteritems():
            total_br = 0
            total_bw = 0

            for node in sorted(v):
                br[0].append(k)
                bw[0].append(k)
                br[1].append(float(v[node]['_br_'])/(1048576))
                bw[1].append(float(v[node]['_bw_'])/(1048576))
                total_br += v[node]['_br_'] / 1048576
                total_bw += v[node]['_bw_'] / 1048576

            tbr[0].append(k)
            tbr[1].append(total_br)
            tbw[0].append(k)
            tbw[1].append(total_bw)

        # draw it up (2 plots, one with totals, one with ALL vals)
        pyp.plot(br[0], br[1])
        pyp.plot(bw[0], bw[1])

        pyp.xlabel('Interval buckets ({0} secs)'.format(_INTERVAL))
        pyp.ylabel('MB/s')
        pyp.legend(['R', 'W'], loc='upper left')

        # save the first figure
        pyp.savefig(args.topng + ".png")

        pyp.plot(tbr[0], tbr[1])
        pyp.plot(tbw[0], tbw[1])
        pyp.xlabel('Interval buckets ({0} secs)'.format(_INTERVAL))
        pyp.ylabel('MB/s')
        pyp.legend(['tR', 'tW'], loc='upper left')
        pyp.savefig(args.topng + "_total.png")


            
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-H','--hostfile',
                        dest='hostfile',
                        required=False,
                        help='*file* is a list of hosts (1 per line) \
                        of the hosts you\'d like to capture. All other hosts \
                        are filtered out.')
    parser.add_argument('-f', '--file',
                        dest='inputfile',
                        required=True,
                        help='path to input file containing an mmpmon trace.')
    parser.add_argument('-i', '--interval',
                        dest='interval',
                        required=True,
                        type=int,
                        help='interval in which mmpmon data was collected')
    parser.add_argument('-e','--end',
                        dest='endtime',
                        required=False,
                        help='Dont collect data after YYYY-MM-DD_HH:MM:SS')
    parser.add_argument('-s','--start',
                        dest='starttime',
                        required=False,
                        help='Dont collect data before YYYY-MM-DD_HH:MM:SS')
    parser.add_argument('--topng',
                        dest='topng',
                        required=False,
                        help='write plot to a png')
    args = parser.parse_args()

    main(args)


