#!/usr/bin/env python
import argparse
import json
from collections import defaultdict
from gpfs.funcs import zscore, stddev

def tree():
    return defaultdict(tree)

def main(args):
    filename = args.filename

    try:
        f = open(filename, 'r')
    except IOError as ioe:
        print "Error opening '{0}': {1}".format(filename, ioe)

    pdisks = tree()
    header = f.readline().rstrip('\n')[0:-1]    # dont want newline or blank chars
    fields = header.split(':')


    for line in f:
        contents = line.split(':')
        index = contents[14]
        rg = contents[9]
        pdisks[rg]['pdisks'][index] = dict(zip(fields, contents))

    rg_rp = []  # recovery group relative performance
    rp_li = []  # recovery group list
    t_rel_perf = [] # total relative performance

    # couldnt figure out a list comprehension here...
    for rg, pd in pdisks.iteritems():
        for p in pd['pdisks'].keys():
            t_rel_perf.append(float(pdisks[rg]['pdisks'][p]['relativePerformance']))
            rg_rp.append(float(pdisks[rg]['pdisks'][p]['relativePerformance']))

        # get the stddev of the rg's relative perf
        pd['stats']['stddev'] = stddev(rg_rp)[0] # 2nd field [1] is variance
        pd['stats']['mean'] = sum(rg_rp) / len(rg_rp)

        rg_rp = []  # clear it out...
    
    t_rel_perf_sd = stddev(t_rel_perf)[0]
    max_zscore = 2

    # this again...
    #   check the standard scores per recovery group, and then against all disks
    print "PDisks (per RG) with a low relative performance"
    print "="*80
    for rg, pd in pdisks.iteritems():
        print "RG: {0}, Avg_Rel_Perf: {1}".format(rg, pd['stats']['mean'])
        for p, v in pd['pdisks'].iteritems():
            sscore = zscore(float(v['relativePerformance']), 
                float(pd['stats']['mean']), 
                float(pd['stats']['stddev']))
            #if sscore > max_zscore:
            if v['relativePerformance'] < 1.0:
                print "Pdisk: {0}, Rel_Perf: {1}, IOErrors: {2}, IOTimeouts: {3}, mediaErrors: {4}, checksumErrors: {5}, pathErrors: {6}".format(
                        v['pdiskName'], v['relativePerformance'], v['IOErrors'],
                        v['IOTimeouts'], v['mediaErrors'], v['checksumErrors'],
                        v['pathErrors'])
            

    #total_rp_stddev = stddev([ y[1] for x,y in enumerate(relativeperf) ])
    #for k, v in perf.iteritems():

         
    from IPython import embed; embed()
    #json.dump(pdisks, open('/tmp/pdisk.json', 'w'))

if __name__ == '__main__':

    parser = argparse.ArgumentParser('look for bad pdisks')
    parser.add_argument('-f', '--filename',
                        dest='filename',
                        required=True,
                        help='the filename containing all of the pdisk data, \
                        gathered via mmlspdisk all -Y.')
    args = parser.parse_args()
    main(args)
    
