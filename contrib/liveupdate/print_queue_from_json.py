#!/usr/bin/env python
import argparse
import gpfs.nodequeue
import json
import operator
import sys



def main(args):
    """This will print out a list of nodes in groups"""


    _NUM_GROUPS=args.numgroups
    state = json.load(open(args.jsonfile))

    nq = gpfs.nodequeue.NodeQueue(state)

    nq.create_queue(_NUM_GROUPS)
    nq.print_queue()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='create a node queue from a json \
                                    file for experimenting. The -n flag will \
                                    organize nodes into groups, putting the \
                                    highest score nodes (quorum-manager, \
                                    quorum, manager, client) on top first')
    parser.add_argument('-f', '--file',
                        dest='jsonfile', 
                        required=True,
                        help='json file to load')
    parser.add_argument('-n', '--numgroups', 
                        dest='numgroups', 
                        required=True, 
                        type=int,
                        help='number of groups for the nodes.')
    args = parser.parse_args()

    main(args)
