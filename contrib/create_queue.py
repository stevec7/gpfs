#!/usr/bin/env python
import argparse
import json
import operator
import sys



def main(args):
    """This will print out a list of nodes in groups"""


    _NUM_GROUPS=args.numgroups
    state = json.load(open(args.jsonfile))

    nodelist = []

    for n in state['nodes'].itervalues():
        fg = n['fg']

        # get first failure group (should be fixed later)
        try:
            ffg = fg[0]
        except:
            ffg = 0

        nodelist.append((n['weight'], ffg , n)) 

    # sort the nodelist based on node weights (quorum/manager/client/etc)
    nodelist.sort(reverse=True)

    # create some nested lists that are separate node groups
    nodequeue = {}
    for k in range(0,_NUM_GROUPS):
        nodequeue[k] = []


    # here we take the nodelist and take an entry off the top and add it to its
    #   own nodequeue[int] group
    i = 0
    for n in nodelist:
        nodequeue[i].append(n[2])
        i=(i+1)%_NUM_GROUPS

    # add nodes (shortname) to a list of finished nodes...
    finished_queue = []
        
    for group in nodequeue.itervalues():
        print "===== GROUP ====="
        members = [ str(member['short_name']) for member in group ]
        print "=== Members: {0}".format(members)

        for node in group:
            print "Node: {0}, Roles: {1}, FG: {2}".format(
                    node['short_name'], node['roles'], node['fg'])

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
