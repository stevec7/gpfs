#!/usr/bin/env python
import argparse
import json
import operator
import re
import sys
import gpfs.node
from fabric.tasks import execute
from fabric.api import env, local, run, parallel
from fabric.context_managers import hide, show, settings



def main(args):
    """This will print out a list of nodes in groups"""


    _NUM_GROUPS=args.numgroups
    dryrun = args.dryrun
    state = json.load(open(args.jsonfile))
    gpfsvers = args.gpfsvers

    # check gpfs version arg
    if not re.match('^\d+\.\d+\.\d+\-\d+', gpfsvers):
        raise SystemError('gpfs_version arg must be in format: X.Y.Z-V')

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

    # create node object to pass state dict to
    gpfsnode = gpfs.node.Node(
                    state,
                    {'use_hostbased' : True}
                )

    for group in nodequeue.itervalues():
        print "===== GROUP ====="
        members = [ str(member['short_name']) for member in group ]
        print "=== Members: {0}".format(members)

        # set this nodes status to 'starting'
        for m in members:
            gpfsnode.update_node_key(m, 'action_status', 'starting')

        env.hosts = members
        env.parallel = True
        env.use_hostbased = True

        #kwargs = {'reboot_node' : False, 
        #            'dry_run': True, 
        #            'gpfs_version' : 
        #            '3.5.0-7'
        #            }
        
        updated_state = execute(gpfsnode.update_gpfs_software, gpfsvers,
                False, False)

        # since this is being run in parallel, the state dict isnt being updated.
        #   specifying a return value returns a dict in the following format:
        #   {'nodename': whatever_returned_from_method}. In this case, the
        #   method returns a dictionary, for EACH node, a la: 
        #   {'nodename': dict, 'nodename2': dict, etc...}
        #
        #   so, update the global state dictionary...
        for k, v in updated_state.iteritems():
            state['nodes'][k] = v

    # dump this json for now to look at results...
    json.dump(state, open('/tmp/dry_update.json', 'w'))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='create a node queue from a json \
                                    file for experimenting. The -n flag will \
                                    organize nodes into groups, putting the \
                                    highest score nodes (quorum-manager, \
                                    quorum, manager, client) on top first')
    parser.add_argument('-d', '--dryrun',
                        default=True,
                        dest='dryrun',
                        required=False,
                        type=bool,
                        help='dryrun, just show what would\'ve been done...')
    parser.add_argument('-f', '--file',
                        dest='jsonfile', 
                        required=True,
                        help='json file to load')
    parser.add_argument('-g', '--gpfsvers', 
                        dest='gpfsvers', 
                        required=True, 
                        help='Version of GPFS to update to. Ex: -g \'3.5.0-11\'')
    parser.add_argument('-n', '--numgroups', 
                        dest='numgroups', 
                        required=True, 
                        type=int,
                        help='number of groups for the nodes.')
    args = parser.parse_args()

    main(args)
