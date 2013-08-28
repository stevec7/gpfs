#!/usr/bin/env python
import argparse
import json
import operator
import re
import sys
import gpfs.node
import gpfs.cluster
import gpfs.nodequeue
from collections import defaultdict
from fabric.tasks import execute
from fabric.api import env, local, run, parallel
from fabric.context_managers import hide, show, settings

def tree():
    return defaultdict(tree)


def main(args):
    """This will print out a list of nodes in groups"""


    _NUM_GROUPS = args.numgroups
    _MAX_FAILURES = args.maxfails
    dryrun = args.dryrun
    gpfsvers = args.gpfsvers
    rebootnodes = args.rebootnodes
    host = args.host
    total_failures = 0

    # check gpfs version arg
    if not re.match('^\d+\.\d+\.\d+\-\d+', gpfsvers):
        raise SystemError('gpfs_version arg must be in format: X.Y.Z-V')

    # grab the GPFS cluster state
    state = tree()
    env.hosts = [ args.host, ]
    env.use_hostbased = True
    cluster = gpfs.cluster.GPFSCluster(state)

    # this builds a complete GPFS cluster state defaultdict
    with settings(
            hide('running'),
            output_prefix='',
            warn_only=True
            ):
                execute(cluster.build_cluster_state)
                execute(cluster.get_managers)
                execute(cluster.get_all_kernel_and_arch)
                execute(cluster.get_all_gpfs_baserpm)


    # create a nodequeue
    nq = gpfs.nodequeue.NodeQueue(state)
    nq.create_queue(_NUM_GROUPS)

    # create node object to pass state dict to
    gpfsnode = gpfs.node.Node(
                    state,
                    {'use_hostbased' : True}
                )

    for group in nq.nodequeue.itervalues():
        print "===== GROUP ====="
        members = [ str(member['short_name']) for member in group ]
        print "=== Members: {0}".format(members)

        # set this nodes status to 'starting'
        for m in members:
            gpfsnode.update_node_key(m, 'action_status', 'starting')

        env.hosts = members
        env.parallel = True
        env.use_hostbased = True

        updated_state = execute(gpfsnode.update_gpfs_software, gpfsvers,
                rebootnodes, dryrun )

        # since this is being run in parallel, the state dict isnt being updated.
        #   specifying a return value returns a dict in the following format:
        #   {'nodename': whatever_returned_from_method}. In this case, the
        #   method returns a dictionary, for EACH node, a la: 
        #   {'nodename': dict, 'nodename2': dict, etc...}
        #
        #   so, update the global state dictionary...
        for k, v in updated_state.iteritems():
            state['nodes'][k] = v

            if state['nodes'][k]['failed'] == 1:
                total_failures = total_failures + 1

        if total_failures > 1:
            print "Total failures: {0}".format(total_failures)


        if total_failures > _MAX_FAILURES:
            print "ERROR: Node failures ({0}) > maxnodefailures ({1})".format(total_failures,
                    _MAX_FAILURES)
            print "Exiting..."
            sys.exit(1)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run a rolling update on a \
                        a GPFS cluster.')
    parser.add_argument('-c', '--cm',
                        dest='host',
                        required=True,
                        help='the gpfs cluster manager you want to spawn this from.')
    parser.add_argument('-d', '--dryrun',
                        action='store_true',
                        dest='dryrun',
                        required=False,
                        help='dryrun, just show what would\'ve been done...')
    parser.add_argument('-g', '--gpfsvers', 
                        dest='gpfsvers', 
                        required=True, 
                        help='Version of GPFS to update to. Ex: -g \'3.5.0-11\'')
    parser.add_argument('-n', '--numgroups', 
                        dest='numgroups', 
                        required=True, 
                        type=int,
                        help='number of update groups for the nodes.')
    parser.add_argument('-r', '--reboot_nodes', 
                        action='store_true',
                        default=False,
                        dest='rebootnodes', 
                        required=False,
                        help='reboot node after software update')
    parser.add_argument('--maxfailures',
                        default=2,
                        dest='maxfails',
                        required=False,
                        type=int,
                        help='max number of update failures to tolerate.')
    args = parser.parse_args()

    main(args)
