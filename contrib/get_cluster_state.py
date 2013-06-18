#!/usr/bin/env python
import argparse
import json
import operator
import sys
import time
from collections import defaultdict
from gpfs.cluster import GPFSCluster
from gpfs.node import Node
from fabric.api import run, execute, env
from fabric.context_managers import settings, hide, show
from fabric.operations import reboot


parser = argparse.ArgumentParser(description='grabs GPFS cluster state and dumps to a json file')
parser.add_argument('-f', '--file', dest='jsonfile', required=True,
            help='json file to dump to')
parser.add_argument('-n', '--node', dest='destnode', required=True, 
            help='node to grab cluster state from')
args = parser.parse_args()

def tree():
    return defaultdict(tree)

state = tree()
env.hosts = [ args.destnode, ]
env.use_hostbased = True
cluster = GPFSCluster(state)

nobj = Node( state, {'use_hostbased' : True} )


with settings(
        hide('running'), 
        output_prefix='',
        warn_only=True
        ):
            execute(cluster.build_cluster_state)
            execute(cluster.get_managers)
            execute(cluster.get_all_kernel_and_arch)
            execute(cluster.get_all_gpfs_baserpm)

# we don't want to run operations on hosts that are down, unknown, or arbitrating.
#   in fact, we should fix those problems before we start, :-)
env.hosts = []
for h in state['nodes'].keys():
    if state['nodes'][h]['gpfs_state'] == 'active':
        env.hosts.append(h)
    else:
        pass


_QUORUM_MANAGER_SCORE = 8
_QUORUM_SCORE = 5
_MANAGER_SCORE = 3
_CLIENT_SCORE = 1
_SIMULT_QUORUM_MANAGERS = 1

# now, create a 'plan' to do things:
#
#   - we should organize the nodes into a queue/list to update
#   and then assign a weight to each node based on it's roles
#   to update them intelligently
#
# assign a node weight and an action status, where action_status =
#   'queued', 'running_action' 'rebooting', 'finished_action', ''
for node in state['nodes'].keys():
    state['nodes'][node]['action_status'] = ''

    if state['nodes'][node]['roles'] == 'quorum-manager':
        state['nodes'][node]['weight'] = _QUORUM_MANAGER_SCORE
    elif state['nodes'][node]['roles'] == 'quorum':
        state['nodes'][node]['weight'] = _QUORUM_SCORE
    elif state['nodes'][node]['roles'] == 'manager':
        state['nodes'][node]['weight'] = _MANAGER_SCORE
    else:
        state['nodes'][node]['weight'] = _CLIENT_SCORE

# create a queue of nodes based on their weight
#   sort the nodelist by weight (asc)
#node_queue = sorted( [(i, state['nodes'][i]['weight']) \
#                for i in state['nodes'].keys()], \
#                key=operator.itemgetter(1) )

json.dump(state, open(args.jsonfile, 'w'))

