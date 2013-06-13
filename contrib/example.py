#!/usr/bin/env python
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


def tree():
    return defaultdict(tree)

state = tree()
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
            #execute(cluster.get_all_gpfs_verstring)

#env.hosts = state['nodes'].keys()
env.hosts = []
for h in state['nodes'].keys():
    if state['nodes'][h]['gpfs_state'] == 'active':
        env.hosts.append(h)
    else:
        pass

#with settings(
#        parallel=False,
        #pool_size=8,
        #linewise=True,
#        ):

        # get kernel vers and arch
#        execute(nobj.get_gpfs_baserpm)
#        execute(nobj.get_gpfs_verstring)
    

_QUORUM_MANAGER_SCORE = 10
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
#   'queued', 'updating' 'rebooting', 'finished_update', ''
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
node_queue = sorted( [(i, state['nodes'][i]['weight']) \
                for i in state['nodes'].keys()], \
                key=operator.itemgetter(1) )

#print state
json.dump(state, open('/tmp/mirafs0state.txt', 'w'))

#print node_queue
#json.dump(node_queue, open('/tmp/node_queue.txt', 'w'))
