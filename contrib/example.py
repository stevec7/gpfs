#!/usr/bin/env python
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

nobj = Node( {'use_hostbased' : True} )

env.hosts = ['ddn42-vm1']

with settings(
        hide('running'), 
        output_prefix=''
        ):
            execute(cluster.build_cluster_state)
            execute(cluster.get_managers)

env.hosts = state['nodes'].keys()

with settings(
        parallel=True,
        #pool_size=8,
        #linewise=True,
        ):

        # get kernel vers and arch
        execute(nobj.get_kernel_and_arch)
        execute(nobj.get_gpfs_baserpm)
        execute(nobj.get_gpfs_verstring)
    
        epoch = time.mktime(time.gmtime())
        state['nodes'][env.host]['updated'] = epoch

        print "Host: %s" % (env.host)
        print "Time: %s" % (epoch)
#for n in state['nodes'].keys():
#    kernel_vers, arch = node.get_kernel_and_arch()
#    state['nodes'][node]['kernel_vers'] = kernel_vers
#    state['nodes'][node]['arch'] = arch
            #gpfs.get_node_kernel_and_arch()
            #gpfs.get_node_gpfs_baserpm()
            #gpfs.get_node_gpfs_verstring()

            # don't die on me here...
            #reboot(240)
            #print state
            #gpfs.get_node_gpfs_state()
            #print state['nodes'][env.host]['gpfs_state']

print "\n\nddn42-vm1:"
print state['nodes']['ddn42-vm1']
print "\n\nddn42-vm2:"
print state['nodes']['ddn42-vm2']

print state
