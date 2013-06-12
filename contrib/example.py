#!/usr/bin/env python
import sys
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

node = Node( {'use_hostbased' : True} )


with settings(
        hide('running'), 
        output_prefix=''
        ):
            gpfs.build_cluster_state()
            gpfs.get_gpfs_managers()
            #gpfs.get_node_kernel_and_arch()
            #gpfs.get_node_gpfs_baserpm()
            #gpfs.get_node_gpfs_verstring()

            # don't die on me here...
            #reboot(240)
            #print state
            #gpfs.get_node_gpfs_state()
            #print state['nodes'][env.host]['gpfs_state']

print state

