#!/usr/bin/env python
from collections import defaultdict
from gpfs import commands
from fabric.api import run, execute, env
from fabric.context_managers import settings, hide, show


def tree():
    return defaultdict(tree)


state = tree()
env.use_hostbased = True
env.hosts = ['apollyon',]
obj = commands.GPFSCommands(state)
with settings(hide('running'), output_prefix=''):
    obj.build_cluster_state()
    obj.get_gpfs_managers()
    obj.get_node_kernel_and_arch()
    obj.get_node_gpfs_baserpm()
    obj.get_node_gpfs_verstring()

print state

