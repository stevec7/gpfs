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



def tree():
    return defaultdict(tree)

def main(args):

    state = tree()
    env.hosts = [ args.destnode, ]
    env.use_hostbased = True
    cluster = GPFSCluster(state)

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

    # write all of this to a json dump
    json.dump(state, open(args.jsonfile, 'w'))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='grabs GPFS cluster state \
                                    and dumps to a json file')
    parser.add_argument('-f', '--file', dest='jsonfile', required=True,
                help='json file to dump to')
    parser.add_argument('-n', '--node', dest='destnode', required=True, 
                help='node to grab cluster state from')
    args = parser.parse_args()

    main(args)
