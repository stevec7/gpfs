#!/usr/bin/env python

import argparse
import operator
import os
import pprint
import re
import StringIO
import sys
from collections import defaultdict
from fabric.tasks import execute
from fabric.api import env, local, run, parallel#, reboot
from fabric.context_managers import hide, show, settings


# some global regexes
_SKIP_REGEXES = [ '^----', '^======', ' Node  Daemon',
                 'GPFS cluster information', '^ *$',
                 'GPFS cluster configuration servers:', ' Node number',
                 ' File system   Disk name', 'file system',
                 ' Node number', 'disk         driver', 'name         type',
                 ]
_LINE_REGEX = []    

for r in _SKIP_REGEXES:
    _LINE_REGEX.append(re.compile(r)) 


def tree():
    """creates a nested dictionary. harms' favorite function"""
    return defaultdict(tree)


def build_cluster_state(global_state):
    """
    Builds a large dictionary containing filesystem information, node info,
    node states, failure group membership, etc. 
    Will be used for the rest of this script
    """
    colon = re.compile(':')
    f = StringIO.StringIO()

    # get node information, roles, etc
    run('mmlscluster', stdout=f)

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX):
            continue
        elif colon.search(line):
            key = line.split(':')[0].strip(' ').replace(' ', '_').lower()
            value = line.split(':')[1].strip(' ').strip('\n')
            global_state[key] = value
        else:
            lf = ' '.join(line.split()).split() # line fields
            admin_node_name = lf[3]
            node_short_name = admin_node_name.split('.')[0]

            if len(lf) < 5:
                roles = 'client'    # client in terms of GPFS roles
                                    #   still could be NSD server
            else:
                roles = lf[4]

            global_state['nodes'][node_short_name]['node_index'] = lf[0]
            global_state['nodes'][node_short_name]['daemon_node_name'] = lf[1]
            global_state['nodes'][node_short_name]['node_ip'] = lf[2]
            global_state['nodes'][node_short_name]['admin_node_name'] = lf[3]
            global_state['nodes'][node_short_name]['roles'] = roles
            global_state['nodes'][node_short_name]['fg'] = [] # empty list

    f.truncate(0)

    # get initial node states
    run('mmgetstate -L -a', stdout=f)

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX):
            continue
        
        else:
            lf = ' '.join(line.split()).split()
            node_short_name = lf[1]
            gpfs_state = lf[5]
            global_state['nodes'][node_short_name]['gpfs_state'] = gpfs_state

    f.truncate(0)

    # get node failure group membership
    run('mmlsnsd', stdout=f)
    disks = tree()
    filesystems = []

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX):
            continue
        else:
            filesystem = line.split()[0]
            disk_name = line.split()[1]
            nsd_servers = line.split()[2]
            disks[disk_name]['name'] = disk_name
            disks[disk_name]['nsdservers'] = nsd_servers

            if filesystem not in filesystems:
                filesystems.append(filesystem)
   
    f.truncate(0) 

    for fs in filesystems:
        cmd = "mmlsdisk %s" % fs
        run(cmd, stdout=f)

        for line in f.getvalue().splitlines():

            if any(regex.match(line) for regex in _LINE_REGEX):
                continue
        
            else:
                disk_name = line.split()[0]
                fg = line.split()[3]
                disks[disk_name]['fg'] = fg


    for d in disks.keys():
        for n in disks[d]['nsdservers'].split(','):
            fg = disks[d]['fg']
            short_name = n.split('.')[0]

            if fg not in global_state['nodes'][short_name]['fg']:
                global_state['nodes'][short_name]['fg'].append(fg)
            
    return


def get_current_managers(global_state):
    """
    Get the current cluster/filesystem managers at any point in time, and 
    update the global_state dictionary
    """

    f = StringIO.StringIO()

    run('mmlsmgr', stdout=f)

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX):
            continue
    
        elif re.match('Cluster manager node: ', line):
            clusterman = line.split()[-1].strip('(').strip(')')
            global_state['managers']['cluster'] = clusterman
        
        # this should get the filesystem manager lines
        else:
            fs = line.split()[0]
            fsman = line.split()[-1].strip('(').strip(')')
            global_state['managers'][fs] = fsman

    return

def get_node_gpfs_state(global_state, node):
    """
    Get the GPFS state of the node at any given time, and update
    the global_state dictionary
    """

    f = StringIO.StringIO()

    with settings(
            hide('running'),
            output_prefix=''
        ):
        run('mmgetstate', stdout=f)

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX):
            continue

        else:
            gpfs_state = line.split()[2]

    global_state['nodes'][node]['gpfs_state'] = gpfs_state

    return

def get_node_gpfs_vers(global_state):
    """
    Get the GPFS version on a given node

    @param global_state: global state dictionary
    @type global_state: dict

    @return NOTHING
    """

    f = StringIO.StringIO()
    node = env.host

    with settings(
            hide('running'),
            output_prefix=''
        ):
        run('rpm -q gpfs.base --queryformat "%{name} %{version} %{release}\n"'
            , stdout=f)

    global_state['nodes'][node]['gpfs_vers'] = '-'.join(f.getvalue().split()[1:])

    return 

def kernel_and_arch(global_state):
    """Gather the kernel version and architecture of the nodes"""

    f = StringIO.StringIO()
    run('uname -a', stdout=f)

    node = f.getvalue().split()[1]
    kernel_vers = f.getvalue().split()[2]
    arch = kernel_vers.split('.')[-1]

    global_state['nodes'][node]['kernel_vers'] = kernel_vers
    global_state['nodes'][node]['arch'] = arch

    return

def change_cluster_man(global_state, node):
    """
    Sets the new GPFS cluster manager. This function is meant to be run
    non-wrapped (such as with(blah, blah)) from the calling class, to set 
    proper command timeouts.
    
    @param global_state: dictionary that holds all information
    @type global_state: dictionary

    @param node: new proposed cluster manager. MUST BE A GPFS QUORUM-MANAGER NODE
    @type node: string

    @return: I REGRET NOTHING
    """

    #change_command = "mmchmgr -c %s" % node
    #with settings(
    #       command_timeout=120,
    #       warn_only=False,    # if this command fails, we want to bail 
    #   ):
    #   run(command)    
 
    return

def change_fs_man(global_state, fs, node):
    """
    Sets the new GPFS filesystem manager. This function is meant to be run
    non-wrapped (such as with(blah, blah)) from the calling class, to set 
    proper command timeouts.
    
    @param global_state: dictionary that holds all information
    @type global_state: dictionary

    @param fs: filesystem
    @type fs: string

    @param node: new proposed fs manager. MUST BE A GPFS QUORUM-MANAGER NODE
    @type node: string

    @return: I REGRET NOTHING
    """

    #change_command = "mmchmgr %s %s" % (fs, node)
    #with settings(
    #       command_timeout=120,
    #       warn_only=False,    # if this command fails, we want to bail 
    #   ):
    #   run(command)    
    return

def update_gpfs(global_state, node, gplbin_rpm):
    """
    Updates GPFS on a per host basis

    @param global_state: same global_state dict from all other functions
    @type global_state: dict

    @param node: the node being updated
    @type node: string

    @param gplbin_rpm: the full name of the gpfs.gplbin rpm to install.
        you must provide this on your own, and make sure it's correct
    @type gplbin_rpm: string

    @return NOTHING
    """
    print "You call this now, you die."
    sys.exit(-1)
    #update_pkgs = "yum -qy update gpfs.base gpfs.docs gpfs.msg.en_US gpfs.gpl"
    #install_gplbin = "yum -y install %s" % gplbin_rpm

    # now run everything, and try not to fail
    #global_state['nodes'][node]['action_status'] = 'updating'
    #run('mmshutdown')
    #run(update_pkgs)
    #run(install_gplbin)
    #global_state['nodes'][node]['action_status'] = 'rebooting'
    #reboot(wait=120)
    #

    return

def main(**args):

    global_state = tree()

    hostname = args['host']
    env.use_hostbased = True
    env.hosts = [hostname,]

    if args['gpfsvers'] and not re.match('\d\.\d\.\d\-\d+', args['gpfsvers']):
        print "Wrong --gpfsvers format. Ex: --gpfsvers 3.5.0-7"
        sys.exit(-1)
        
    # undocumented fabric setting:
    #   https://github.com/fabric/fabric/issues/281
    #   thank the lord
    #env.output_prefix = ''

    # this will create a dictionary of cluster states
    with settings(
            hide('running'),
            output_prefix='',
            warn_only=False
        ):
        execute(build_cluster_state, global_state)
        execute(get_current_managers, global_state)

    _QUORUM_MANAGER_SCORE = 10
    _MANAGER_SCORE = 5
    _NSD_NODE_SCORE = 1
    _SIMULT_QUORUM_MANAGERS = 1
    #_GPFS_UPDATE_VERSION = '3.5.0-11'

    # get the kernel ver / arch for each host, and add them to the global state
    #   this will aid in finding the correct GPFS package,
    #   as well as installing the correct gpfs.gplbin rpm
    env.hosts = global_state['nodes'].keys()
    with settings(
            hide('running'),
            parallel=False, # parallel=True doesnt update the global_state, 
                            #   plus, it's fucking slower
                            #   - need to get this working though, since
                            #       it takes 7.5 minutes on the mira-fs0 cluster
            output_prefix='',
            warn_only=True
        ):
        execute(kernel_and_arch, global_state)
        execute(get_node_gpfs_vers, global_state)

    # now, create a 'plan' to do things:
    #
    #   - we should organize the nodes into a queue/list to update
    #   and then assign a weight to each node based on it's roles
    #   to update them intelligently
    #
    # assign a node weight and an action status, where action_status = 
    #   'queued', 'updating' 'rebooting', 'finished_update', ''
    for node in global_state['nodes'].keys():
        global_state['nodes'][node]['action_status'] = ''

        if global_state['nodes'][node]['roles'] == 'quorum-manager':
            global_state['nodes'][node]['weight'] = _QUORUM_MANAGER_SCORE
        elif global_state['nodes'][node]['nodes'] == 'manager':
            global_state['nodes'][node]['weight'] = _MANAGER_SCORE
        else:
            global_state['nodes'][node]['weight'] = _NSD_NODE_SCORE

    # create a queue of nodes based on their weight
    #   sort the nodelist by weight (asc)
    node_queue = sorted( [(i, global_state['nodes'][i]['weight']) \
                    for i in global_state['nodes'].keys()], \
                    key=operator.itemgetter(1) )
            
     
    
    


def command_line_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', '-H',
                        dest='host',
                        required=True,
                        help='host to get initial cluster state from')
    parser.add_argument('--gpfsvers',
                        dest='gpfsvers',
                        required=False,
                        help='GPFS version to update to. Format Ex: 3.5.0-11')
    args = vars(parser.parse_args())

    main(**args)

if __name__ == '__main__':

    command_line_args()
