import operator
import os
import re
import StringIO
import sys
from collections import defaultdict
from fabric.tasks import execute
from fabric.api import env, local, run, parallel
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




class GPFSCommands(object):

    def __init__(self, state):
        self.state = state

    # hate this method
    def _tree(self):
        return defaultdict(self._tree)

    def build_cluster_state(self):
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
                self.state[key] = value
            else:
                lf = ' '.join(line.split()).split() # line fields
                admin_node_name = lf[3]
                node_short_name = admin_node_name.split('.')[0]

                if len(lf) < 5:
                    roles = 'client'    # client in terms of GPFS roles
                                        #   still could be NSD server
                else:
                    roles = lf[4]

                self.state['nodes'][node_short_name]['node_index'] = lf[0]
                self.state['nodes'][node_short_name]['daemon_node_name'] = lf[1]
                self.state['nodes'][node_short_name]['node_ip'] = lf[2]
                self.state['nodes'][node_short_name]['admin_node_name'] = lf[3]
                self.state['nodes'][node_short_name]['roles'] = roles
                self.state['nodes'][node_short_name]['fg'] = [] # empty list

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
                self.state['nodes'][node_short_name]['gpfs_state'] = gpfs_state

        f.truncate(0)

        # get node failure group membership
        run('mmlsnsd', stdout=f)
        disks = self._tree()
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

                if fg not in self.state['nodes'][short_name]['fg']:
                    self.state['nodes'][short_name]['fg'].append(fg)

        return

    def get_gpfs_managers(self):
        """
        Get the current cluster/filesystem managers at any point in time, and
        update the self.state dictionary
        """

        f = StringIO.StringIO()

        run('mmlsmgr', stdout=f)

        for line in f.getvalue().splitlines():

            if any(regex.match(line) for regex in _LINE_REGEX):
                continue

            elif re.match('Cluster manager node: ', line):
                clusterman = line.split()[-1].strip('(').strip(')')
                self.state['managers']['cluster'] = clusterman

            # this should get the filesystem manager lines
            else:
                fs = line.split()[0]
                fsman = line.split()[-1].strip('(').strip(')')
                self.state['managers'][fs] = fsman

        return

    def get_node_kernel_and_arch(self):
        """Gather the kernel version and architecture of the nodes"""

        node = env.host

        f = StringIO.StringIO()
        run('uname -a', stdout=f)

        kernel_vers = f.getvalue().split()[2]
        arch = kernel_vers.split('.')[-1]

        self.state['nodes'][node]['kernel_vers'] = kernel_vers
        self.state['nodes'][node]['arch'] = arch

        return


    def get_node_gpfs_state(self, node):
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


    def get_node_gpfs_baserpm(self):
        """
        Get the GPFS version on a given node

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

        self.state['nodes'][node]['gpfs_vers'] = '-'.join(f.getvalue().split()[1:])

        return
        

    def get_node_gpfs_verstring(self):
        """
        Gets daemon version (daemon needs to be running)
        """

        f = StringIO.StringIO()
        node = env.host

        with settings(
                hide('running'),
                output_prefix=''
            ):
            run('mmfsadm dump version | grep "Build branch"', stdout=f)

        
        #for line in f.getvalue().splitlines():
                
        return
