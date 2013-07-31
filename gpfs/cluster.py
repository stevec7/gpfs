import operator
import os
import re
import StringIO
import sys
from collections import defaultdict
from fabric.tasks import execute
from fabric.api import env, local, run, parallel
from fabric.context_managers import hide, show, settings


# some global regexes that we want to skip when reading line output
_SKIP_REGEXES = [ '^----', '^======', ' Node  Daemon',
                 'GPFS cluster information', '^ *$',
                 'GPFS cluster configuration servers:', ' Node number',
                 ' File system   Disk name', 'file system',
                 ' Node number', 'disk         driver', 'name         type',
                 ' Summary information', 'Number of ']
_LINE_REGEX = []

for r in _SKIP_REGEXES:
    _LINE_REGEX.append(re.compile(r))




class GPFSCluster(object):

    def __init__(self, state, envs=None):
        self.state = state

        # pass in any relevent fabric.api.env vars
        if envs:
            for k in envs:
                env[k] = envs[k]

    # hate this method
    def _tree(self):
        return defaultdict(self._tree)

    def _assign_node_weights(self):
        """
        Assigns node weights based roles
        """
        _CONFIG_SERVER_SCORE = 11
        _QUORUM_MANAGER_SCORE = 8
        _QUORUM_SCORE = 5
        _MANAGER_SCORE = 3
        _CLIENT_SCORE = 1

        for node in self.state['nodes'].keys():

            fullname = self.state['nodes'][node]['admin_node_name']

            if self.state['nodes'][node]['roles'] == 'quorum-manager':
                self.state['nodes'][node]['weight'] = _QUORUM_MANAGER_SCORE
            elif self.state['nodes'][node]['roles'] == 'quorum':
                self.state['nodes'][node]['weight'] = _QUORUM_SCORE
            elif self.state['nodes'][node]['roles'] == 'manager':
                self.state['nodes'][node]['weight'] = _MANAGER_SCORE
            else:
                self.state['nodes'][node]['weight'] = _CLIENT_SCORE


            # check to see if node is primary/secondary config server
            #   - don't want them both in the same group
            if self.state['primary_server'] == fullname or \
                self.state['secondary_server'] == fullname:
                    self.state['nodes'][node]['weight'] = _CONFIG_SERVER_SCORE
        

        return

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
                self.state['nodes'][node_short_name]['short_name'] = node_short_name

                # add a blank state for use later
                self.state['nodes'][node_short_name]['action_status'] = ''

                # assign node weights as well
                self._assign_node_weights()

        # create a list of ALL nodes that are 'quorum-manager'
        for node in self.state['nodes'].itervalues():
            if re.match('^quorum', node['roles']):
                    self.state.setdefault('quorum_nodes', []).append(node['short_name'])

            

        f.truncate(0)

        # get initial node states
        run('mmgetstate -L -a -s', stdout=f)

        for line in f.getvalue().splitlines():

            if any(regex.match(line) for regex in _LINE_REGEX):
                continue

            elif re.match('^Quorum =', line):
                min_quorum = int(line.split(',')[0].split('=')[1].strip())
                self.state['min_quorum_nodes'] = min_quorum

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
                if isinstance(disks[d]['fg'], defaultdict):
                    pass

                else:
                    fg = int(disks[d]['fg'])
                    short_name = n.split('.')[0]

                if fg not in self.state['nodes'][short_name]['fg']:
                    self.state['nodes'][short_name]['fg'].append(fg)

        # if you are going to be running some sort of action, 
        #   create a finished_queue list for future use
        self.state['finished_queue'] = []
        return


    def get_all_gpfs_baserpm(self):
        """
        Get the gpfs.base rpm version and updates the state dictionary

        @return NOTHING
        """

        f = StringIO.StringIO()

        with settings(
                hide('running'),
                output_prefix='',
                warn_only=True
            ):
            run('mmdsh -v -N all \'rpm -q gpfs.base --queryformat "%{name} %{version} %{release}\n"\''
                , stdout=f)

        for line in f.getvalue().splitlines():
            node = line.split(':')[0].split('.')[0]
            baserpm = '-'.join(line.split()[2:])
            self.state['nodes'][node]['gpfs_baserpm'] = baserpm 

        return

    def get_all_gpfs_verstring(self):
        """
        Gets daemon version (daemon needs to be running)

        This doesn't seem to work well at times...
        """

        f = StringIO.StringIO()

        with settings(
                hide('running'),
                output_prefix='',
                warn_only=True
            ):
            run('mmdsh -v -N all \'mmfsadm dump version | grep "Build branch"\'', stdout=f)
            #run('mmdsh -v -N all \'mmfsadm dump version | grep "Build branch"\'', stdout=f)

        for line in f.getvalue().splitlines():
            node = line.split(':')[0].split('.')[0]
            build_branch = line.split("\"")[1]
            self.state['nodes'][node]['gpfs_build_branch'] = build_branch

        return

    def get_all_kernel_and_arch(self):
        """Gather the kernel version and architecture of ALL nodes and
        then update the state dictionary with ALL node values

        @return: NOTHING
        """

        f = StringIO.StringIO()

        with settings(
                hide('running'),
                output_prefix='',
                warn_only=True
            ):
            run('mmdsh -v -N all uname -a', stdout=f)

        for line in f.getvalue().splitlines():
            node = line.split(':')[0].split('.')[0]
            kernel_vers = line.split()[3]
            arch = line.split()[3].split('.')[-1]
            self.state['nodes'][node]['kernel_vers'] = kernel_vers
            self.state['nodes'][node]['arch'] = arch

        return

    def get_managers(self):
        """
        Get the current cluster/filesystem managers at any point in time, and
        updates the self.state dictionary
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

# objectless cluster commands
def change_cluster_manager(node):
    """
    Change cluster manager.

    @param node: new cluster manager node
    @type node: string
    """

    env.host_string = node
    run("mmchmgr -c %s" % (node))

    return

def change_fs_manager(node, filesystem):
    """
    Change the filesystem manager. 

    @param node: new filesystem manager node
    @type node: string

    @param filesystem: the filesystem to change the manager for
    @type filesystem: string

    @return True/False
    """

    env.host_string = node
    run("mmchmgr %s %s" % (filesystem, node))

    return

def get_all_gpfs_state():
    """
    Get the GPFS state on all nodes

    @return all_state: dictionary in format: { 'node_name' : 'state' }
    """

    f = StringIO.StringIO()
    all_state = {}

    with settings(
            hide('running'),
            output_prefix='',
            warn_only=True
        ):
        run("mmgetstate -a", stdout=f)

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX):
            continue

        else:
            lf = ' '.join(line.split()).split()
            node_short_name = lf[1]
            gpfs_state = lf[2]

            all_state[node_short_name] = gpfs_state

    return all_state

def mount_fs_on_all(filesystem):
    """
    Mount a GPFS filesystem on all nodes in the cluster

    @param filesystem: filesystem to mount on all nodes
    @type filesystem: string

    @return True or False
    """

    result = run("mmmount {0} -a".format(filesystem))

    if result.return_code == 0:
        return True, "Success"
    else:
        return False, result

def mount_fs_on_all_active(filesystem, node_states):
    """
    Mount a GPFS filesystem on all active nodes

    @param node_states: dict of nodes with 'active' gpfs state
    @type node_states: dict

    @return NOTHING
    """

    active_nodes = []

    # add all active nodes to a list
    for k,v in node_states.iteritems():
        if v == 'active':
            active_nodes.append(k)
        else:
            pass

    # create a comma separated list of nodes in active state
    active_nodes_string = ','.join(active_nodes)

    # run the command
    with hide('everything'):
        run("mmmount {0} -N {1}".format(filesystem, active_nodes_string))

    return

def umount_fs_on_all(filesystem):
    """
    Unmount a GPFS filesystem on all nodes in the cluster

    @param filesystem: filesystem to unmount on all nodes
    @type filesystem: string

    @return True or False
    """

    env.host_string = node
    result = run("mmumount {0} -a".format(filesystem))

    if result.return_code == 0:
        return True, "Success"
    else:
        return False, result

def shutdown_all_gpfs():
    """Shuts down GPFS on ALL nodes"""

    #run('mmshutdown -a')

    return

def startup_all_gpfs():
    """Starts up GPFS on ALL nodes"""

    #run('mmstartup -a')

    return
