import re
import StringIO
import sys
from collections import defaultdict
from fabric.tasks import execute
from fabric.api import env, local, run, parallel
from fabric.context_managers import hide, show, settings
from gpfs.cluster import change_fs_manager, change_cluster_manager

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

class Node(object):
    '''This class is used to interact and modify a state 
    dictionary based on node attributes'''

    def __init__(self, state, envs=None):
        """initializes class, but accepts fabric env vars if necessary
       
        @param state: global dictionary. should be passed after 
            gpfs.cluster.build_cluster_state creates initial dictionary
        @type state: defaultdict

        @param envs: a list of fabric environment vars if necessary
        @type envs: list
        """

        self.state = state

        # pass in any relevent fabric.api.env vars
        if envs:
            for k in envs:
                env[k] = envs[k]

    def update_action_status():

        return

    def update_gpfs_state(self):
        """Updates the GPFS state in the state dictionary"""

        f = StringIO.StringIO()
        node = env.host

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

        self.state['nodes'][node]['gpfs_state'] = gpfs_state
        return 

    def update_gpfs_software(self, gpfs_version, reboot_node=False, 
            dry_run=True):
        """Update the gpfs rpms, install the portability layer,
        rpm and start gpfs back up.

        You need to have a fully populated gpfs cluster state dictionary
        for this to work properly

        @param gpfs_version: version to update GPFS to, EX: '3.5.0-11'
        @type gpfs_version: string

        @param reboot_node: reboot the node after updating
        @type reboot_node: boolean

        @param dry_run: True runs the update, False prints the actions
        @type dry_run: boolean

        @return True or False
        @rtype: boolean
        """

        nodename = env.host_string

        # check the gpfs_version arg
        if not re.match('^\d+\.\d+\.\d+\-\d+', gpfs_version):
            raise SystemError('gpfs_version arg must be in format: X.Y.Z-V')

        # is this node a cluster or filesystem manager?
        for item in self.state['managers'].items():

            # format: "managers": {
            #           "cluster": "abaddon",
            #           "devfs0": "apollyon"
            #           }
            manager = item[1]
            resource = item[0]  # could be 'cluster' or a fs name

            if nodename == manager:
                print "\'%s\' manages: %s" % ( nodename, resource)

                # find a node that is a quorum-manager, and move the role to it
                #   - however, first check if there is any nodes in the finished
                #   queue that can handle the task first...
                #
                # create list of quorum-managers that are in the finished_queue
                myset = set(self.state['finished_queue'])
                qmgrs = list(myset.intersection(state['quorum_managers']))

                # select a quorum-manager node from the qmgrs list,
                #   which contains quorum-manager nodes from the finished_queue
                #   list
                if len(qmgrs) > 0:

                    new_man = qmgrs[0]  # get first quorum-manager from the list

                    if resource == 'cluster':
                        # change the cluster manager to the new manager
                        print "Changing cluster manager to {0}, found via\
                            finished_queue.".format(new_man)
                        if dry_run is False:
                            #change_cluster_manager(new_man)
                            pass

                        self.state['managers']['cluster'] = new_man
                    else:
                        print "Changing {0} to {1} found via \
                            finished_queue.".format(new_man, resource)
                        if dry_run is False:
                            #change_fs_manager(new_man, resource)
                            pass

                        self.state['managers'][resource] = new_man

                else:
                    # select another manager node
                    found_new_manager = False

                    for mgr in self.state['quorum_managers']:
                        if mgr != nodename:

                            new_man = mgr
                            found_new_manager = True
                            break
                            
                    # couldn't find new manager, we're screwed, bail...
                    if not found_new_manager:
                        print "Error, could not find suitable manager node."
                        print "Exiting..."
                        sys.exit(1)

                    if resource == 'cluster':
                        # change the cluster manager to the new manager
                        print "Changing cluster manager to {0}".format(new_man)
                        if dry_run is False:
                            #change_cluster_manager(new_man)
                            pass

                        self.state['managers']['cluster'] = new_man

                    else:
                        print "Changing {0} to {1} found via \
                            finished_queue.".format(new_man, resource)
                        if dry_run is False:
                            #change_fs_manager(new_man, resource)
                            pass

                        self.state['managers'][resource] = new_man

        # now, do the update action if we are clear...
        self.state['nodes'][nodename]['action_status'] = 'running_action'
        print "run(do_something) to node: {0}".format(nodename)

        if reboot_node is True:
            self.state['nodes'][nodename]['action_status'] = 'rebooting'
            print "Rebooting node: {0}".format(nodename)

        # check the state of GPFS on the node
        print "Checking state of {0} after software update".format(nodename)
        print "Checking GPFS packages match {0} version...".format(gpfs_version)
        software_levels = get_gpfs_software_levels(nodename)

        for package in software_levels.items():
            if re.match('^gpfs.gplbin', package[0]):
                kern = self.state['nodes'][nodename]['kernel_vers']
                good_gplbin = "gpfs.gplbin-" % (kern)

                if package[0] == good_gplbin and package[1] == gpfs_version:
                    have_good_gplbin = True
                else:
                    pass


            if gpfs_version != package[1]:
                print " - FAILED: {0} version is: {1}".format(
                        package[0], package[1])
    
        # dont have a proper gplbin package...
        if not have_good_gplbin:
            print " - FAILED: {0} version is: {1}".format(
                package[0], package[1])

        print "Checking GPFS daemon state..."
        new_state = get_gpfs_state(nodename)
        print "Node GPFS state {0} is: {0}".format(new_state)
    
        # try to start gpfs
        if dry_run is False:
            if new_state != 'active':

                print ""
                startup_gpfs(nodename)
                time.sleep(15)

                new_state = get_gpfs_state(nodename)

                # something is wrong here...
                if new_state != 'active':
                    print "Problem starting GPFS on %s" % (nodename)
                    self.state['nodes'][nodename]['gpfs_state'] = new_state
                    self.state['nodes'][nodename]['action_status'] = 'broken'

                    return False

        # set that node's state to finished and update the gpfs_state
        self.state['nodes'][nodename]['gpfs_state'] = new_state
        self.state['nodes'][nodename]['action_status'] = 'finished'
        self.state['finished_queue'].append(nodename)

        return True


# objectless subroutines 
def get_kernel_and_arch(node):
    """Gather the kernel version and architecture of a node

    @param node: node to get info from
    @type node: string

    @return: a tuple containing the kernel and arch of a node
    @rtype: tuple
    """

    f = StringIO.StringIO()
    env.hide = ('running')
    env.host_string = str(node)
    env.output_prefix = ''
        
    run('uname -a', stdout=f)

    kernel_vers = f.getvalue().split()[2]
    arch = kernel_vers.split('.')[-1]

    return kernel_vers, arch

def get_gpfs_baserpm(node):
    """
    Get the gpfs.base rpm version

    @param node: node to get info from
    @type node: string

    @return baserpm: a string in the format: '3.5.0-11' 
    @rtype: string
    """

    f = StringIO.StringIO()
    env.hide = ('running')
    env.host_string = str(node)
    env.output_prefix = ''
        
    run('rpm -q gpfs.base --queryformat "%{name} %{version} %{release}\n"'
        , stdout=f)
    
    baserpm = '-'.join(f.getvalue().split()[1:])
    return baserpm

def get_gpfs_software_levels(node):
    """
    Get the GPFS software levels for the following packages:
    gpfs.base
    gpfs.docs
    gpfs.gpl
    gpfs.gplbin
    gpfs.msg.en_US

    @param node: node to check software levels on
    @type node: string

    @return software: dict of software levels
    @rtype: dict
    """

    software_levels = {}

    f = StringIO.StringIO()
    env.hide = ('running')
    env.host_string = str(node)
    env.output_prefix = ''

    # first, get the normal packages
    run('rpm -q gpfs.base gpfs.gpl gpfs.docs gpfs.msg.en_US \
            --queryformat "%{name} %{version}-%{release}\n"', stdout=f)


    for line in f.getvalue().splitlines():
        key = line.split()[0]
        value = line.split()[1]
        software_levels[key] = value

    # the gpfs.gplbin package is harder
    # 
    # need to get the kernel version, and then do some crappy grepping
    #
    # the problem is that there could be more than one gpfs.gplbin rpm
    #   installed, so do something with that as well...
    f.truncate(0)

    run('rpm -qa | grep gpfs.gplbin | grep `uname -r` | xargs -I\'{}\' \
            rpm -q \'{}\' --queryformat "%{name} %{version}-%{release}\n"', 
            stdout=f)

    for line in f.getvalue().splitlines():
        key = line.split()[0]
        value = line.split()[1]
        software_levels[key] = value

    return software_levels

def get_gpfs_state(node):
    """
    Get the GPFS state of the node at any given time, and update
    the global_state dictionary. also returns the state to a calling process

    @param node: node to get the gpfs state from
    @type node: string

    @return: gpfs_state: the state
    @rtype: string
    """

    f = StringIO.StringIO()
    env.hide = ('running')
    env.host_string = str(node)
    env.output_prefix = ''

    run('mmgetstate', stdout=f)

    for line in f.getvalue().splitlines():

        if any(regex.match(line) for regex in _LINE_REGEX): 
            continue

        else:
            gpfs_state = line.split()[2]

    return gpfs_state

def get_gpfs_verstring(node):
    """
    Gets daemon version (daemon needs to be running)

    @param node: node to get verstring from
    @type node: string

    @return: version string
    @rytpe: string
    """

    f = StringIO.StringIO()
    env.hide = ('running')
    env.host_string = str(node)
    env.output_prefix = ''

    run('mmfsadm dump version | grep "Build branch"', stdout=f)

    gpfsverstr = f.getvalue().split()[1]

    return gpfsverstr

def mount_filesystem(filesystem, node):
    """Mount GPFS filesystems on a given node
    
    @param filesystem: filesystem to mount, 'all' mounts all eligible
        GPFS filesystems on that node
    @type filesystem: string
    @param node: node to run command on
    @type node: string

    @return NOTHING
    """

    env.host_string = node
    run("mmmount %s" % filesystem)
    
    return

def start_gpfs(node):
    """Starts GPFS on a given node
    @param node: node to run command on
    @type node: string
       
    @return NOTHING   
    """

    env.host_string = node
    run('mmstartup')
    return

def shutdown_gpfs(node):
    """Shuts down GPFS on a given node
    @param node: node to run command on
    @type node: string
       
    @return NOTHING   
    """

    env.host_string = node
    run('mmshutdown')

    return

def unmount_filesystem(filesystem, node):
    """Mount GPFS filesystems on a given node
    
    @param filesystem: filesystem to mount, 'all' mounts all eligible
        GPFS filesystems on that node
    @type filesystem: string
    @param node: node to run command on
    @type node: string

    @return NOTHING
    """

    env.host_string = node
    run("mmumount %s" % filesystem)

    return

