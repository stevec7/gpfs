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


class Node(object):

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

    def get_kernel_and_arch(self):
        """Gather the kernel version and architecture of a node

        @return: a tuple containing the kernel and arch of a node
        @rtype: tuple
        """

        node = env.host
        f = StringIO.StringIO()

        with settings(
                hide('running'),
                output_prefix=''
            ):
            run('uname -a', stdout=f)

        kernel_vers = f.getvalue().split()[2]
        arch = kernel_vers.split('.')[-1]
        self.state['nodes'][node]['kernel_vers'] = kernel_vers
        self.state['nodes'][node]['arch'] = arch

        return 

    def get_gpfs_state(self):
        """
        Get the GPFS state of the node at any given time, and update
        the global_state dictionary. also returns the state to a calling process

        @return: gpfs_state: the state
        @rtype:
        """

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

        return gpfs_state

    def get_gpfs_baserpm(self):
        """
        Get the gpfs.base rpm version

        @return NOTHING
        """

        f = StringIO.StringIO()
        node = env.host

        with settings(
                hide('running'),
                output_prefix='',
            ):
            run('rpm -q gpfs.base --queryformat "%{name} %{version} %{release}\n"'
                , stdout=f)
        
        self.state['nodes'][node]['gpfs_baserpm'] = '-'.join(f.getvalue().split()[1:])
        return 

    def get_gpfs_verstring(self):
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

        self.state['nodes'][node]['gpfs_build_branch'] = f.getvalue().split()[1]
        return 

    def mount_filesystem(self, filesystem):
        """Mount GPFS filesystems on a given node
        
        @param filesystem: filesystem to mount, 'all' mounts all eligible
            GPFS filesystems on that node
        @type filesystem: string

        @return NOTHING
        """

        run("mmmount %s" % filesystem)
        return

    def unmount_filesystem(self, filesystem):
        """Mount GPFS filesystems on a given node
        
        @param filesystem: filesystem to mount, 'all' mounts all eligible
            GPFS filesystems on that node
        @type filesystem: string

        @return NOTHING
        """

        run("mmumount %s" % filesystem)
        return

    def start_gpfs(self):
        """Starts GPFS on a given node"""

        run('mmstartup')
        return

    def shutdown_gpfs(self):
        """Shuts down GPFS on a given node"""

        run('mmshutdown')
        return

    def update_gpfs_rpms(self, version, arch, kernel, reboot=False):
        """Update the following GPFS rpms:
            - gpfs.base
            - gpfs.docs
            - gpfs.msg.en_US
            - gpfs.gpl
            - gpfs.gplbin
        
        This will use yum. This requires shutting down the GPFS daemon

        @param version: version of GPFS to update to, in form: X.Y.Z-V
        @type version: string

        @param arch: package architecture (x86_64 vs ppc64)
        @type arch: string

        @param kernel: the kernel version, ex: 2.6.32-358.6.2.el6.x86_64
        @type kernel: string

        @param reboot: reboot the node after installing/update the rpms
        @type reboot: boolean

        @return True or False, e.g. success or fail
        """
       
        f = StringIO.StringIO()
        node = env.host
        versionstring = "%s.%s" % (version, arch)
        packages = [ 'gpfs.base', 'gpfs.gpl', 'gpfs.docs', 'gpfs.msg.en_US' ]
        yumupdate = "yum -y update-to "

        # generate the yum update line
        for p in packages:
            yumupdate = yumupdate + "%s-%s " % (p, versionstring) 

        # generate the correct gplbin package name, ex:
        #   gpfs.gplbin-2.6.32-358.6.2.el6.x86_64-3.5.0-7.x86_64
        #   - blame IBM for that...
        gplbin = "gpfs.gplbin-%s-%s" % (kernel, versionstring)

        #   do the deed...
        #
        # first, shutdown GPFS on the node
        self.shutdown_gpfs()
        state['nodes'][node]['gpfs_state'] = 'down'
        #
        # update the rpms
        state['nodes'][node]['action_status'] = 'updating'
        run(yumupdate)

        # install new gplbin rpm
        cmd = "yum -y install %s" % (gplbin)
        run(cmd)

        # if we need to reboot the server, do so
        if reboot is True:
            state['nodes'][node]['action_status'] = 'rebooting'
            reboot()
            self.start_gpfs()

        else:
            self.start_gpfs()

        # check GPFS state
        time.sleep(30)
        out = self.get_gpfs_state()

        if out == 'active':
            pass
        else:  #try to start GPFS
            self.startup_gpfs()
            time.sleep(30)
            out = self.get_gpfs_state()
        
            if out != 'active':
                print "Critical error starting GPFS on host: %s" % (env.host)
            else:
                pass

        return True

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
