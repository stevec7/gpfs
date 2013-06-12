import sys
from collections import defaultdict
from fabric.tasks import execute
from fabric.api import env, local, run, parallel
from fabric.context_managers import hide, show, settings

class Node(object):

    def __init__(self, envs=None):
        """initializes class, but accepts fabric env vars if necessary
        
        @param envs: a list of fabric environment vars if necessary
        @type envs: list
        """

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

        return kernel_vers, arch


    def get_gpfs_state(self):
        """
        Get the GPFS state of the node at any given time, and update
        the global_state dictionary
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
                output_prefix=''
            ):
            run('rpm -q gpfs.base --queryformat "%{name} %{version} %{release}\n"'
                , stdout=f)

        return '-'.join(f.getvalue().split()[1:])

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

        return f.getvalue().split()[1]

    def mount_filesystem(self, filesystem):
        """Mount GPFS filesystems on a given node
        
        @param filesystem: filesystem to mount, 'all' mounts all eligible
            GPFS filesystems on that node
        @type filesystem: string

        @return NOTHING
        """
        return

    def start_gpfs(self):
        """Starts GPFS on a given node"""
        return

    def shutdown_gpfs(self):
        """Shuts down GPFS on a given node"""
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

        #print "yumupdate: %s" % yumupdate
        #print "gplbin: %s" % gplbin

        return True
