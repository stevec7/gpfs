#!/usr/bin/env python
import argparse
import os
import subprocess
import sys

def main(args):

    nsd = args.nsd

    # check if mmlsnsd command is available
    test = os.access('/usr/lpp/mmfs/bin/mmlsnsd', os.X_OK)
    if not test:
        print "mmlsnsd command not available"
        sys.exit(1)
    else:
        cmdargs = "-d {0}".format(nsd)
        out = subprocess.Popen('/usr/lpp/mmfs/bin/mmlsnsd', cmdargs, 
                shell=True, stdout=subprocess.PIPE)

        for line in out.stdout.readlines():
            if nsd in line:
                servers = line.split()[-1].split(',')

                if len(servers) == 1:
                    print "Primary: {0}".format(servers[0])
                elif len(servers) > 1:
                    print "Primary: {0}, Secondary: {1}".format(
                            servers[0], servers[1])
                else:
                    print "No servers found."





if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='gets the primary and secondary \
                                servers for a given nsd.')
    parser.add_argument('-d', '--disk',
                        dest='nsd',
                        required=True,
                        help='disk to get prim/sec nsd servers for.')

    args = parser.parse_args()
    main(args)
