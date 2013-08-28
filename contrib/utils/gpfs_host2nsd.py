#!/usr/bin/env python
import argparse
import os
import subprocess
import sys

def main(args):

    host = args.host

    # check if mmlsnsd command is available
    test = os.access('/usr/lpp/mmfs/bin/mmlsnsd', os.X_OK)
    if not test:
        print "mmlsnsd command not available"
        sys.exit(1)
    else:
        nsds = []

        out = subprocess.Popen('/usr/lpp/mmfs/bin/mmlsnsd', 
                shell=True, stdout=subprocess.PIPE)

        for line in out.stdout.readlines():
            if host in line:
                nsd = line.split()[1]
                primary = line.split()[-1].split(',')[0]
                if primary == host:
                    nsds.append(nsd)


        print nsds


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='gets the nsds the supplied \
                        host is the primary server for.')
    parser.add_argument('-H', '--host',
                        dest='host',
                        required=True,
                        help='host to get the nsds its a primary for.')

    args = parser.parse_args()
    main(args)
