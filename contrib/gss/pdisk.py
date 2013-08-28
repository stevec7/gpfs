#!/usr/bin/env python
import json
from collections import defaultdict

def tree():
    return defaultdict(tree)

def main(args):
    filename = args.filename

    try:
        f = open(filename, 'r')
    except IOError as ioe:
        print "Error opening '{0}': {1}".format(filename, ioe)

    pdisks = tree()
    header = f.readline().rstrip('\n')[0:-1]    # dont want newline or blank chars
    fields = header.split(':')

    for line in f:
        contents = line.split(':')
        index = contents[15]
        pdisks[index] = dict(zip(fields, contents))

      
    json.dump(pdisks, open('/tmp/pdisk.json', 'w'))

if __name__ == '__main__':

   parser = argparse.ArgumentParser('look for bad pdisks')
   parser.add_argument('-f', '--filename',
                        dest='filename',
                        required=True,
                        help='the filename containing all of the pdisk data, \
                        gathered via mmlspdisk all -Y.')
