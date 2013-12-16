#!/usr/bin/env python
import json
import numpy as np
from collections import defaultdict
from optparse import OptionParser

def yodict():
	return defaultdict(yodict)

def main(options, args):
	diskmap = yodict()

	with open(options.filename, 'r') as f:
		#   28.104046   7998 TRACE_IO: FIO: write data tag 225395665 23682052 ioVecSize 128 1st buf 0x41509B0000 nsdId AC170567:50655A01 da 263:8366374912 nSectors 16384 err 0
		for line in f:
			fields = line.split()
			io_t = fields[4]
			inode = fields[7]
			iblock = fields[8]
			disk = fields[17].split(':')[0]
			lba = fields[17].split(':')[1]
			sectors = fields[19]
			iosz = sectors * 512
			io_s = lba
			io_e = lba + iosz

			if diskmap[disk][lba]:
				diskmap[disk][lba] += 1
			else:
				diskmap[disk][lba] = 1

	#print sorted(diskmap, key=lambda x: diskmap[x][lba])
	print json.dumps(diskmap, sort_keys=True, indent=4)

if __name__ == '__main__':
	parser = OptionParser()
	parser.add_option('-f', '--filename',
						dest='filename',
    					help='tracefile to open (formatted for FIO only)')
 	options, args = parser.parse_args()
	if not options.filename:   # if filename is not given
		parser.error('Filename not given')
 	main(options, args)
