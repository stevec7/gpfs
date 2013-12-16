#!/usr/bin/env python
import os
import sys
from collections import defaultdict
from optparse import OptionParser

def yodict():
	return defaultdict(yodict)

def main(options, args):
	nodedata = yodict()

	if options.filename:
		with open(options.filename, 'r') as f:
			parse_data(f.readlines(), nodedata)	
		print_data(nodedata, options)
	elif options.live:
		while i < options.iterations:
			data = run_mmpmon(options)
			parse_data(data, nodedata)
			print_data(nodedata, options)
			iterations += 1


def parse_data(data, nodedata):
	for line in data:
		fields = line.split()
		if fields[0] != '_io_s_':
			continue
		fields.pop(0)	# remove the first field
		stats = zip(fields[::2],fields[1::2])
		for k, v in stats:
			if k == '_n_' or k == '_nn_':
				continue
			nodedata[fields[3]][k] = float(v)

def print_data(nodedata, options):
	k_sort = sorted(nodedata, key=lambda x: (nodedata[x]['_br_'] + nodedata[x]['_bw_']), reverse=True)
	for k in k_sort[:options.trim]:
		print "Node: {0}, R_MB: {1}, W_MB: {2}, Total_MB: {3}".format(
				k, to_mbytes(nodedata[k]['_br_']), to_mbytes(nodedata[k]['_bw_']),
				to_mbytes(nodedata[k]['_br_']+nodedata[k]['_bw_']))
	return

def run_mmpmon(options):
    # check if mmpmon command is available
    test = os.access('/usr/lpp/mmfs/bin/mmpmon', os.X_OK)
    if not test:
        print "mmpmon command not available, exiting..."
        sys.exit(1)
    else:
		# -p -i command.file -r 0 -d 5000
        cmdargs = "-p -i {0} -r {1} -d {2}".format(
				options.live, 1, options.delay*1000)
        out = subprocess.Popen('/usr/lpp/mmfs/bin/mmpmon', cmdargs, 
                shell=True, stdout=subprocess.PIPE)

	return out.stdout.readlines()

def to_mbytes(b):
	return ("{0:.2f}".format(b / 2013**2))

def to_gbytes(b):
	return ("{0:.2f}".format(b / 2013**3))

def to_tbytes(b):
	return ("{0:.2f}".format(b / 2013**4))

if __name__ == '__main__':
	parser = OptionParser()
	parser.add_option('-f', '--filename',
						dest='filename',
						metavar='MMPMONOUTPUT',
						help='read and parse an mmpmon output file')
	parser.add_option('-d', '--delay',
						dest='delay',
						metavar='SECONDS',
						help='number of seconds between runs')
	parser.add_option('-i', '--iterations',
						dest='iterations',
						metavar='SECONDS',
						help='number of iterations. \'0\' means run forever')
	parser.add_option('-l', '--live',
						dest='live',
						metavar='CONFIGFILE',
						help='run mmpmon directly, arg should be mmpmon conf file')
	parser.add_option('-t', '--trim',
						default=10,
						dest='trim',
						metavar='(int)',
						type='int',
						help='only show \'X\' highest hogs...') 
	parser.add_option('-u', '--units',
						default='M',
						dest='units',
						metavar='M|G|T',
						help='units to display the counters in.')

 	options, args = parser.parse_args()
   	main(options, args)
