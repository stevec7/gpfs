import datetime
import gzip
import sys
from collections import defaultdict 
from gpfstrace.funcs import zscore, stddev, count_iterations
from IPython import embed


class TraceParser(object):

    def __init__(self, tracelog, verbose):
        self.tracelog = tracelog
        self._SECTOR_SIZE = 512
        self.verbose = verbose

        # some regexes to filter out lines we don't want
        self.IORegex = ['QIO:', 'SIO:', 'FIO:']
        self.TSRegex = ['tscHandleMsgDirectly:', 'tscSendReply:', 'sendMessage',
                        'tscSend:', 'tscHandleMsg:']
        self._FILTER_MAP = {
                        'io':   'TRACE_IO',
                        'rdma': 'TRACE_RDMA',
                        'ts':   'TRACE_TS',
                        'brl':  'TRACE_BRL' 
        }

    def _assemble_io_stats(self):
        """Takes raw tracelog dict and computes disk stats"""

        for v in self.tracelog['trace_io'].values():

            # some of the traces triplets don't contain 'fio'
            #   if so, don't bother looking at this...
            if not 'fio' in v:
                continue
                        
            # the disk data was read/written to
            try:
                disk = int(v['fio']['disknum'])
                v['iosize'] = v['fio']['nSectors'] * self._SECTOR_SIZE
            except TypeError as te:
                print "Type error: {0}".format(te)
                print v

            # some of the client logs don't have QIO/SIO for certain things
            #   like log writes, so check for those...
            if 'sio' in v and \
                    'fio' in v:
                v['iotime'] = v['fio']['tracetime'] - \
                                v['sio']['tracetime']
                self.tracelog['trace_io']['disks'][disk]['iotimes'].setdefault(disk, []).append(
                        float(v['iotime']))

            # figure out how long the IO was queued...
            if 'qio' in v and \
                    'sio' in v:
                io_time_in_queue = v['sio']['tracetime'] - \
                                   v['qio']['tracetime']
                v['time_in_queue'] = io_time_in_queue

                # get the start time in epoch, since we now have the finish time (epoch)
                #   and the duration of the IO
                io_start_time = v['sio']['tracetime'] + self.trace_start_epoch
                io_queued_time = v['qio']['tracetime'] + self.trace_start_epoch
                v['sio']['start_time'] = io_start_time
                v['qio']['queued_time'] = io_queued_time

            # increment the disks bucket per FIO
            try:
                self.tracelog['trace_io']['disks'][disk]['iosizes'].setdefault(
                        disk, []).append(int(v['iosize']))
            except TypeError as te:
                print "Type error: {0}".format(te)
                continue

        total_bytes = 0
        total_iops = 0
        avg_io_tm_data = []
        avg_io_tm_meta = []
        avg_long_io_tm_data = []
        avg_long_io_tm_meta = []
        avg_bucket_data = []
        avg_bucket_meta = []


        # calculate the average time per iop per disk and average io size
        for k, v in self.tracelog['trace_io']['disks'].iteritems():

            try:
                num_iops = len(v['iosizes'][k])
                total_io_bytes = sum(v['iosizes'][k])
                total_io_time = sum(v['iotimes'][k])
                average_io_size = total_io_bytes / len(v['iosizes'][k])
                average_io_time = total_io_time / len(v['iotimes'][k])

                v['stats']['avg_io_tm'] = average_io_time
                v['stats']['avg_io_sz'] = average_io_size
                v['stats']['total_bytes_io'] = total_io_bytes
                v['stats']['total_time_io'] = total_io_time
                v['stats']['longest_io'] = max(v['iotimes'][k])
                v['stats']['num_iops'] = num_iops
            except ZeroDivisionError as zde:
                #print "Zero Division error: {0}".format(zde)
                # these only happen when the "write logData" operation
                #   the problem is that the PID changes on the FIO of a triplet,
                #   so the unique ID "disknum:diskaddr:pid" changes
                continue
            except ValueError as ve:
                print "Value error: {0}".format(ve)
                continue

            if v['stats']['avg_io_sz'] > 1048576: 
                avg_io_tm_data.append(v['stats']['avg_io_tm'])
                avg_long_io_tm_data.append(v['stats']['longest_io'])
                avg_bucket_data.append(v['stats']['avg_io_tm'])
            else:   # metadata disks most likely, fix later...
                avg_io_tm_meta.append(v['stats']['avg_io_tm'])
                avg_long_io_tm_meta.append(v['stats']['longest_io'])
                avg_bucket_meta.append(v['stats']['avg_io_tm'])

            # gather some totals
            total_bytes += v['stats']['total_bytes_io']
            total_iops += v['stats']['num_iops']

        # compute the standard deviations and then set those k, v pairs
        std_dev_avg_io_tm_data, data_var = stddev(avg_io_tm_data)[0:2]
        std_dev_avg_io_tm_meta, meta_var = stddev(avg_io_tm_meta)[0:2]
        avg_io_tm_data = sum(avg_bucket_data) / len(avg_bucket_data)
        avg_io_tm_meta = sum(avg_bucket_meta) / len(avg_bucket_meta)

        self.tracelog['trace_io']['stats']['avg_io_tm_data'] = avg_io_tm_data
        self.tracelog['trace_io']['stats']['avg_io_tm_meta'] = avg_io_tm_meta
        self.tracelog['trace_io']['stats']['stddev_io_data'] = std_dev_avg_io_tm_data
        self.tracelog['trace_io']['stats']['stddev_io_meta'] = std_dev_avg_io_tm_meta
        self.tracelog['trace_io']['stats']['stddev_io_data_var'] = data_var
        self.tracelog['trace_io']['stats']['stddev_io_meta_var'] = meta_var
        self.tracelog['trace_io']['stats']['total_bytes'] = total_bytes
        self.tracelog['trace_io']['stats']['total_iops'] = total_iops

        return

    def _assemble_ts_stats(self):
        """Takes raw tracelog dict and computes ts stats"""

        # make a dict subkey
        self.tracelog['trace_ts']['stats']

        for k, v in self.tracelog['trace_ts'].iteritems():
           
            # messages received
            if 'tscHandleMsgDirectly' in v:
               
                try:
                    msg = v['tscHandleMsgDirectly']['msg']
                    fromwho = v['tscHandleMsgDirectly']['node_ip']

                    if not msg in self.tracelog['trace_ts']['stats']['received']:
                        self.tracelog['trace_ts']['stats']['received'][msg] = []

                    self.tracelog['trace_ts']['stats']['received'][msg].append(fromwho)
                except Exception as e:
                    print "Exception in 'received', '{0}'".format(e)
                    continue

            if 'tscHandleMsg' in v:
                try:
                    msg = v['tscHandleMsg']['msg']
                    fromwho = v['tscHandleMsg']['node_ip']

                    if not msg in self.tracelog['trace_ts']['stats']['received']:
                        self.tracelog['trace_ts']['stats']['received'][msg] = []

                    self.tracelog['trace_ts']['stats']['received'][msg].append(fromwho)
                except Exception as e:
                    print "Exception in 'received', '{0}'".format(e)
                    continue

            # messages sent
            if 'sendMessage' in v:

                try:
                    # need the 'msg' field from the tscSend operation
                    if 'msg' in v['tscSend']:
                        msg = v['tscSend']['msg']
                    else:
                        msg = v['tscSendReply']['msg']

                    towho = v['sendMessage']['node_ip']

                    if not msg in self.tracelog['trace_ts']['stats']['sent']:
                        self.tracelog['trace_ts']['stats']['sent'][msg] = []

                    self.tracelog['trace_ts']['stats']['sent'][msg].append(towho)
                except Exception as e:
                    print "Exception in 'sent', '{0}'".format(e)
                    continue
        return

    def _lookup_node_name(self, node):
        """Attempts to use the nodetable to find the hostname..."""

        if node in self.tracelog['trace_ts']['nodetable']:
            return self.tracelog['trace_ts']['nodetable'][node]
        else:
            return node     # just return the IP

    def _parse_io_trace(self, line):
        """Parses lines with TRACE_IO"""

        op = line.split()[3].strip(':')
        pid = line.split()[1]
        l = line.split()
        traceref = self.tracelog['trace_io']

        # we will figure out the OID of the IO operation based on the
        #   disknum:diskaddr address, since that's the only thing that is
        #   the same between the 3 lines of an IO operation, queued (QIO),
        #   starting (SIO), finished (FIO)

        # the line formats vary, sigh...
        if op == 'QIO':
            #oid = l[17]
            oid = l[17] + ":" + pid
            traceref[oid]['qio']['pid'] = pid
            traceref[oid]['qio']['tracetime'] = float(l[0])
            #traceref[oid]['qio']['diskid'] = l[15]
            traceref[oid]['qio']['disknum'] = l[17].split(':')[0]
            traceref[oid]['qio']['diskaddr'] = l[17].split(':')[1]
            traceref[oid]['qio']['optype'] = ' '.join(l[4:6])
            traceref[oid]['qio']['nSectors'] = int(l[19])
            traceref[oid]['qio']['align'] = l[21]
            #traceref[oid]['qio']['line'] = line

            # get the IO tags
            traceref[oid]['qio']['tags'] = (l[7:9])

        elif op == 'SIO':
            #oid = l[12]
            oid = l[12] + ":" + pid
            traceref[oid]['sio']['tracetime'] = float(l[0])
            traceref[oid]['sio']['pid'] = pid
            traceref[oid]['sio']['diskid'] = l[10]
            traceref[oid]['sio']['disknum'] = l[12].split(':')[0]
            traceref[oid]['sio']['diskaddr'] = l[12].split(':')[1]
            traceref[oid]['sio']['nSectors'] = int(l[14])
            #traceref[oid]['sio']['line'] = line

        elif op == 'FIO':
            #oid = l[17]
            oid = l[17] + ":" + pid
            traceref[oid]['fio']['tracetime'] = float(l[0])
            traceref[oid]['fio']['pid'] = pid
            traceref[oid]['fio']['diskid'] = l[15]
            traceref[oid]['fio']['disknum'] = l[17].split(':')[0]
            traceref[oid]['fio']['diskaddr'] = l[17].split(':')[1]
            traceref[oid]['fio']['optype'] = ' '.join(l[4:6])
            traceref[oid]['fio']['nSectors'] = int(l[19])
            traceref[oid]['fio']['finish_time'] = self.trace_start_epoch + \
                traceref[oid]['fio']['tracetime']
            #traceref[oid]['fio']['line'] = line

            # get the IO tags
            traceref[oid]['fio']['tags'] = (l[7:9])

        return

    def _parse_ts_trace(self, line):
        """Parses lines with TRACE_TS"""

        op = line.split()[3].strip(':')
        pid = line.split()[1]
        l = line.split()

        # make nested dictionary traceref...
        traceref = self.tracelog['trace_ts']

        if op == 'tscHandleMsgDirectly':
            # we don't want the reply messages for now...
            if l[7].strip('\'').strip('\',') == 'reply':
                return

            msg_id = l[9].strip(',')
            oid = msg_id + ':' + pid
            traceref[oid][op]['tracetime'] = float(l[0])
            #traceref[oid][op]['pid'] = pid
            traceref[oid][op]['msg'] = l[7].strip('\'').strip('\',')
            traceref[oid][op]['msg_id'] = msg_id
            traceref[oid][op]['len'] = l[11]
            #traceref[oid][op]['node_id'] = l[13]
            traceref[oid][op]['node_ip'] = l[14]
            #traceref[oid][op]['line'] = line

        elif op == 'tscSendReply':
            msg_id = l[9].strip(',')
            oid = msg_id + ':' + pid
            traceref[oid][op]['tracetime'] = float(l[0])
            #traceref[oid][op]['pid'] = pid
            traceref[oid][op]['msg'] = l[7].strip('\'').strip('\',')
            traceref[oid][op]['msg_id'] = msg_id
            #traceref[oid][op]['replyLen'] = l[11]
            #traceref[oid][op]['line'] = line

        elif op == 'sendMessage':
            msg_id = l[9].strip(',')
            oid = msg_id + ':' + pid
            traceref[oid][op]['tracetime'] = float(l[0])
            #traceref[oid][op]['pid'] = pid
            #traceref[oid][op]['node_id'] = l[5]
            traceref[oid][op]['node_ip'] = l[6]
            traceref[oid][op]['nodename'] = l[7].strip(':')
            traceref[oid][op]['msg_id'] = msg_id
            #traceref[oid][op]['type'] = l[11]
            #traceref[oid][op]['tagP'] = l[13]
            #traceref[oid][op]['seq'] = l[15]
            #traceref[oid][op]['state'] = l[17]
            #traceref[oid][op]['line'] = line

            # as a bonus, try to build an ip -> nodename table
            if not l[6] in traceref['nodetable']:
                traceref['nodetable'][l[6]] = l[7].strip(':')

        elif op =='tscHandleMsg':
            msg_id = l[9].strip(',')
            oid = msg_id + ':' + pid
            traceref[oid][op]['tracetime'] = float(l[0])
            #traceref[oid][op]['pid'] = pid
            traceref[oid][op]['msg'] = l[7].strip('\'').strip('\',')
            traceref[oid][op]['msg_id'] = msg_id
            traceref[oid][op]['len'] = l[9]
            traceref[oid][op]['node_id'] = l[13]
            traceref[oid][op]['node_ip'] = l[14]
            #traceref[oid][op]['line'] = line

        elif op == 'tscSend':
            if "rc = 0x" in line:  # useless line
                return
            oid = l[13] + ':' + pid
            traceref[oid][op]['tracetime'] = float(l[0])
            #traceref[oid][op]['pid'] = pid
            traceref[oid][op]['msg'] = l[7].strip('\'').strip('\',')
            #traceref[oid][op]['n_dest'] = l[9]
            #traceref[oid][op]['data_len'] = l[11]
            traceref[oid][op]['msg_id'] = l[13]
            #traceref[oid][op]['msg_buf'] = l[15]
            #traceref[oid][op]['mr'] = l[17]

        else:
            # do nothing
            return
        
        return

    # Public methods
    #
    #
    def parse_trace(self, filename, filters=None):

        try:
            # create a filter list
            filter_list = [self._FILTER_MAP[i] for i in filters.split(',')]
        except KeyError as ke:
            print "Error, filter ({0}) is not a valid filter.".format(ke)
            print "Please use the following filters: {0}".format(
                    self._FILTER_MAP.keys())
            sys.exit(1)

        skip = 0
        trace_start = 0

        # open the trace report file
        try:
            f = gzip.open(filename, 'rb')
        except IOError as ioe:
            print "Error opening file: {0}".format(ioe)
            sys.exit(0)

        for line in f:

            # grab the date from the first line to use it later...
            if skip == 0:
                ld = line.split()[2:]   # longdate
                datearg = "{0}-{1}-{2} {3}:{4}:{5}".format(ld[-1], ld[1],
                        ld[2], ld[3].split(':')[0], ld[3].split(':')[1],
                        ld[3].split(':')[2])
                # will be in epoch time...
                self.trace_start_epoch = int(datetime.datetime.strptime(
                            datearg, '%Y-%b-%d %H:%M:%S').strftime('%s'))
                self.tracelog['start_epoch'] = self.trace_start_epoch
                skip += 1
                continue
            elif skip == 1:
                ld = line.split()[3:]   # longdate
                datearg = "{0}-{1}-{2} {3}:{4}:{5}".format(ld[-1], ld[1], 
                        ld[2], ld[3].split(':')[0], ld[3].split(':')[1], 
                        ld[3].split(':')[2])
                # will be in epoch time...
                self.trace_stop_epoch = int(datetime.datetime.strptime(
                            datearg, '%Y-%b-%d %H:%M:%S').strftime('%s'))
                self.tracelog['stop_epoch'] = self.trace_stop_epoch
                skip += 1
                continue
            # this is to skip the lines 3-8. shameful to say the least...
            elif skip > 1 and skip < 8:
                skip += 1
                continue
            elif line.split()[2].strip(':') not in filter_list:
                # skip any lines we don't want
                continue
            elif line.split()[2].strip(':') == 'TRACE_IO' and \
                line.split()[3] in self.IORegex:
                self._parse_io_trace(line)
            elif line.split()[2].strip(':') == 'TRACE_TS' and \
                line.split()[3] in self.TSRegex:
                self._parse_ts_trace(line)
            else:
                continue

        # assemble the stats for enabled filters
        if 'io' in filters.split(','):
            self._assemble_io_stats()
        if 'ts' in filters.split(','):
            self._assemble_ts_stats()

        return

    def print_disk_summary(self, max_zscore=4):
        """Print out a summary of disk statistics...

        @param max_zscore: the number of standard deviations above the mean
            we'd like to filter
        @type max_zscore: int

        @return: NOTHING
        """

        if len(self.tracelog['trace_io'].keys()) < 1:
            print "No disk data collected."
            return

        # total number of seconds the trace ran...
        trace_elapsed_secs = self.tracelog['stop_epoch'] -\
             self.tracelog['start_epoch']

        # summarize some disk stats
        if self.verbose:
            print "Disk Summary:"
            print "*" * 80
            for k, v in sorted(self.tracelog['trace_io']['disks'].iteritems()):
                formatstr = "Disk: {0}, IOPS: {1}, Avg_IO_T: {2:.3f}, " + \
                    "Avg_IO_Sz: {3}, Longest_IO: {4:.3f}, Total_Bytes: {5}, " + \
                    "Total_IO_Time: {6:.3f}"
                try:
                    print formatstr.format(
                    k, v['stats']['num_iops'], 
                    v['stats']['avg_io_tm'],
                    v['stats']['avg_io_sz'], 
                    v['stats']['longest_io'],
                    v['stats']['total_bytes_io'],
                    v['stats']['total_time_io'])

                except ValueError as ve:
                    continue

        print
        print
        print "Disks with Avg IO times > {0} standard deviations".format(
                max_zscore)
        print "Average IO Times (data: {0:.4f}, metadata: {1:.4f})".format(
            self.tracelog['trace_io']['stats']['avg_io_tm_data'], 
            self.tracelog['trace_io']['stats']['avg_io_tm_meta'])
        print "Deviation Variance: ( data: {0:.2f}, metadata: {1:.2f})".format(
                max_zscore * (self.tracelog['trace_io']['stats']['stddev_io_data_var']),
                max_zscore * (self.tracelog['trace_io']['stats']['stddev_io_meta_var']) )
        #print "Variance: ( data: {0:.4f}, metadata: {1:.4f} )".format(
        #        data_var, meta_var)
                                                            
        print "*" * 80

        for k, v in sorted(self.tracelog['trace_io']['disks'].iteritems()):
            if not v['stats']['avg_io_tm']:
                continue    # some disks just dont have the right fields...
            else:
                d_io_tm = v['stats']['avg_io_tm']
                strfrmt = "Disk: {0}, Avg_IO_Time {1:.4f}, ZScore: {2:.4f}"

            try:
                if v['stats']['avg_io_sz'] > 1048576: # data disks
                    mean = self.tracelog['trace_io']['stats']['avg_io_tm_data']
                    stddev = self.tracelog['trace_io']['stats']['stddev_io_data']
                    zs = zscore(d_io_tm, mean, stddev)
                    if zs > max_zscore:
                        print strfrmt.format(k, d_io_tm, zs)

                else:   # metadata
                    mean = self.tracelog['trace_io']['stats']['avg_io_tm_meta']
                    stddev = self.tracelog['trace_io']['stats']['stddev_io_meta']
                    zs = zscore(d_io_tm, mean, stddev)
                    if zs > max_zscore:
                        print strfrmt.format(k, d_io_tm, zs)

            except TypeError as te:
                print "TypeError: {0}".format(te)
                continue

        total_bytes = self.tracelog['trace_io']['stats']['total_bytes']
        total_iops = self.tracelog['trace_io']['stats']['total_iops']

        print
        print
        print "Totals:"
        print "*" * 80
        print "Total Gigabytes Read/Written: {0}".format(float(total_bytes) / 1024 / 1024 / 1024)
        print "Total IO Operations: {0}".format(total_iops)
        print "Total IO Trace Time: {0} secs".format(trace_elapsed_secs)
        print "Total GB/s: {0:.3f}".format(
                float(total_bytes / 1024 / 1024 / 1024) / float(trace_elapsed_secs))

    def print_network_summary(self):
        """Prints out network summary"""

        if len(self.tracelog['trace_ts'].keys()) < 1:
            print "No network data collected."
            return

        trace_elapsed_secs = self.tracelog['stop_epoch'] -\
             self.tracelog['start_epoch']

        # Received messages
        print "Received messages:"
        print "*" * 80
        for k, v in self.tracelog['trace_ts']['stats']['received'].iteritems():
            print "Msg: '{0}', Times_Received: '{1}'".format(k, len(v))
            if self.verbose:
                for n,c in count_iterations(v).items():
                    print "\t\tNode: {0}, Count: {1}".format(
                            self._lookup_node_name(n),c)

        # Sent messages
        print
        print
        print "Sent messages:"
        print "*" * 80
        for k, v in self.tracelog['trace_ts']['stats']['sent'].iteritems():
            print "Msg: '{0}', Times_Sent: '{1}'".format(k, len(v))
            if self.verbose:
                for n,c in count_iterations(v).items():
                    print "\t\tNode: {0}, Count: {1}".format(
                            self._lookup_node_name(n),c)
        print
        print
        print "Totals:"
        print "*" * 80
