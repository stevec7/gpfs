class mmpmon(object):

    def __init__(self):
        self.name = 'mmpmon'
        self.nodefields = { '_n_': 'nodeip', '_nn_': 'nodename',
            '_rc_': 'status', '_t_': 'seconds', '_tu_': 'microsecs',
            '_br_': 'bytes_read', '_bw_': 'bytes_written', 
            '_oc_': 'opens', '_cc_': 'closes', '_rdc_': 'reads',
            '_wc_': 'writes', '_dir_': 'readdir', '_iu_': 'inode_updates' }

        self.nodelabels = {}

        self.fsfields = { '_n_': 'nodeip', '_nn_': 'nodename',
            '_rc_': 'status', '_t_': 'seconds', '_tu_': 'microsecs',
            '_cl_': 'cluster', '_fs_': 'filesystem', '_d_': 'disks',
            '_br_': 'bytes_read', '_bw_': 'bytes_written', 
            '_oc_': 'opens', '_cc_': 'closes', '_rdc_': 'reads',
            '_wc_': 'writes', '_dir_': 'readdir', '_iu_': 'inode_updates' }
   
        self.fslabels = {}

    def _add_nodes(self, nodelist):
        """Add nodes to the mmpmon nodelist"""
        return
        
    def _reset_stats(self):
        """Reset the IO stats"""
        return
