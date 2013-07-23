class NodeQueue(object):

    def __init__(self, state):
        '''Need to pass a state dictionary that
        was generated via the gpfs.cluster.build_cluster_state()
        method
        '''
        self.state = state
        self.nodequeue = {}

    
    def create_queue(self, num_groups):
        '''Create a nodequeue based on weights, failure group, etc

        @param num_groups: number of update groups to have 
        @type num_groups: int


        @return NOTHING
        '''

        nodelist = []

        for n in self.state['nodes'].itervalues():
            fg = n['fg']

            # get first failure group (should be fixed later)
            try:
                ffg = fg[0]
            except:
                ffg = 0

            nodelist.append((n['weight'], ffg , n))

        # sort the nodelist based on node weights (quorum/manager/client/etc)
        nodelist.sort(reverse=True)

        for k in range(0,num_groups):
            self.nodequeue[k] = []


        # here we take the nodelist and take an entry off the top and add it to its
        #   own nodequeue[int] group
        i = 0
        for n in nodelist:
            self.nodequeue[i].append(n[2])
            i= (i+1) % num_groups

        return

    def print_queue(self):
        '''prints out the node queue'''

        for group in self.nodequeue.itervalues():
            print "===== GROUP ====="
            members = [ str(member['short_name']) for member in group ]
            print "=== Members: {0}".format(members)

            for node in group:
                print "Node: {0}, Roles: {1}, FG: {2}, Score: {3}".format(
                    node['short_name'], node['roles'], node['fg'],
                    node['weight'])

        return    
    
