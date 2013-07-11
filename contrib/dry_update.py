#!/usr/bin/env python
import argparse
import json
import operator
import gpfs.node
import sys
#from fabric.tasks import execute
#from fabric.api import env, local, run, parallel
#from fabric.context_managers import hide, show, settings

parser = argparse.ArgumentParser(description='experiment with json dumps of gpfs states')
parser.add_argument('-f', '--file', dest='jsonfile', required=True,
            help='json file to load')
parser.add_argument('-n', '--numgroups', dest='numgroups', required=True, type=int,
            help='number of groups for the actions')
parser.add_argument('--reboot_node', dest='reboot_node', default=False, required=False, type=bool,
            help='number of groups for the actions')
args = parser.parse_args()


_NUM_GROUPS=args.numgroups
state = json.load(open(args.jsonfile))
reboot_node = args.reboot_node

#node_queue = sorted( [(i, state['nodes'][i]['weight']) \
#                    for i in state['nodes'].keys()], \
#                    key=operator.itemgetter(1) )

# create gpfs.node.Node() object and pass the json state
gpfsnode = gpfs.node.Node(state)

nodelist = []

for n in state['nodes'].itervalues():
    fg = n['fg']

    # get first failure group (should be fixed later)
    try:
        ffg = fg[0]
    except:
        ffg = 0

    nodelist.append((n['weight'], ffg , n)) 

# sort the nodelist based on node weights (quorum/manager/client/etc)
nodelist.sort(reverse=True)

# create some nested lists that are separate node groups
nodequeue = {}
for k in range(0,_NUM_GROUPS):
    nodequeue[k] = []


# here we take the nodelist and take an entry off the top and add it to it's own
#   nodequeue[int] group
i = 0
for n in nodelist:
    nodequeue[i].append(n[2])
    i=(i+1)%_NUM_GROUPS

# add nodes (shortname) to a list of finished nodes...
finished_queue = []
    
for x in nodequeue.itervalues():
    print "===== GROUP ====="

    #env.hosts = [ str(s['short_name']) for s in x ]

    #print env.hosts

    #env.hosts = [ group_hosts, ] 
    #env.use_hostbased = True

    #run('uptime')

    for y in x:

        # is this node a cluster or filesystem manager?
        for node in state['managers'].items():

            if y['short_name'] == node[1]:
                managerof = node[0]
                print "\'%s\' manages: %s" % ( y['short_name'], managerof)

                # figure out if this node is a cluster manager, OR an fs manager
                if managerof == 'cluster':
                    manager_type = 'cluster'
                    command = "change_cluster_manager"
                else:
                    manager_type = 'fs'
                    command = "change_fs_manager"
                

                # find a node that is a quorum-manager, and move the role to it
                #   - however, first check if there is any nodes in the finished
                #   queue that can handle the task first...
                #   - also, make sure to update the cluster/fs manager value...
                #
                # compare the finished_queue list with the manager_nodes list
                sets = set(finished_queue)
                compare = list(sets.intersection(state['quorum_managers']))

                # select first node from finished_queue
                if len(compare) > 0:
                    if managerof == 'cluster':
                        print "run(change_cluster_manager, %s) (FQ)" % (compare[0])
                        state['managers']['cluster'] = compare[0]
                    else:
                        print "run(change_fs_manager, %s, %s) (FQ)" % ( compare[0], node[0])
                        state['managers'][node[0]] = compare[0]
                # no qm nodes in finished_queue, use any other qm node
                else:
                    # find new manager node
                    for n in state['quorum_managers']:
                        if y['short_name'] != n:
                            new_manager = n
                            break
                    
                    if managerof == 'cluster':
                        print "run(change_cluster_manager, %s)" % (new_manager)
                        state['managers']['cluster'] = new_manager
                    else:
                        print "run(change_fs_manager, %s, %s)" % (new_manager, node[0])
                        state['managers'][node[0]] = new_manager

                    
        # now, do the update/action/etc
        state['nodes'][y['short_name']]['action_status'] = 'running_action'
        #print "Node: %s, Roles: %s" % (state['nodes'][y['short_name']]['short_name'], state['nodes'][y['short_name']]['roles'])
        print "run(do_something) to node: %s" % y['short_name']

        if reboot_node is True:
            state['nodes'][y['short_name']]['action_status'] = 'rebooting'
            print "Rebooting node: %s" % y['short_name']
            
        # check the state of GPFS on the node
        #gpfs.node.get_gpfs_state(node[1])
        
        
        

        # set that node's state to finished
        state['nodes'][y['short_name']]['action_status'] = 'finished'
        finished_queue.append(y['short_name'])


