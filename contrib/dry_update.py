#!/usr/bin/env python
import argparse
import json
import operator
#import gpfs.node
import sys
#from fabric import env

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
#_NUM_GROUPS=4
reboot_node = args.reboot_node
#state = json.load(open("state.json"))

#node_queue = sorted( [(i, state['nodes'][i]['weight']) \
#                    for i in state['nodes'].keys()], \
#                    key=operator.itemgetter(1) )

nodelist = []

for n in state['nodes'].itervalues():
    fg = n['fg']
    try:
        anothervar = fg[0]
    except:
        anothervar = 0

    nodelist.append((n['weight'], anothervar, n)) 

nodelist.sort(reverse=True)

nodequeue = {}
for k in range(0,_NUM_GROUPS):
    nodequeue[k] = []

i = 0
for n in nodelist:
    nodequeue[i].append(n[2])
    i=(i+1)%_NUM_GROUPS

finished_queue = []
    
for x in nodequeue.itervalues():
    print "===== GROUP ====="
    for y in x:
        #env.host = y['short_name']

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

                    
            #

        # now, do the update/action/etc
        state['nodes'][y['short_name']]['action_status'] = 'running_action'
        print "run(do_something) to node: %s" % y['short_name']

        if reboot_node is True:
            state['nodes'][y['short_name']]['action_status'] = 'rebooting'
            print "Rebooting node: %s" % y['short_name']
            
        # check the state of GPFS on the node
        
        

        # set that node's state to finished
        state['nodes'][y['short_name']]['action_status'] = 'finished'
        finished_queue.append(y['short_name'])


