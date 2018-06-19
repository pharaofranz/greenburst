#!/usr/bin/env python

import subprocess
import datetime

statusStr = ''

#ncnodes = 4 # number of compute nodes
#cnodes = []
#for nid in range(ncnodes): cnodes.append('abc%i'%nid)

statusStr += 'GREENBURST system status as of %s\n\n'%datetime.datetime.now()

## Power status
#for cnode in cnodes:
#    statusStr += '%s power status:\n'%cnode
#    statusStr += '/usr/bin/ipmitool -I lanplus -U ADMIN -P ADMIN -H %sx power status\n'%cnode
#    output = subprocess.check_output('/usr/bin/ipmitool -I lanplus -U ADMIN -P ADMIN -H %sx power status'%cnode, shell=True)
#    statusStr += output
#    statusStr += '\n'

## Disk space and latest files
#for cnode in cnodes:
#    try:
#        statusStr += '%s disk usage:\n'%cnode
#        output = subprocess.check_output('ssh griffin@%s \'df -h /data\''%cnode, shell=True)
#        statusStr += output
#        statusStr += '\n'
#
#        statusStr += '%s most recent files:\n'%cnode
#        output = subprocess.check_output('ssh griffin@%s \'ls -1rtlhA /data/Survey/Data | tail -n 6\''%cnode, shell=True)
#        statusStr += output
#        statusStr += '\n'
#    except Exception, e:
#        statusStr += 'ERROR: SSH to node %s not connecting, restart node\n\n'%cnode

statusStr += 'GREENBURST uptime:\n'
statusStr += subprocess.check_output('uptime', shell=True)

statusStr += '\nGREENBURST current users:\n'
statusStr += subprocess.check_output('users', shell=True)

statusStr += '\nGREENBURST head node disk usage:\n'
statusStr += subprocess.check_output('df -h / /sdata /ldata', shell=True)

print statusStr
