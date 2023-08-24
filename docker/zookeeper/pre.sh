#!/bin/bash

# This puts zookeeper node id to its 'myid' file
# For more info look here at steps 4, 5 : http://zookeeper.apache.org/doc/r3.5.7/zookeeperAdmin.html#sc_zkMulitServerSetup
mkdir -p /tmp/zookeeper
for ip in $(ifconfig -a | grep 'inet' | awk '{print $2}')
do
    ID=$(grep -F "$ip" /opt/zookeeper/conf/zoo.cfg | cut -d= -f1 | cut -d. -f2)
    if [ -n "$ID" ]
    then
        echo "$ID" > /tmp/zookeeper/myid
        break
    fi
done
