#!/bin/bash

usage() {
    echo "Usage: start-scache"
    exit 1
}

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

. "$sbin/config.sh"



SLAVES=`cat ${SCACHE_HOME}/conf/slaves`
# echo $USER


echo "start master"
. "$sbin/start-master.sh"
sleep 5

for slave in $SLAVES; do
    echo "start Scache on $slave"
    exec ssh $slave ${SCACHE_HOME}/sbin/start-client.sh & 
done
