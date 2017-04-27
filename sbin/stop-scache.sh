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


for slave in $SLAVES; do
    echo "stop Scache on $slave"
    exec ssh $slave ${SCACHE_HOME}/sbin/stop-client.sh & 
done

sleep 1

echo "stop master"
. "$sbin/stop-master.sh"


