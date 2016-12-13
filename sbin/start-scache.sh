#!/bin/bash

usage() {
    echo "Usage: start-scache"
    exit 1
}

SLAVES=`cat ${SCACHE_HOME}/conf/slaves`
# echo $USER

sh ./start-master.sh

for slave in $SLAVES; do
    echo $slave
done
