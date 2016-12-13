#!/bin/bash

usage() {
    echo "Usage: start-master"
    exit 1
}


cat ${SCACHE_HOME}/conf/scache.conf | \
while read CONF; do
    # echo $CONF
done
