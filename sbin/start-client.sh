#!/bin/bash

usage() {
    echo "Usage: start-client"
    exit 1
}

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`


. "$sbin/config.sh"


JAR=${SCACHE_HOME}/target/scala-2.11/SCache-assembly-0.1-SNAPSHOT.jar

nohup java -cp $JAR org.scache.deploy.ScacheClient >/dev/null 2>&1 &
