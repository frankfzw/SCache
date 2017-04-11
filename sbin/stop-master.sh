#!/bin/bash

PID=`jps | grep ScacheMaster | awk '{print $1}'`

kill $PID
