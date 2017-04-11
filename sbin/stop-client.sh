#!/bin/bash

PID=`jps | grep ScacheClient | awk '{print $1}'`

kill $PID
