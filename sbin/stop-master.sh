#!/bin/bash

PID=`jps | grep Master | awk '{print $1}'`

kill $PID
