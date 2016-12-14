#!/bin/bash

PID=`jps | grep Client | awk '{print $1}'`

kill $PID
