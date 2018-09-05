#!/bin/sh

PIDS=$(ps ax | grep -i 'postBackClient' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No post back Client to stop"
  exit 1
else 
  kill -s TERM $PIDS
  echo "close PostBackClient"
fi
