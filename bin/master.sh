#!/bin/bash

#
# Tencent is pleased to support the open source community by making TubeMQ available.
#
# Copyright (C) 2012-2019 Tencent. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at
#
# https://opensource.org/licenses/Apache-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.
#

#project directory
if [ -z "$BASE_DIR" ] ; then
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG="`dirname "$PRG"`/$link"
    fi
  done
  BASE_DIR=`dirname "$PRG"`/..

  # make it fully qualified
  BASE_DIR=`cd "$BASE_DIR" && pwd`
  #echo "TubeMQ master is at $BASE_DIR"
fi

source $BASE_DIR/bin/env.sh

AS_USER=`whoami`
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/master.log"
PID_DIR="$BASE_DIR/logs"
PID_FILE="$PID_DIR/.master.run.pid"

function running(){
	if [ -f "$PID_FILE" ]; then
		pid=$(cat "$PID_FILE")
		process=`ps aux | grep " $pid "|grep "\-Dtubemq\.home=$BASE_DIR" | grep -v grep`;
		if [ "$process" == "" ]; then
	    	return 1;
		else
			return 0;
		fi
	else
		return 1
	fi
}

function start_server() {
	if running; then
		echo "Master is running."
		exit 1
	fi

    mkdir -p $PID_DIR
    touch $LOG_FILE
    mkdir -p $LOG_DIR
    chown -R $AS_USER $PID_DIR
    chown -R $AS_USER $LOG_DIR
    
    config_files="-f $BASE_DIR/conf/master.ini"
    
	echo "Starting Master server..."
    
   	echo "$JAVA $MASTER_ARGS  com.tencent.tubemq.server.tools.MasterStartup $config_files"
    sleep 1
    nohup $JAVA $MASTER_ARGS  com.tencent.tubemq.server.tools.MasterStartup $config_files 2>&1 >>$LOG_FILE &
    echo $! > $PID_FILE
    chmod 755 $PID_FILE
}

function stop_server() {
	if ! running; then
		echo "Master is not running."
		exit 1
	fi
	count=0
	pid=$(cat $PID_FILE)
	while running;
	do
	  let count=$count+1
	  echo "Stopping TubeMQ master $count times"
	  if [ $count -gt 10 ]; then
	  	  echo "kill -9 $pid"
	      kill -9 $pid
	  else
	      kill $pid
	  fi
	  sleep 6;
	done	
	echo "Stop TubeMQ master successfully."
	rm $PID_FILE
}

function help() {
    echo "Usage: master.sh {start|stop|restart}" >&2
    echo "       start:      start the master server"
    echo "       stop:       stop the master server"
    echo "       restart:    restart the master server"
}

command=$1
shift 1
case $command in
    start)
        start_server $@;
        ;;    
    stop)
        stop_server $@;
        ;;
    restart)
        $0 stop $@
        $0 start $@
        ;;
    help)
        help;
        ;;
    *)
        help;
        exit 1;
        ;;
esac
