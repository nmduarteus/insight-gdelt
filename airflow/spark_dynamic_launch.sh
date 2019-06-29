#!/bin/bash

MAINPATH=/usr/local/spark
MAINCONF=$MAINPATH/conf
MAINBIN=$MAINPATH/sbin

if [ "$1" != "" ]; then
    if [ "$1" = "addnode" ]; then
        aws ec2 describe-instances --filters 'Name=tag:Name,Values=autoscale' | sed -n "s/^.*PublicDnsName\": \"\(.*\)\",.*$/\1/p" | sed '/^$/d' | sort --unique > $MAINCONF/autonodes
        cat $MAINCONF/slaves $MAINCONF/autonodes > $MAINCONF/slaves1
        mv $MAINCONF/slaves1 $MAINCONF/slaves
    elif [ "$1" = "removenode" ]; then
       echo "Removing Nodes"
       diff $MAINCONF/slaves $MAINCONF/autonodes | cut -c3- | tail -n +2 > $MAINCONF/slaves2
       mv $MAINCONF/slaves2 $MAINCONF/slaves
    fi

    sh $MAINBIN/stop-all.sh
    sh $MAINBIN/start-all.sh
else
    echo "Option was not defined"
fi