#!/bin/bash

PUBLICIP=dell01.rutgers.edu
PORT=5560


if [ $# -eq 6 ]; then

java -Xms256m -Xmx1024m -cp $CLASSPATH tassl.automate.federation.FedClient -serverIP $PUBLICIP -serverPort $PORT -messageType OPT -tupleTag $1 -taskType $2 -budget $3 -deadline $4 -timestamp $5 -appSpecific $6

else
 echo "1: ./startFedClient.sh <REQ|CPT> <red|black> <budget> <deadline> <timestamp> <appSpecific(cound be multiple split with '-')>"
fi
