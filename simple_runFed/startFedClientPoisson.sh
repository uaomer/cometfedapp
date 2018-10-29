#!/bin/bash

PUBLICIP=dell14.rutgers.edu
PORT=5560


if [ $# -eq 6 ]; then

java -Xms256m -Xmx1024m -cp $CLASSPATH tassl.automate.federation.FedClientPoisson -serverIP $PUBLICIP -serverPort $PORT -messageType OPT -tupleTag $1 -taskType $2 -budget $3 -deadline $4 -timestamp $5 -distFile $6

else
 echo "1: ./startFedClient.sh <REQ|CPT> <red|black> <budget> <deadline> <timestamp>"
 echo "2: ./startFedClient.sh <END> <REQ|CPT> <red|black>"
fi
