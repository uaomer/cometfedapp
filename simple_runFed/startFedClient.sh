#!/bin/bash

PUBLICIP=dell01.rutgers.edu
PORT=5560


if [ $# -eq 5 ]; then

java -Xms256m -Xmx1024m -cp $CLASSPATH tassl.automate.federation.FedClient -serverIP $PUBLICIP -serverPort $PORT -messageType OPT -tupleTag $1 -taskType $2 -budget $3 -deadline $4 -timestamp $5

elif [ $# -eq 3 ]; then
java -Xms256m -Xmx1024m -cp $CLASSPATH tassl.automate.federation.FedClient -serverIP $PUBLICIP -serverPort $PORT -messageType $1 -tupleTag $2 -taskType $3

elif [ $# -eq 6 ]; then
java -Xms256m -Xmx1024m -cp $CLASSPATH tassl.automate.federation.FedClient -serverIP $PUBLICIP -serverPort $PORT -tupleTag $1 -taskType $2 -budget $3 -deadline $4 -destIP $5 -destPort $6

else
 echo "1: ./startFedClient.sh <REQ|CPT> <red|black> <budget> <deadline> <timestamp>"
 echo "2: ./startFedClient.sh <END> <REQ|CPT> <red|black>"
fi
