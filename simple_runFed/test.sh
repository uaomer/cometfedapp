#! /bin/bash

counter=0

while [ $counter -lt 10 ];do
./startFedClient.sh CPT red 5 10
let counter++
done

