For test purpose:
If user want to use bidding mechanism, define only "REQ,red" in subscriptions.properties file
If user want to use normal computation mechanism, define only "CPT,red".

In other words, don't define both "REQ,red" and "CPT,red" in the subscription file. Otherwise, there might be unstable problem. The reason is that every FedWorker would create a thread listen on the same port which will receive reply results from appMaster. When there are multiple FedWorker, then the result could be sent to anyone of them.

testmode=enableSeparateExecution means the user want to enable appMaster/appWorker doing the real computation.
testmode=disableSeparateExecution means all computation are done by fedWorker instead of sending them to appWorker.

