/*
 * Copyright (c) 2009, NSF Center for Autonomic Computing, Rutgers University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and
 * the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of the NSF Center for Autonomic Computing, Rutgers University, nor the names of its
 * contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

/*
 * MeteorStarter.java
 *
 * Created on Nov 24, 2010 12:06:45 PM
 */
package tassl.automate.federation;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.concurrent.RequestVariable;
import programming5.io.ArgHandler;
import programming5.io.Debug;
import programming5.io.FileHandler;
import programming5.io.Serializer;
import programming5.net.IncompleteResultException;
import programming5.net.MalformedMessageException;
import programming5.net.Message;
import programming5.net.MessageArrivedEvent;
import programming5.net.MessagingClient;
import programming5.net.NetworkException;
import programming5.net.PluggableClient;
import programming5.net.ServerDaemon;
import programming5.net.ServiceObject;
import programming5.net.ServiceObjectFactory;
import programming5.net.TerminationAwareSubscriber;
import tassl.automate.agent.CloudBridgingScheduler;
import tassl.automate.application.node.FedMeteorDrtsPeer;
import tassl.automate.application.node.FedMeteorDrtsPeerControl;
import tassl.automate.network.ControlConstant;
import tassl.automate.overlay.OverlayControlInterface;
import tassl.automate.overlay.OverlayControlMessage;
import tassl.automate.overlay.chord.ChordID;
import tassl.automate.util.ConfigurationFileManager;
import tassl.automate.util.FedLog;
import tassl.automate.util.SSLClient;
import tassl.automate.util.SSLServerDaemon;
import tassl.automate.util.programming5.TCPclient;
import tassl.automate.util.programming5.TCPServerDaemon;

/**
 * @author Samprita Hegde
 * created on Nov 24, 2010 12:06:45 PM
 *
 */
public class FedMeteorStarter implements ServiceObjectFactory {

    public static enum NodeType {

        MASTER, WORKER, REQUEST_HANDLER, WORKFLOW_MANAGER, CLOUD_AGENT
            };
    public FedMeteorDrtsPeer peer;
    protected OverlayControlInterface<FedMeteorDrtsPeer> controlInterface;
    protected Vector<String> nodeList;
    protected Hashtable<String, String> baseProperties;
    protected Hashtable<String, Hashtable<String, String>> nodeDescriptors;
    protected Hashtable<String, Hashtable<String, String>> exceptionTable;
    protected String localNode;
    protected ServerDaemon serverDaemon;
    protected List bootStrapNodes; //list of nodes that are part of the overlay. We contact them to join the same overlay.
    boolean ismaster=false;
    
    public FedMeteorStarter() {
        try {
            serverDaemon = new TCPServerDaemon(this, ControlConstant.COMET_CONTROL_PORT);
            serverDaemon.start();
            //System.out.println("Meteor control server started");
            FedLog.writeLog("manageLog","manageLog%g.log","Meteor control server started","info");
        } catch (NetworkException ex) {
            //Logger.getLogger(FedMeteorStarter.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
    }
    
    /** Creates a new instance of MeteorStarter */
    public FedMeteorStarter(String nodeFileName, String portFileName, String[] appPropertyFiles, String[] exceptionFileNames, List bootStrapNodes) {
        ismaster=false;
        this.bootStrapNodes=bootStrapNodes;
        
        controlInterface = new OverlayControlInterface<FedMeteorDrtsPeer>(ControlConstant.OVERLAY_CONTROL_PORT);
        nodeList = ConfigurationFileManager.generateNodeList(nodeFileName, portFileName);
        localNode = nodeList.elementAt(0);
        baseProperties = ConfigurationFileManager.parsePropertyFiles(appPropertyFiles);
        nodeDescriptors = new Hashtable<String, Hashtable<String, String>>();
        exceptionTable = ConfigurationFileManager.parseExceptionFiles(exceptionFileNames);
        
        enableDebug();

        //this is needed by MeteorSpaceService. Since this cannot change, we can put in the system properties
        System.setProperty("RoutingKeys",baseProperties.get("RoutingKeys"));
        
        //set properties
        for (String node : nodeList) {
            nodeDescriptors.put(node, ConfigurationFileManager.customizeProperties(baseProperties, node, exceptionTable));
        }

        //set total num of workers
        int numNotWorker = 0;
        if (exceptionTable != null) {
            for (String nodekey : exceptionTable.keySet()) {
                Hashtable<String, String> nodeExceptions = new Hashtable<String, String>();
                nodeExceptions = exceptionTable.get(nodekey);
                for (Entry<String, String> exception : nodeExceptions.entrySet()) {
                    if (exception.getKey().equalsIgnoreCase("meteor.NodeType")
                        && NodeType.valueOf(exception.getValue()) != NodeType.WORKER) {
                        numNotWorker++;
                    }
                }
            }
        }

        //set role of node
        int masterSequence = 0;
        int workerSequence = 0;
        int reqHandlerSequence = 0;
        int wfManagerSequence = 0;
        int agentSequence = 0;
        for (Hashtable<String, String> propertyTable : nodeDescriptors.values()) {
            
            propertyTable.put("WorkerNum", "" + (nodeList.size() - numNotWorker));
            
            NodeType peerRole;
            String nodeType = propertyTable.get("meteor.NodeType");
            if (nodeType != null) {
                peerRole = NodeType.valueOf(nodeType);
            } else {
                peerRole = NodeType.WORKER;
            }
            switch (peerRole) {
                case MASTER: {
                    propertyTable.put("MasterID", Integer.toString(++masterSequence));
                    ismaster=true;
                }
                    break;
                case WORKER: {
                    propertyTable.put("WorkerID", Integer.toString(++workerSequence));
                }
                    break;
                case REQUEST_HANDLER: {
                    propertyTable.put("ReqHandlerID", Integer.toString(++reqHandlerSequence));
                }
                    break;
                case WORKFLOW_MANAGER: {
                    propertyTable.put("WFManagerID", Integer.toString(++wfManagerSequence));
                }
                    break;
                case CLOUD_AGENT: {
                    propertyTable.put("AgentID", Integer.toString(++agentSequence));
                }
                    break;
            }
        }
        
        //JAVI: I think that the daemon is only needed for the master to receive tasks or get workers leave signal
        if(ismaster==true){
            try {
                if(baseProperties.get("chord.NETWORK_CLASS") != null && baseProperties.get("chord.NETWORK_CLASS").equals("SSL")){
                    serverDaemon = new SSLServerDaemon(this, ControlConstant.COMET_CONTROL_PORT);
                    //log SSL serverDaemon
                    //System.out.println("start sslserver daemon");
                    FedLog.writeLog("manageLog","manageLog%g.log", "start sslserver daemon","info");

                }
                else{
                    serverDaemon = new TCPServerDaemon(this, ControlConstant.COMET_CONTROL_PORT);
                    //log TCP serverDaemon
                    //System.out.println("start tcpserver daemon");
                    FedLog.writeLog("manageLog","manageLog%g.log","start tcpserver daemon","info");

                }
                serverDaemon.start();

            } catch (NetworkException ex) {
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            }
        }
        
        if (Debug.isEnabled("MeteorStarter")) {
            for (String node : nodeList) {
                Hashtable<String, String> p = nodeDescriptors.get(node);
                //Debug.println("[MeteorStarter] " + node + ": nodetype=" + p.get("meteor.NodeType") + " masterid=" + p.get("MasterID") + " workerid=" + p.get("WorkerID") + " reqHandlerid=" + p.get("ReqHandlerID"), "MeteorStarter");
            }
        }
    }

    public void enableDebug(){
        String debugSets = baseProperties.get("meteor.DEBUG");
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    Debug.enable(setName);
                }
            }
            else {
                Debug.enable("meteor");
            }
        }
    }
    
    public Class getDrtsControlClass(){
        return FedMeteorDrtsPeerControl.class;
    }
    
    public void initMeteor() throws URISyntaxException, IncompleteResultException {
        //Debug.println("[MeteorStarter] localNode= " + localNode, "MeteorStarter");
        FedLog.writeLog("manageLog","manageLog%g.log","[MeteorStarter] localNode= " + localNode,"info");
        try {
            String joinBitsProperty = baseProperties.get("mapReduce.IDFileName");
            if (joinBitsProperty != null) {
                Vector<ChordID> chordIDs = getBalancedChordIDs(joinBitsProperty);
                if (chordIDs == null) {
                    peer = controlInterface.startAsBootstrap(getDrtsControlClass(), nodeDescriptors, localNode, 300000);
                } else {
                    peer = controlInterface.startAsBootstrap(getDrtsControlClass(), nodeDescriptors, localNode, chordIDs, 300000);
                }
            } else {
                peer = controlInterface.startAsBootstrap(getDrtsControlClass(), nodeDescriptors, localNode, 300000, bootStrapNodes);
                
            }
        } catch (InstantiationException ie) {
            //System.out.println("Meteor init failed");
            FedLog.writeLog("manageLog","manageLog%g.log","Meteor init failed","severe");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ie),"severe");
            //Debug.printStackTrace(ie);
        }
    }

    public Vector<ChordID> getBalancedChordIDs(String joinBitsProperty) {
        Vector<ChordID> chordIDs = new Vector<ChordID>();

        int numTasks = 0;
        String strNumMapTasks = baseProperties.get("NumMapTasks");
        if (strNumMapTasks == null) {
            String fileName = baseProperties.get("InputDataFile");
            File f = new File(fileName);
            numTasks = f.listFiles().length;
        } else {
            numTasks = Integer.parseInt(strNumMapTasks);
        }

        BigInteger[] idMapping = new BigInteger[numTasks + 1];
        try {
            fillIDMapping(idMapping, joinBitsProperty);
            //                    int nodeJoinBits = Integer.parseInt(joinBitsProperty);
            int ringBits = Integer.parseInt(baseProperties.get("chord.ID_BITS"));
            //                    SimulationHelper simulationHelper = new SimulationHelper();
            BigInteger[] nodeIDs = new BigInteger[nodeList.size()];
            int idRange = numTasks / nodeList.size();
            int idIndex = idRange;
            for (int i = 0; i < nodeIDs.length; i++) {
                nodeIDs[i] = idMapping[idIndex];
                idIndex += idRange;
            }
            java.util.Arrays.sort(nodeIDs);
            for (int i = 0; i < nodeIDs.length; i++) {
                chordIDs.add(new ChordID(nodeIDs[i], ringBits));
            }
            //System.out.println("chordIDs=" + chordIDs);
            FedLog.writeLog("manageLog","manageLog%g.log","chordIDs = " + chordIDs,"info");
        } catch (IOException ioe) {
            //System.out.println("Could not load ID mapping file; starting with random IDs: " + ioe.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log","Could not load ID mapping file; starting with random IDs: " + ioe.getMessage(),"severe");    
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ioe),"severe");
            return null;
        }
        return chordIDs;
    }

    public static boolean checkRequestHandler(String[] requestHandlerList) {
        boolean inReqHand = false;

        try {
            String localIPaddr = InetAddress.getLocalHost().getHostAddress();
            for (String line : requestHandlerList) {
                String lineIPaddr = InetAddress.getByName(line).getHostAddress();
                //check local host is in the list
                if (localIPaddr.equals(lineIPaddr) == true) {
                    inReqHand = true;
                    break;
                }
            }
        } catch (IOException ex) {
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }

        return inReqHand;
    }

    public void startApp() {
        //if some nodes fail to join the overlay, ignore them (not desirable)
        //nodeList = controlInterface.getRemoteNodeURIList(); //refresh nodeList

        //local node starts first
        peer.ConfigureApp();
        
        //cloud scheduler starts when
        //this is a master described in meteor.properties as a scheduler
        try {
            if (ismaster == true
                && baseProperties.get("Scheduler") != null
                && baseProperties.get("Scheduler").equals(peer.getLocalPeerID().replace("//", ""))) {
                //Debug.println("[MeteorStarter] SchedulerClass instantiated", "MeteorStarter");
                FedLog.writeLog("manageLog","manageLog%g.log","[MeteorStarter] SchedulerClass instantiated","severe");
                Class schedulerClass = Class.forName(baseProperties.get("SchedulerClass"));
                CloudBridgingScheduler sched = (CloudBridgingScheduler) schedulerClass.newInstance();
                new Thread(sched).start();
            }
        } catch (ClassNotFoundException ex) {
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (InstantiationException ex) {
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (IllegalAccessException ex) {
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }

        //remote nodes start
        if (nodeList != null) {
            for (String node : nodeList) {
                try {
                    if (!node.equals(localNode)) {
                        controlInterface.remoteControl(node, "ConfigureApp");
                    }
                } catch (IncompleteResultException ire) {
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ire),"severe");
                } catch (NetworkException ne) {
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ne),"severe");
                }
            }
        }
    }

    public void remoteRunMethod(String methodName, Vector<String> remoteNodeList, Serializable... parameters) {
        if (remoteNodeList != null) {
            for (String node : remoteNodeList) {
                try {
                    controlInterface.remoteControl(node, methodName, parameters);
                } catch (IncompleteResultException ire) {
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ire),"severe");
                } catch (NetworkException ne) {
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ne),"severe");
                }
            }
        }
    }

    public void fillIDMapping(BigInteger[] idMapping, String mapFileName) throws IOException {
        FileHandler mapFile = new FileHandler(mapFileName, FileHandler.HandleMode.READ);
        for (int i = 0; i < idMapping.length; i++) {
            String line = mapFile.readLine();
            if (line != null) {
                String[] entry = line.split(" ");
                idMapping[i] = new BigInteger(entry[1].trim());
            }
        }
        mapFile.close();
    }

    public FedMeteorDrtsPeer getDrtsPeer() {
        return peer;
    }

    public Vector<String> getNodeMap() {
        return nodeList;
    }

	public Hashtable<String, Hashtable<String, String>> getNodeDescriptors() {
        return nodeDescriptors;
    }

    public ServiceObject getServiceObject() {
        return new MeteorControlObject();
    }

    protected class MeteorControlObject implements ServiceObject, TerminationAwareSubscriber<MessageArrivedEvent> {

        MessagingClient serviceConnection;

        public synchronized void signalEvent(MessageArrivedEvent event) {
                try {
                    OverlayControlMessage controlMessage = new OverlayControlMessage(event.getContentBytes());
                
                    if (controlMessage.isInitMessage()) {
                        //System.out.println("Received InitMessage");
                        FedLog.writeLog("manageLog","manageLog%g.log","Received InitMessage","info"); 
                        FedLog.writeLog("manageLog","manageLog%g.log","MeteorControlObjcet-> initializing " + controlMessage.getURI(),"info");
                       
                        Hashtable<String, Hashtable<String, String>> recvDescriptors = (Hashtable<String, Hashtable<String, String>>) Serializer.deserialize(controlMessage.getItemAsByteArray(1));

                        //add bootstrap properties
                        Hashtable<String, String> bootstrapProperties = new Hashtable<String, String>(baseProperties);
                        FedLog.writeLog("manageLog","manageLog%g.log","MeteorStarter-> bootstrapProperties " + bootstrapProperties.toString(),"info"); 
                  
                        recvDescriptors.put(localNode, bootstrapProperties);

                        //start overlay
                        controlInterface.startAsBootstrap(getDrtsControlClass(), recvDescriptors, localNode, 300000);
                        FedLog.writeLog("manageLog","manageLog%g.log","MeteorControlObjcet-> NodeURIList: " + controlInterface.getNodeURIList(),"info"); 
                        FedLog.writeLog("manageLog","manageLog%g.log","MeteorControlObjcet-> RemoteNodeURIList: " + controlInterface.getRemoteNodeURIList(),"info"); 

                        //execute application
                        remoteRunMethod("ConfigureApp", new Vector<String>(recvDescriptors.keySet()));

                    } else if (controlMessage.isTerminateMessage()) {
                       // System.out.println("Received TerminateMessage");
                       // System.out.println("terminating URIs= " + controlMessage.getURI());
                        FedLog.writeLog("manageLog","manageLog%g.log","Received TerminateMessage","info"); 
                        FedLog.writeLog("manageLog","manageLog%g.log","terminating URIs= " + controlMessage.getURI(),"info"); 
                        
                        Vector<String> terminateURIs = (Vector<String>) Serializer.deserialize(controlMessage.getItemAsByteArray(1));
                        for (String terminateURI : terminateURIs) {
                            if (controlInterface.getRemoteNodeURIList().contains(terminateURI)) {
                                Debug.println("terminateURI=" + terminateURI, "MeteorControlObject");
                                controlInterface.terminateInstance(terminateURI);
                            } else {
                                //System.out.println(terminateURI + " is not part of the overlay");
                                FedLog.writeLog("manageLog","manageLog%g.log",terminateURI + " is not part of the overlay","info"); 
                            }
                        }
                    } else if (controlMessage.isURIMessage()) {
                        //System.out.println(controlMessage.getMessageItem(0));
                        FedLog.writeLog("manageLog","manageLog%g.log",controlMessage.getMessageItem(0),"info");
                        return;
                    }

                    //send reply
                    Message reply = new Message();
                    reply.setHeader(OverlayControlMessage.URI_MSG);
                    reply.addMessageItem("Done");
                    serviceConnection.send(reply.getMessageBytes(), controlMessage.getMessageItem(0));

                } catch (IOException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                } catch (ClassNotFoundException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                } catch (URISyntaxException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                } catch (InstantiationException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                } catch (IncompleteResultException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                } catch (MalformedMessageException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                } catch (NetworkException ex) {
                    //System.err.println(ex.getMessage());
                    FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
                }
            }

        public void newClient(PluggableClient client) {
            serviceConnection = (MessagingClient) client;
            serviceConnection.addListener(this);
        }

        public void noMoreEvents() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void subscriptionTerminated() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public void dynamicMeteorStart(String... propertyFiles) {
        peer = FedMeteorDrtsPeer.joinOverlay(ConfigurationFileManager.parsePropertyFiles(propertyFiles));
        startApp();
    }

    public void dynamicGroupStart(String... propertyFiles) {
        try {
            //System.out.println("Dynamic group join");
            FedLog.writeLog("manageLog","manageLog%g.log","Dynamic group join","info");

            //add bootstrap properties into nodeDescriptor
            String bootstrapPeerID = baseProperties.get("chord.LOCAL_BOOTSTRAP");
            if (bootstrapPeerID == null) {
                //System.err.println("chord.LOCAL_BOOTSTRAP should be set in chord.properties");
                FedLog.writeLog("manageLog","manageLog%g.log","chord.LOCAL_BOOTSTRAP should be set in chord.properties","severe");
                System.exit(0);
            }
            Hashtable<String, String> bootstrapProperties = new Hashtable<String, String>(baseProperties);
            Debug.println("bootstrapProperties " + bootstrapProperties.toString(), "MeteorStarter");
            nodeDescriptors.put(bootstrapPeerID, bootstrapProperties);

            //start remote nodes
            controlInterface.startAsBootstrap(getDrtsControlClass(), nodeDescriptors, bootstrapPeerID, 300000);
            //System.out.println("NodeURIList: " + controlInterface.getNodeURIList());
            //System.out.println("RemoteNodeURIList: " + controlInterface.getRemoteNodeURIList());
            FedLog.writeLog("manageLog","manageLog%g.log","NodeURIList: " + controlInterface.getNodeURIList(),"info");
            FedLog.writeLog("manageLog","manageLog%g.log","RemoteNodeURIList: " + controlInterface.getRemoteNodeURIList(),"info");
            remoteRunMethod("ConfigureApp", nodeList);

        } catch (URISyntaxException ex) {
            //System.err.println(ex.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (InstantiationException ex) {
            //System.err.println(ex.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (IncompleteResultException ex) {
            //System.err.println(ex.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
    }

    private MessagingClient getClient(){
            MessagingClient client;
            if(baseProperties.get("chord.NETWORK_CLASS") != null && baseProperties.get("chord.NETWORK_CLASS").equals("SSL")){
                    client = new SSLClient();
            }
            else{
                    client = new TCPclient();
            }
            return client;
    }

    public void dynamicGroupLeave(Vector<String> nodeURIs, String bootstrapPeerID) {
        if (nodeURIs != null) {
            try {
                //create overlay control message

		MessagingClient client = getClient();
                OverlayControlMessage terminateMessage = new OverlayControlMessage();
                terminateMessage.addMessageItem("//" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + ControlConstant.COMET_CONTROL_PORT); //set sender
                terminateMessage.addMessageItem(Serializer.serializeBytes(nodeURIs));

                //set URI with meteor control port, not using overlay port
                URI uri = new URI(bootstrapPeerID);
                String meteorControlServer = "//" + uri.getHost() + ":" + ControlConstant.COMET_CONTROL_PORT;
                //System.out.println("Sending leave message to " + meteorControlServer);
                //Logger manageLog = Logger.getLogger("manageLog");
                //manageLog.info("Sending leave message to " + meteorControlServer);
                FedLog.writeLog("manageLog","manageLog%g.log","Sending leave message to " + meteorControlServer,"info");
                client.send(terminateMessage.getMessageBytes(), meteorControlServer);

                //wait reply
                RequestVariable<Object> request = new RequestVariable<Object>();
                if (request.awaitUninterruptibly(300000, TimeUnit.MILLISECONDS)) {
                    Object ret = request.getResult();
                    if (ret != null) {
                        if (ret instanceof Exception) {
                            //System.err.print(((Exception) ret).getMessage());
                            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack((Exception)ret),"severe");
                        }
                    } else {
                        //System.out.println(ret);
                        FedLog.writeLog("manageLog","manageLog%g.log",ret.toString(),"info");
                    }
                } else {
                    //System.err.print("Incomplete Result: Timeout");
                    FedLog.writeLog("manageLog","manageLog%g.log","Incomplete Result: Timeout","info");
                }
            } catch (IOException ex) {
                //System.err.println(ex.getMessage());
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            } catch (URISyntaxException ex) {
                //System.err.println("Error in bootstrapURI " + ex.getMessage());
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            } catch (NetworkException ex) {
                //System.err.println("Error in sending leave message to bootstrap " + ex.getMessage());
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            }
        }
    }

    public void sendControlMessage(String messageType, Object item) {
        try {
            //get bootstrap
            String bootstrapPeerID;
            if (bootStrapNodes!=null){
                bootstrapPeerID=(String)bootStrapNodes.get(0);
            }else{
                bootstrapPeerID=baseProperties.get("chord.LOCAL_BOOTSTRAP");
            }
            //System.out.println("Bootstrap=" + bootstrapPeerID);
           // Logger manageLog = Logger.getLogger("manageLog");
            //manageLog.info("Bootstrap=" + bootstrapPeerID);
            FedLog.writeLog("manageLog","manageLog%g.log","Bootstrap=" + bootstrapPeerID,"info");
            if (bootstrapPeerID == null) {
                //System.err.println("chord.LOCAL_BOOTSTRAP should be set in chord.properties");
                FedLog.writeLog("manageLog","manageLog%g.log","chord.LOCAL_BOOTSTRAP should be set in chord.properties","severe");
                System.exit(0);
            }

            //create control message
            MessagingClient client = getClient();
            Message controlMessage = new Message();
            controlMessage.setHeader(messageType);
            controlMessage.addMessageItem("//" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + ControlConstant.COMET_CONTROL_PORT);   //set sender
            controlMessage.addMessageItem(Serializer.serializeBytes(nodeDescriptors));

            //set URI of meteor control server
            URI uri = new URI(bootstrapPeerID);
            String meteorControlServer = "//" + uri.getHost() + ":" + ControlConstant.COMET_CONTROL_PORT;
            //System.out.println("Sending meteor control message to " + meteorControlServer);
           // manageLog.info("Sending meteor control message to " + meteorControlServer);
            FedLog.writeLog("manageLog","manageLog%g.log","Sending meteor control message to " + meteorControlServer,"info");
            client.send(controlMessage.getMessageBytes(), meteorControlServer);

            //wait reply
            RequestVariable<Object> request = new RequestVariable<Object>();
            if (request.awaitUninterruptibly(300000, TimeUnit.MILLISECONDS)) {
                Object ret = request.getResult();
                if (ret != null) {
                    if (ret instanceof Exception) {
                       // System.err.print(((Exception) ret).getMessage());
                        FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack((Exception)ret),"severe");
                    }
                } else {
                    //System.out.println(ret);
                    FedLog.writeLog("manageLog","manageLog%g.log",ret.toString(),"info");
                }
            } else {
                //System.err.print("Incomplete Result: Timeout");
                FedLog.writeLog("manageLog","manageLog%g.log","Incomplete Result: Timeout","info");
            }
        } catch (IOException ex) {
            //System.err.println(ex.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (URISyntaxException ex) {
            //System.err.println("Error in bootstrapURI " + ex.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (NetworkException ex) {
            //System.err.println("Error in sending join message to bootstrap " + ex.getMessage());
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
    }

        
    //this method cleans the Overlay once the master finishes
    public void terminateAll(){
        
        System.out.println("Terminate Comet");
        //Logger manageLog = Logger.getLogger("manageLog");
        //manageLog.info("Terminate Comet");
        FedLog.writeLog("manageLog","manageLog%g.log","Terminate Comet","info");
        terminateComet();
       
        System.out.println("Terminate Overlay");
        //manageLog.info("Terminate Overlay");
        FedLog.writeLog("manageLog","manageLog%g.log","Terminate Comet","info");
        terminateOverlayRemote();
        
        System.out.println("End Listeners");
        //manageLog.info("End Listeners");
        FedLog.writeLog("manageLog","manageLog%g.log","End Listeners","info");
        endConnectionListeners();
    }
    
    public void terminateComet() {        
        peer.terminateInstance();        
        if(serverDaemon instanceof TCPServerDaemon){            
            ((TCPServerDaemon)serverDaemon).end();            
        }else{
            ((SSLServerDaemon)serverDaemon).end();
        }
    }

    public void terminateOverlayRemote(){
        try {
            controlInterface.terminateOverlay();
        } catch (NetworkException ex) {
            Logger.getLogger(FedMeteorStarter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void endConnectionListeners(){
        try {
            controlInterface.endConnectionListeners();
        } catch (NetworkException ex) {
            ex.printStackTrace();
            Logger.getLogger(FedMeteorStarter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
