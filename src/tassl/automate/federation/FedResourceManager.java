/*
 * Copyright (c) 2009, NSF Center for Autonomic Computing, Rutgers University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided 
 * that the following conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and 
 * the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
 *  the following disclaimer in the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the NSF Center for Autonomic Computing, Rutgers University, nor the names of its 
 *  contributors may be used to endorse or promote products derived from this software without specific prior 
 *  written permission.
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
 *  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY 
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 *  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 *  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *  POSSIBILITY OF SUCH DAMAGE.
 *  
 */
package tassl.automate.federation;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;
import programming5.io.ArgHandler;
import programming5.io.FileHandler;
import tassl.automate.comet.XmlTuple;
import tassl.automate.federation.server.EventServer;
import tassl.automate.util.FedLog;
/**
 *
 * @author Ioan Petri
 */
public class FedResourceManager {
    
    HashMap <String, HashMap<String,Object>> peers= new HashMap();
    //ports, and used status(free, used, notusable)        
    HashMap <String,String> ports = new HashMap();
    List bootstrapnodes=null;
    String publisherport;
    String machinePublicName="";
    String infoServicePort;
    boolean APPEND = true; 
    String globalTempPort="";
    private ServerSocket infoServiceSocket = null;
    private Socket cSocket = null;
    private List tempBootstrapnode;
    private String usedBootstrapnode;
    
    private String infoServiceMsg = "";
    FedInfoSystem infoService;
    
    private List reqNodes;
    private List singleCptNodes;
    private List associateCptNodes;
    
    
    private final Object lock = new Object();
    
    public void startManager(String[] args) {
        System.out.println("WEBSOCKET STARTED!!!");
        EventServer call = new EventServer();
        //manageLog = createLog("manageLog","manageLog%g.log");
        //manageLog.info("Initiating...");
        FedLog.writeLog("manageLog","manageLog%g.log","Initiating...","info");
                
        ArgHandler argHandler = new ArgHandler(args);
        Random randomGenerator = new Random();
        List contentTemp=new ArrayList();        
        //ALL the instances will be in localhost. We create a master and then a worker per line in the subscriptionFile file.
        
        //this is needed by TCPHandler (processResultSubmit()), because we do not have MasterID and we need the master to get it. 
        // it should be fine because only the master start the TCPServerModuler that will get the messages from the workers
        // in setResult.
        System.setProperty("FEDERATION","1");
        
        try{
            machinePublicName=argHandler.getStringArg("-managerPublicName");
        }catch(java.lang.IllegalArgumentException e){
            try {
                machinePublicName=InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException ex) {
                machinePublicName="localhost";
                //Logger.getLogger(FedResourceManager.class.getName()).log(Level.SEVERE, null, ex);
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            }
        }
        
        // Read Archivo del boostrapnodes. Boostrapnodes will be null if the file is missing.
        try{
            String bootstrapnodesFile=argHandler.getStringArg("-bootstrapnodesFile");
            addBootStrapNodes(bootstrapnodesFile);
        }catch (java.lang.IllegalArgumentException e){
            //System.out.println("No -bootstrapnodesFile argument found. We will use the default bootstrapnode");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
        }
        
        // Read infoServicePort number
        try{
            infoServicePort = argHandler.getStringArg("-infoServicePort");            
        }catch (java.lang.IllegalArgumentException e){
            infoServicePort = "5561";
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
        }
        System.setProperty("infoServicePort",infoServicePort);
        
        
        //end add for paper
        
        //read ports. We keep a  list of ports used
        String portFile = argHandler.getStringArg("-portFile");        
        addPorts(portFile);
        
        //create Publisher
        createPublisher(args,randomGenerator);
        
        infoServiceMsg = infoServiceMsg.concat(machinePublicName+":"+ publisherport+",publisher");   
        tempBootstrapnode = bootstrapnodes;
        //update boostrapnodes so workers join localmaster
        if(bootstrapnodes==null){
            bootstrapnodes=new ArrayList();
            bootstrapnodes.add(machinePublicName+":"+publisherport);
        }else{
            List temp=new ArrayList();
            temp.add(machinePublicName+":"+publisherport);
            temp.addAll(bootstrapnodes);
            bootstrapnodes=temp;
        }
                
        //Create Subscribers(workers)
        reqNodes = new ArrayList<String>();
        singleCptNodes = new ArrayList<String>();
        associateCptNodes = new ArrayList<String>();
        
        //read the subscriptions that we want. It will start a worker per subscription.
        String subscriptions = argHandler.getStringArg("-subscriptionFile");
        FileHandler subscriptionsHandler;          
        try {
            subscriptionsHandler = new FileHandler(subscriptions, FileHandler.HandleMode.READ);        
            String subtemp=subscriptionsHandler.readLine();
                        
            while(subtemp!=null){                                 
                //crear subscribers (workers) que se unen al publisher (master). 
                createSubscriberGeneric(args, randomGenerator,subtemp);
                
                subtemp=subscriptionsHandler.readLine();  
            }
            subscriptionsHandler.close();
        } catch (IOException ex) {
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
        
        infoService = new FedInfoSystem();   
        infoService.updateInfo(infoServiceMsg);
        infoService.setSiteIP(machinePublicName);
        
        //infoService.loadConfig();        
        //System.out.println("infoService.getSiteIP(): "+infoService.getSiteIP());
        
        //if current site is not boostrapnode(tempBootstrapnode.get(0)) and bootstrapnode is up, send initial message with local join 
        //information to bootstrapnode and receive infoServiceList from bootstrapnode, then use it to update local infoServiceList
        if (tempBootstrapnode != null && tempBootstrapnode.size() > 0){
            if(!isBootstrapNode(tempBootstrapnode.get(0)) && bootstrapnodeIsUp() ){

                    String infoServiceList = getInfoServiceList("UPDATE-"+infoServiceMsg);
                    //System.out.println("infoServiceMsg: "+infoServiceList);
                    infoService.updateInfo(infoServiceList);
                    //send subscription info to every other node in the network
                    sendMsgToAll("UPDATE-"+infoServiceMsg);
            }
            //bootstrapnode exist but not up, and it's not localhost
            else if(!isBootstrapNode(tempBootstrapnode.get(0)) && !bootstrapnodeIsUp()){
                System.out.println("bootstrapnode is not up!Add current node as bootstrap!");
            }
            else if(isBootstrapNode(tempBootstrapnode.get(0)))
                System.out.println("Current node is bootstrapnode!!");
                
        }else{
            //if no bootstrapnode is defined in bootstrap file
            System.out.println("bootstrapnode file is empty!");   
            tempBootstrapnode = bootstrapnodes;
        }
        
        //create server thread to receive new site's infoService request  
        try {
            infoServiceSocket = new ServerSocket(Integer.parseInt(infoServicePort));
            infoServiceSocket.setReuseAddress(true);
        } catch (IOException ex) {
            FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
        }
        
        boolean FLAG = true;
        while (FLAG) {
            try {
                cSocket = infoServiceSocket.accept();

                Thread infoRequest = new Thread(new processInfoRequest(cSocket,args));
                infoRequest.start();
            } catch (IOException ex) {
                FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
            }
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
        }

    }
    
    public boolean createSubscriberGeneric(String[] args, Random randomGenerator, String subtemp){
        globalTempPort="";
        if (subtemp.split(",")[0].equals("REQ")) {
            subtemp = subtemp.replaceFirst("REQ", "CPT");

            if (createSubscriber(args, randomGenerator, subtemp) != true) {
                return false;
            }
            //associateCptNodes.add(globalTempPort+":"+subtemp);
            //infoServiceMsg = infoServiceMsg.concat(":"+globalTempPort+",subscriber,"+subtemp);
            subtemp = subtemp.replaceFirst("CPT", "REQ");
            subtemp = subtemp.replaceFirst(",", "," + globalTempPort + ",");
        }
        
        //replace bid with cpt in subtemp
        //create compute subcriber
        //modify subtemp to include port as second "bid,port,red..."
        //create bid.                 
        String[] temp = subtemp.split(",");
        if (subtemp.split(",")[0].equals("REQ")) {
            if (createSubscriber(args, randomGenerator, subtemp) != true) {
                return false;
            }
            infoServiceMsg = infoServiceMsg.concat(":" + globalTempPort + ",subscriber," + temp[0] + "," + temp[2] + "," + temp[1]);
            //myPort:cpt,red:associatePort
            //reqNodes.add(globalTempPort+":"+temp[0]+","+temp[2]+":"+temp[1]);
        } else {
            //create CPT subscriber with "SINGLE" flag. CPT subscriber without "SINGLE" would be associated to REQ node
            if (createSubscriber(args, randomGenerator, subtemp + ",SINGLE") != true) {
                return false;
            }
            infoServiceMsg = infoServiceMsg.concat(":" + globalTempPort + ",subscriber," + subtemp);
            //singleCptNodes.add(globalTempPort+":"+subtemp);
        }
        return true;
    }
    
    public boolean createSubscriber(String[] args, Random randomGenerator, String subscription){
        List contentTemp=new ArrayList(); 
        //copy args to dynamic list
        List <String>tempargsList=new ArrayList();
        tempargsList.addAll(Arrays.asList(args));
        
        //find available port
        String port=findFreePort(ports);
        globalTempPort=port;
        if (port.equals("-1")){
            //System.err.println("ERROR: there is not available ports.");
            FedLog.writeLog("manageLog","manageLog%g.log","ERROR: there is not available ports.","severe");
            return false;
        }
        
        tempargsList=removeProperty(tempargsList,"-exceptionFile");
        //create subscription file. This file has a single subscription that will be read by the worker
        contentTemp.clear();
        contentTemp.add("subscription="+subscription.trim());        
        String subFileTemp1 = createTempFile(".subscriptionFile",contentTemp, randomGenerator);        
        tempargsList=setProperty(tempargsList,"-propertyFile",subFileTemp1); 
        //create nodeFile
        contentTemp.clear();
        contentTemp.add(machinePublicName+":1");        
        String nodeFileTemp1 = createTempFile(".nodeFile",contentTemp, randomGenerator);        
        tempargsList=setProperty(tempargsList,"-nodeFile",nodeFileTemp1);       

        // crear un portfile
        contentTemp.clear();
        contentTemp.add(port);
        String portFileTemp1 = createTempFile(".portFile",contentTemp, randomGenerator);
        tempargsList=setProperty(tempargsList,"-portFile",portFileTemp1);           
        //set port to used
        ports.put(port, "used");
        
        FedLog.writeLog("manageLog","manageLog%g.log","Starting Worker: "+machinePublicName+": "+port,"info");

        //start app
        FedAppStarter subscriber= new FedAppStarter(tempargsList.toArray(new String[]{}),nodeFileTemp1,portFileTemp1,bootstrapnodes);       
        
        //store Info
        HashMap <String,Object> tempHash=new HashMap();
        tempHash.put("type", "Subscriber");
        tempHash.put("portFile", portFileTemp1);
        tempHash.put("nodeFile", nodeFileTemp1);                       
        tempHash.put("appStarter", subscriber);
        peers.put(port, tempHash);
        //subscriber.getFedMeteorStarter().getDrtsPeer();
        
        return true;
    }
    public void createPublisher(String[] args, Random randomGenerator){
        List contentTemp=new ArrayList(); 
        //copy args to dynamic list
        List <String>argsList=new ArrayList();
        argsList.addAll(Arrays.asList(args));        
        
        publisherport=findFreePort(ports);
        if (publisherport.equals("-1")){
            //System.err.println("ERROR: there is not ports available. Please add more ports.");
            FedLog.writeLog("manageLog","manageLog%g.log","ERROR: there is not ports available. Please add more ports.","severe");
            System.exit(-1);
        }
        //create exceptionFile for the master in localhost:selected port.
        contentTemp.clear();
        contentTemp.add(machinePublicName+":"+publisherport);
        contentTemp.add("meteor.NodeType=MASTER");
        String exceptionFile = createTempFile(".exceptionFile",contentTemp, randomGenerator);        
        argsList=setProperty(argsList,"-exceptionFile",exceptionFile);       
        
        //create nodeFile
        contentTemp.clear();
        contentTemp.add(machinePublicName+":1");        
        String nodeFileTemp = createTempFile(".nodeFile",contentTemp, randomGenerator);        
        argsList=setProperty(argsList,"-nodeFile",nodeFileTemp);       
        
        // crear un portfile
        contentTemp.clear();
        contentTemp.add(publisherport);
        String portFileTemp= createTempFile(".portFile",contentTemp, randomGenerator);
        argsList=setProperty(argsList,"-portFile",portFileTemp);               
        //set port to used
        ports.put(publisherport, "used");
        //start app
        //System.out.println("Starting Publisher");
        FedLog.writeLog("manageLog","manageLog%g.log","Starting Publisher: "+machinePublicName+":"+publisherport,"info");

        
        FedAppStarter publisher = new FedAppStarter(argsList.toArray(new String[]{}),nodeFileTemp,portFileTemp,bootstrapnodes);
        
        //set info service port
        ((FedMeteorPublisherMaster)publisher.getDrtsPeerMaster()).setInfoServicePort(infoServicePort);

        //store Info
        HashMap <String,Object> tempHash=new HashMap();
        tempHash.put("type", "Publisher");
        tempHash.put("portFile", portFileTemp);
        tempHash.put("nodeFile", nodeFileTemp);
        tempHash.put("exceptionFile", exceptionFile);        
        tempHash.put("appStarter", publisher);
        peers.put(publisherport, tempHash);
        //Done with publisher(location.propertiesmaster)
    }
    public void addPorts(String portFile){
        FileHandler portFileHandler;        
        try {
            portFileHandler = new FileHandler(portFile, FileHandler.HandleMode.READ);  
            String porttemp=portFileHandler.readLine();
            while (porttemp != null) {
                if(!ports.containsKey(porttemp.trim()))
                    ports.put(porttemp.trim(),"free");
                porttemp=portFileHandler.readLine();
            }
            portFileHandler.close();
        } catch (IOException ex) {
            //Logger.getLogger(FedResourceManager.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
    }
    public void addBootStrapNodes(String bootstrapnodesFile){
        FileHandler fileHandler;
        if (bootstrapnodesFile!=null){
            if (bootstrapnodes==null)
                bootstrapnodes=new ArrayList();
            
            try {
                fileHandler = new FileHandler(bootstrapnodesFile, FileHandler.HandleMode.READ);
                //create exception file with the master
                String temp=fileHandler.readLine();
                while (temp != null) {
                    bootstrapnodes.add(temp.trim());
                    temp=fileHandler.readLine();
                }   
                fileHandler.close();                
            } catch (IOException ex) {
                //Logger.getLogger(FedResourceManager.class.getName()).log(Level.SEVERE, null, ex);
               FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            }
        }
    }
    /**
     * This method compare current node with the BootstrapNode.
     * @param obj BootstrapNode
     * @return 
     */
    public boolean isBootstrapNode(Object obj){
        return obj.toString().equals(machinePublicName+":"+ publisherport);
    }
    /**
     * This method check if there is any working bootstrapnode.
     * @return true if there is at least one bootstrapnode is up, false otherwise
     */
    public boolean bootstrapnodeIsUp(){
        for(int i = 0; i < tempBootstrapnode.size(); ++i){
            String[] tempNode = tempBootstrapnode.get(i).toString().split(":");
            //System.out.println(tempNode[0]+":"+tempNode[1]);
            if(bootstrapnodeIsUp(tempNode))    
                return true;
        }
        return false;
    }
    
    public boolean bootstrapnodeIsUp(String[] tempNode){
        InetAddress destAddr = null;
        try {
            destAddr = InetAddress.getByName(tempNode[0]);
        } catch (UnknownHostException ex) {
            FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
        }
        int destPort = Integer.parseInt(infoServicePort);
        try {
            Socket sock = new Socket(destAddr, destPort);
            sock.close();
            return true;
        } catch (IOException ex) {
            FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
        }
        return false;
    }
    //find all nodes in the network and send local subscription list to these nodes(not to bootstrapnodes)   
    public void sendMsgToAll(String msg){
        ConcurrentHashMap<String, ConcurrentHashMap<String,String>> list;
    	synchronized (lock) {
            list = infoService.getSiteList();
        }
        Iterator it = list.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next(); 
            String destIP = entry.getKey().toString();
            //only send newjoin msg to those that are not bootstrapnode because bootstrapnode already had the info
            //if(!destIP.equals(usedBootstrapnode) && !destIP.equals(machinePublicName)) 
            if(!destIP.equals(machinePublicName)) 
                sendMsg(destIP,infoServicePort,msg);
        }
    }
    
    public void sendMsg(String _destIP, String _destPort, String msg){
        InetAddress destAddr = null;
        try {
            destAddr = InetAddress.getByName(_destIP);
        } catch (UnknownHostException ex) {
            FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
        }
        int destPort = Integer.parseInt(_destPort);

        DataOutputStream output;

        try {
            Socket sock = new Socket(destAddr, destPort);
            output = new DataOutputStream(sock.getOutputStream());
            output.writeUTF(msg);
            output.flush();
            output.close();
            sock.close();
        } catch (IOException ex) {
            FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
        }
    }
    
    public String createTempFile(String name, List content, Random randomGenerator){
        String tempFile= name+"."+randomGenerator.nextInt(999999999);
        FileHandler fileHandler;
        try {
            fileHandler = new FileHandler(tempFile, FileHandler.HandleMode.OVERWRITE);
            Iterator contentIter=content.iterator();
            while(contentIter.hasNext()) {
                fileHandler.write((String)contentIter.next()+(contentIter.hasNext()?"\n":""));
            }            
            fileHandler.close();
        } catch (IOException ex) {
            //Logger.getLogger(FedResourceManager.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
        return tempFile;
    }
    
    public List removeProperty(List argsList, String tag){
        int index=argsList.indexOf(tag);
        if (index != -1){
            if (index+1 < argsList.size()){
                if (!((String)argsList.get(index+1)).startsWith("-")) {
                    argsList.remove(index+1);
                    argsList.remove(index);                    
                }else{
                    argsList.remove(index);                        
                }
            }else{                
                argsList.remove(index);
            }            
        }
        return argsList;
    }
    
    public List setProperty(List argsList, String tag, String file){
        if (tag.equals("-propertyFile")){
            argsList.add(tag);
            argsList.add(file);
        }else{
            int index=argsList.indexOf(tag);
            if (index != -1){
                if (index+1 < argsList.size()){
                    if (!((String)argsList.get(index+1)).startsWith("-")) {
                        argsList.set(index+1,file);
                    }else{
                        argsList.remove(index);
                        argsList.add(tag);
                        argsList.add(file);
                    }
                }else{                
                    argsList.add(file);
                }            
            }else{
                argsList.add(tag);
                argsList.add(file);
            }
        }
        return argsList;
    }
    
    public String findFreePort(HashMap <String,String> ports){
        for(String port:ports.keySet()){
            if (ports.get(port).equals("free")){
                return port;
            }
        }
        return "-1";
        
    }  

    public String getInfoServiceList(String msg){
        String infoServiceList ="";
        InetAddress destAddr = null;
        String[] nodeInfo = null;
        boolean flag = false;
        //get one(first) available bootstrapnode 
        for(int i = 0; i < tempBootstrapnode.size(); ++i){
            nodeInfo = tempBootstrapnode.get(i).toString().split(":");
            if(bootstrapnodeIsUp(nodeInfo)){    
                usedBootstrapnode = nodeInfo[0];
                flag = true;
                break;    
            }
        }
        if(!flag){
            FedLog.writeLog("manageLog","manageLog%g.log","No bootstrapnode is up!","info");
        }
        else {
            //String[] nodeInfo = tempBootstrapnode.get(0).toString().split(":");
            try {
                destAddr = InetAddress.getByName(usedBootstrapnode);
            } catch (UnknownHostException ex) {
                FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
            }
            int destPort = Integer.parseInt(infoServicePort);

            DataOutputStream output;
            DataInputStream input;
            try {
                Socket sock = new Socket(destAddr, destPort);
                output = new DataOutputStream(sock.getOutputStream());
                input = new DataInputStream(sock.getInputStream());

                output.writeUTF(msg);
                output.flush();

                infoServiceList = input.readUTF();

                output.close();
                input.close();
                sock.close();
            } catch (IOException ex) {
                FedLog.writeLog("manageLog", "manageLog%g.log", FedLog.getExceptionStack(ex), "severe");
            }
        }
        return infoServiceList;
    }
    
    public class processInfoRequest implements Runnable{
        Socket cSocket;
        DataOutputStream output;
        DataInputStream input;
        String[] args;
        
        public processInfoRequest(Socket clientSocket, String[] _args){
            cSocket = clientSocket;
            args = _args;
        }
        
        @Override
        public void run(){
            String message = "";
            Random randomGenerator = new Random();
            
            try {
                input = new DataInputStream(cSocket.getInputStream());
                output = new DataOutputStream(cSocket.getOutputStream());
                if (input.available() != 0) {
                    message = input.readUTF();
                    if (!message.isEmpty()) {
                        //System.out.println("Receive InfoServiceMessage: "+message);
                        //POISON message to remove left node from infoService list
                        if (message.split("-")[0].equals("POISON")) {
                            String removeMsg = "";
                            FedAppStarter publisher = (FedAppStarter) peers.get(publisherport).get("appStarter");
                            //if the poison message is from local site with a LOCAL tag
                            if (message.split("-")[1].equals("LOCAL")) {
                                String param = message.split("-")[2];
                                String _destIP = param.split(",")[0];
                                String _destPort = param.split(",")[1];
                                removeMsg = _destIP + "," + _destPort;
                                //send POISON message to all nodes
                                sendMsgToAll("POISON-" + removeMsg);
                                //remove the node from local list, destPort could be multiple ports 
                                //following the structure: cptPort:bidPort(associate two)
                                synchronized (lock) {
                                    infoService.removeInfo(_destIP + "," + _destPort);
                                }

                                String[] destPorts = _destPort.split(":");
                                for (int i = 0; i < destPorts.length; ++i) {
                                    //get subscriber object
                                    FedAppStarter subscriber = (FedAppStarter) peers.get(destPorts[i]).get("appStarter");

                                    //directly ask subscriber to leave overlay
                                    ((FedMeteorSubscriberWorker) subscriber.getDrtsPeerWorker()).workerLeave(machinePublicName);
                                    //set the used port back to free status
                                    ports.put(destPorts[i],"free");
                                }
                            } else {
                                removeMsg = message.split("-")[1];
                                synchronized (lock) {
                                    infoService.removeInfo(message.split("-")[1]);
                                }
                            }
                            //update local master's information service list 
                            ((FedMeteorPublisherMaster) publisher.getDrtsPeerMaster()).getInfoSys().removeInfo(removeMsg);
                            //System.out.println("FedResourceManager: [master] after POISON  "+ ((FedMeteorPublisherMaster) publisher.getDrtsPeerMaster()).getInfoSys().getSiteList()); 
                            FedLog.writeLog("manageLog", "manageLog%g.log", "Remove subscriber: " + removeMsg, "info");
                            FedLog.writeLog("manageLog", "manageLog%g.log", "---------------------------------------------------------------------------", "info");
                        } else if (message.split("-")[0].equals("ADD")) {
                            String tempInfoServiceMsg = "";
                            String param = message.split("-")[2];
                            String _tupleTag = param.split(",")[0];
                            String _taskType = param.split(",")[1];
                            int addNum = Integer.parseInt(param.split(",")[2]);

                            while (addNum > 0) {
                                synchronized (lock) {
                                    infoServiceMsg = machinePublicName;

                                    if (createSubscriberGeneric(args, randomGenerator, _tupleTag + "," + _taskType) != true) {
                                        FedLog.writeLog("manageLog", "manageLog%g.log", "Create new subscriber failed!", "info");
                                    }
                                    infoService.addInfo(infoServiceMsg, machinePublicName, globalTempPort);
                                    tempInfoServiceMsg = tempInfoServiceMsg.concat(infoServiceMsg);
                                }
                                addNum--;
                            }
                            tempInfoServiceMsg = tempInfoServiceMsg.concat(";");
                            //send information service list to all nodes after new addition
                            sendMsgToAll("UPDATE-" + tempInfoServiceMsg);
                            //give feedback to publisher
                            synchronized (lock) {
                                output.writeUTF(tempInfoServiceMsg.toString());
                                output.flush();
                            }
                        } else if (message.split("-")[0].equals("UPDATE")) {
                            //if current site is bootstrapnode,send back entire infoService list back to new join site
                            if (tempBootstrapnode != null && tempBootstrapnode.size() > 0) {
                                if (isBootstrapNode(tempBootstrapnode.get(0)) || message.split("-")[1].equals("LOCAL")) {
                                    //System.out.println("Send back msg:" + infoService.toString());
                                    synchronized (lock) {
                                        output.writeUTF(infoService.toString());
                                        output.flush();
                                    }
                                }
                            }
                            //extract infoService info from message sent by new join site and add that to current infoService list
                            if (!message.split("-")[1].equals("LOCAL")) {
                                synchronized (lock) {
                                    infoService.updateInfo(message.split("-")[1]);
                                }
                                //System.out.println("infoServiceList"+infoService.getSiteList());
                            }
                            //System.out.println("infoServiceList after update: "+infoService.getSiteList());
                        } else if (message.split("-")[0].equals("CPT")) {
                            if (message.split("-")[1].equals("UPDATE")) {
                                output.writeUTF(singleCptNodes.toString());
                            } else if (message.split("-")[1].equals("DELETE")) {
                                singleCptNodes.remove(message.split("-")[2]);
                            }
                        } else if (message.split("-")[0].equals("REQ")) {
                            if (message.split("-")[1].equals("UPDATE")) {
                                output.writeUTF(reqNodes.toString());
                            } else if (message.split("-")[1].equals("DELETE")) {
                                reqNodes.remove(message.split("-")[2]);
                            }
                        } else if (message.split("-")[0].equals("ScenarioUpdate")) {
                            String []parts=message.split("-");
                            //PETRI
                            String numCompletedAndTime = infoService.updateLocalWorkerScoreCost(parts[1],Double.parseDouble(parts[2]),
                                    parts[3],Long.parseLong(parts[4]),Integer.parseInt(parts[5]), Integer.parseInt(parts[6]), parts[7]);
                           // String numCompletedAndTime = infoService.updateLocalWorkerC4C(parts[1],Double.parseDouble(parts[2]),
                                    //parts[3],Long.parseLong(parts[4]),Integer.parseInt(parts[5]), Integer.parseInt(parts[6]), parts[7]);
                            output.writeUTF(numCompletedAndTime);
                        } else if (message.split("-")[0].equals("ScenarioCheck")) {
                            String []parts=message.split("-");
                            //System.out.println("ScenarioCheck::receive message "+message);
                            //String status=infoService.checkIfExecute(parts[1], 
                            //        Double.parseDouble(parts[2]), parts[3], Long.parseLong(parts[4]));
                            //for paper with energyplus
                            String status=infoService.checkIfExecute(parts[1],Double.parseDouble(parts[2]),
                                    parts[3], Long.parseLong(parts[4]),Integer.parseInt(parts[5]));
                            //end for paper
                            
                            output.writeUTF(status);
                            input.readUTF();        //just to make sure the write is received by the worker
                            
                        } else if (message.split("-")[0].equals("BalanceUpdate")){  //update balance of subscriber
                            String [] parts = message.split("-");  //ip:port-appType-totalbalanceOfThisSubscriber
                            infoService.updateSubscriberBalance(parts[1], parts[2], Double.parseDouble(parts[3]));                            
                        }
                    }
                }
                input.close();
                output.close();
                cSocket.close();        
            } catch (IOException ex) {
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
            }
          
        }
    }
   
    public static void main(String[] args) {
        FedResourceManager frm=new FedResourceManager();
        frm.startManager(args);
    }
}
