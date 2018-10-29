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
 * MeteorDrtsPeer.java
 *
 * Created on Nov 24, 2010 11:07:01 AM
 */
package tassl.automate.application.node;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.*;
import tassl.automate.network.CMessage;
import tassl.automate.network.ControlConstant;
import tassl.automate.network.MessageType;
import tassl.automate.application.network.ServerModule;
import tassl.automate.application.network.TCPServerModule;
import tassl.automate.application.network.SSLServerModule;
import tassl.automate.application.node.isolate.MeteorRequestHandler;
import tassl.automate.comet.ChangeAppState;
import tassl.automate.comet.CometConstants;
import tassl.automate.federation.simpleapi.FedMeteorGenericMaster;
import tassl.automate.federation.simpleapi.FedMeteorGenericWorker;
import tassl.automate.meteor.Meteor;
import tassl.automate.meteor.application.MeteorMasterFramework;
import tassl.automate.meteor.application.MeteorStarter;
import tassl.automate.meteor.application.MeteorWorkerFramework;
import tassl.automate.meteor.meteorSpace.MeteorSpaceService;
import tassl.automate.network.ServerListener;
import tassl.automate.network.TCPServerListener;
import tassl.automate.network.SSLServerListener;
import tassl.automate.overlay.OverlayObject;
import tassl.automate.overlay.OverlayService;
import tassl.automate.overlay.replication.ReplicationAwareListener;
import tassl.automate.overlay.squid.SquidKey;
import tassl.automate.overlay.squid.SquidOverlayService;
import tassl.automate.programmodel.masterworker.ResultCollectingMaster;
import tassl.automate.util.FedLog;



/**
 * @author Samprita Hegde
 * created on Nov 24, 2010 11:07:01 AM
 *
 * This class is used to set the role of each peer and instantiate the peer accordingly
 *
 */
public class FedMeteorDrtsPeer implements TCPServerListener, OverlayObject  {

     // Load properties file
    static {
        String propertiesFileName = System.getProperty("MeteorPropertiesFile", "meteor.properties");
        Properties p = new Properties(System.getProperties());
        try {
            p.load(new FileInputStream(propertiesFileName));
            System.setProperties(p);
        }
        catch (FileNotFoundException fnf) {
            //System.out.println("No Meteor properties file");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(fnf),"severe");
        }
        catch (IOException ioe) {
            //System.out.println("Bad Meteor properties file");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ioe),"severe");
        }

        CometConstants.enableDebugMessage("meteor.DEBUG");
    }
    
    private ServerModule control_server;
    private Meteor peerTupleSpace = null;
    private String localPeerID = null;
    private OverlayService<SquidKey> peerOverlay = null;
    private int masterId,  workerId, reqHandlerId, wfManagerId, agentId;
    private MeteorMasterFramework master = null;
    private MeteorWorkerFramework worker = null;
    private MeteorRequestHandler requestHandler = null;

    protected Hashtable<String, String> peerProperties = null;
    
    private boolean APPEND = true;

    /** Creates a new instance of DrtsPeer */
    public FedMeteorDrtsPeer(byte [] nodeFile, Hashtable<String, String> propertyTable, ServerModule myServerModule) {
        control_server = myServerModule;
        peerProperties = propertyTable;
        try {
            peerOverlay = new SquidOverlayService(nodeFile, peerProperties);
            init(CometConstants.globalSpacename);
        }
        catch (IOException iox) {
            //iox.printStackTrace();
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(iox),"severe");
        }
    }

    /** Creates a new instance of DrtsPeer. It request to join the overlay instead of assuming it is already joined */
    public FedMeteorDrtsPeer(Hashtable<String, String> propertyTable, ServerModule myServerModule) {
        control_server = myServerModule;
        peerProperties = propertyTable;        
        peerOverlay = new SquidOverlayService(peerProperties);
        init(CometConstants.globalSpacename);
        
    }
    
    /**
     * Constructor forth Meteor Drts Peer
     * @param myServerModule
     */

    public FedMeteorDrtsPeer(ServerModule myServerModule) {
        control_server = myServerModule;
        peerOverlay = new SquidOverlayService();
        init(CometConstants.globalSpacename);
    }
   
    public void init(String tsName) {
        try {
            String peerAddr;
            String localURI = getProperty("chord.LOCAL_URI", System.getProperty("chord.LOCAL_URI"));
            if (localURI != null) {
                peerAddr = localURI;
            } else {
                peerAddr = "//" + InetAddress.getLocalHost().getCanonicalHostName() + ":5556";
            }

            boolean enableReplica = Boolean.parseBoolean(getProperty("ReplicationEnable", "true"));

            //join overlay and set local peer id if not ResultCollectingMaster
            if (getProperty("ResultCollectingMaster", null) == null) {
                peerTupleSpace = new MeteorSpaceService(tsName, peerOverlay);
                peerTupleSpace.createspace(tsName);

                //for replication
                //add listener and set initial meteor state
                if (enableReplica) {
                    ChangeAppState appState = new ChangeAppState();
                    peerOverlay.subscribeToMessages((ReplicationAwareListener) peerTupleSpace, "meteor", appState);
                } else {
                    peerOverlay.subscribeToMessages((ReplicationAwareListener) peerTupleSpace, "meteor");
                }

                //System.out.println("Joining master with " + peerAddr);
                FedLog.writeLog("manageLog","manageLog%g.log","Joining master with " + peerAddr,"info");

                peerOverlay.join(peerAddr);
                localPeerID = peerOverlay.getLocalID().nodeURI;
                //System.out.println("DrtsPeer.init(): Join " + peerAddr + " to Bootstrap Node");
                FedLog.writeLog("manageLog","manageLog%g.log","DrtsPeer.init(): Join " + peerAddr + " to Bootstrap Node","info");

                SetPeerRoleID();
                if (this.masterId != -1)
                    startListener(localPeerID);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
        }
    }

    public void startListener(String listenerID) {
        try {
            control_server.AddListener(listenerID, this);
            control_server.start();
            //System.out.println("Peer listener starts");
            FedLog.writeLog("manageLog", "manageLog%g.log","Peer listener starts","info");
        } catch (Exception e) {
            //System.err.println(e);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
        }
    }

    public void handleMsg(CMessage item) {
		handleTCPMsg(item);
	}

    /* (non-Javadoc)
	 * @see tassl.automate.application.network.TCPServerListener#handleTCPMsg(tassl.automate.application.network.CMessage)
	 */
    @Override
    public void handleTCPMsg(CMessage item) {
        int tag = item.getTag();
        if (tag == MessageType.Appliation_control) {
            //TODO: handle control message if required
//            ConfigureApp();
            //System.out.println("Received Application_control message");
            FedLog.writeLog("manageLog", "manageLog%g.log","Received Application_control message","info");
        } else if (tag == MessageType.Task_aggr_agent) {
            peerTupleSpace.aggregateTask(item);
        }
    }

    public void ConfigureApp() {
        
        if (localPeerID == null) {
            //System.out.println("localPeerID is null");
            FedLog.writeLog("manageLog", "manageLog%g.log","localPeerID is null","info");
            
            return;
        }

        try {
            if (this.masterId != -1) { //master
                if (getProperty("ResultCollectingMaster", null) != null) {
                    //Haven't add any log here yet
                    System.out.println("Start sub master at "+localPeerID+" masterid="+this.masterId);
                    FedLog.writeLog("manageLog","manageLog%g.log","Start sub master at "+localPeerID+" masterid="+this.masterId,"severe");
                    master = (MeteorMasterFramework) new ResultCollectingMaster();
                    control_server.AddMasterModule(master);
                    master.startMaster();

                } else {
                    System.out.println("Start the master at "+localPeerID+" masterid="+this.masterId);                  
                    FedLog.writeLog(localPeerID.split(":")[1],"masterLog%g-"+localPeerID.substring(2)+".log","---------------------------------------------------------------------------","info");
                    FedLog.writeLog(localPeerID.split(":")[1],"masterLog%g-"+localPeerID.substring(2)+".log","Initiating...","info");
                    FedLog.writeLog(localPeerID.split(":")[1],"masterLog%g-"+localPeerID.substring(2)+".log","Start the master at "+localPeerID+" masterid="+this.masterId,"info");       
                    
                    Class masterclass =  Class.forName(peerProperties.get("MasterClass"));
                    master = (MeteorMasterFramework) masterclass.newInstance();  
                    master.setMeteorEnv(peerTupleSpace, this.masterId, localPeerID, peerOverlay);
                    
                    ((FedMeteorGenericMaster)master).setPeerProperties(peerProperties);
                    
                    control_server.AddMasterModule(master);
                    master.startMaster();
                }
            }
            if (this.workerId != -1) {  //worker
                System.out.println("Start the worker at "+localPeerID+" workerid="+this.workerId);
                //Logger workerLog = createLog(localPeerID.split(":")[1],"workerLog%g-"+localPeerID.substring(2) +".log");
                FedLog.writeLog(localPeerID.split(":")[1],"workerLog%g-"+localPeerID.substring(2) +".log","---------------------------------------------------------------------------","info");
                FedLog.writeLog(localPeerID.split(":")[1],"workerLog%g-"+localPeerID.substring(2) +".log","Initiating...","info");
                FedLog.writeLog(localPeerID.split(":")[1],"workerLog%g-"+localPeerID.substring(2) +".log","Start the worker at "+localPeerID+" masterid="+this.masterId,"info");
     
              //  workerLog.info("---------------------------------------------------------------------------");
              //  workerLog.info("Initiating...");    
               // workerLog.info("Start the worker at "+localPeerID+" masterid="+this.masterId);
                //close handler  
               // closeLoggerHandler(workerLog);
                
                Class workerclass = Class.forName(peerProperties.get("WorkerClass"));
                worker = (MeteorWorkerFramework) workerclass.newInstance();  
                
                ((FedMeteorGenericWorker)worker).setPeerProperties(peerProperties);
                
                worker.setMeteorEnv(peerTupleSpace, workerId, localPeerID, peerOverlay);
                worker.startWorker();
            }

            if (this.reqHandlerId != -1) { //request handler
                System.out.println("Request Handler not supported yet");
                //System.out.println("Start request handler at "+localPeerID+" reqHandlerid="+this.reqHandlerId);
                //requestHandler = new MeteorRequestHandler(this);
                //requestHandler.start();                
            }

        } catch (InstantiationException ex) {
            //Logger.getLogger(FedMeteorDrtsPeer.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (IllegalAccessException ex) {
            //Logger.getLogger(FedMeteorDrtsPeer.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (ClassNotFoundException ex) {
            //Logger.getLogger(FedMeteorDrtsPeer.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }
    }

    public void SetPeerRoleID() {
              
        
        String propMasterId = getProperty("MasterID", null);
        int mId = new Integer(propMasterId != null ? propMasterId : "-1").intValue();
        String propWorkerId = getProperty("WorkerID", null);
        int wId = new Integer(propWorkerId != null ? propWorkerId : "-1").intValue();
        String propReqHandlerId = getProperty("ReqHandlerID", null);
        int rId = new Integer(propReqHandlerId != null ? propReqHandlerId : "-1").intValue();
        String propWFManagerId = getProperty("WFManagerID", null);
        int wfId = new Integer(propWFManagerId != null ? propWFManagerId : "-1").intValue();
        String propAgentId = getProperty("AgentID", null);
        int aId = new Integer(propAgentId != null ? propAgentId : "-1").intValue();

        this.masterId = mId;
        this.workerId = wId;
        this.reqHandlerId = rId;
        this.wfManagerId = wfId;
        this.agentId = aId;

        //set id to port number (unique id)
        if (propMasterId==null && propWorkerId==null && propReqHandlerId==null && propWFManagerId==null && propAgentId==null) {
            String nodeType = getProperty("meteor.NodeType", "WORKER");
            MeteorStarter.NodeType peerRole = MeteorStarter.NodeType.valueOf(nodeType);
            //System.out.println("MeteorDrtsPeer: PerRole "+peerRole);
            FedLog.writeLog("manageLog","manageLog%g.log","MeteorDrtsPeer: PerRole "+peerRole,"info");
            
            String idStr = localPeerID.split(":")[1];
            try {
                int id = Integer.parseInt(idStr);
                switch(peerRole) {
                    case MASTER: {
                        this.masterId = id;
                    }
                    break;
                    case WORKER: {
                        this.workerId = id;
                    }
                    break;
                    case REQUEST_HANDLER: {
                        this.reqHandlerId = id;
                    }
                    break;
                    case WORKFLOW_MANAGER: {
                        this.wfManagerId = id;
                    }
                    break;
                    case CLOUD_AGENT: {
                        this.agentId = id;
                    }
                    break;
                }
            } catch (Exception e) {
                FedLog.writeLog("manageLog","manageLog%g.log","Invalid role id. "+idStr,"severe");
                FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
            }
        }

        //System.out.println("MeteorDrtsPeer.setRoleOfPeer: "+localPeerID+" meteor.NodeType="+getProperty("meteor.NodeType", "WORKER")+" MasterID="+getProperty("MasterID", null)+" WorkerID="+getProperty("WorkerID", null)+ " ReqHandlerID="+getProperty("ReqHandlerID", null));
        //System.out.println("masterid="+this.masterId+" workerid="+this.workerId+" reqhandlerid="+this.reqHandlerId+" wfManagerid="+this.wfManagerId+" agentId="+this.agentId);
        FedLog.writeLog("manageLog", "manageLog%g.log","MeteorDrtsPeer.setRoleOfPeer: "+localPeerID+" meteor.NodeType="+getProperty("meteor.NodeType", "WORKER")+" MasterID="+getProperty("MasterID", null)+" WorkerID="+getProperty("WorkerID", null)+ " ReqHandlerID="+getProperty("ReqHandlerID", null),"info");
        FedLog.writeLog("manageLog", "manageLog%g.log","masterid="+this.masterId+" workerid="+this.workerId+" reqhandlerid="+this.reqHandlerId+" wfManagerid="+this.wfManagerId+" agentId="+this.agentId,"info");
        FedLog.writeLog("manageLog", "manageLog%g.log","---------------------------------------------------------------------------","info");
    }

    public String getLocalPeerID() {
        return localPeerID;
    }

    public Meteor getMeteorSpace() {
        return peerTupleSpace;
    }

    public OverlayService<SquidKey> getOverlay() {
        return peerOverlay;
    }

    public void terminateInstance() {
        //System.out.println("DrtsPeer terminateInstance "+peerOverlay.getLocalID());
        FedLog.writeLog("manageLog", "manageLog%g.log","DrtsPeer terminateInstance "+peerOverlay.getLocalID(),"info");
        
        if (this.workerId!=-1)
            this.worker.quit();
        else if (this.masterId!=-1)
            this.master.quit();
        else if (this.reqHandlerId!=-1){
            //System.out.println("Request Handler quit");
            //manageLog.info("Request Handler quit");
            FedLog.writeLog("manageLog", "manageLog%g.log","Request Handler quit","info");
            requestHandler.quit();
        }
        try{
            peerOverlay.leave();  //leave nicely
            //peerOverlay.terminate(); //terminates without worrying about rerouting.            
        }catch(java.lang.RuntimeException e){
            //System.out.println("The peer already left the overlay.");
            FedLog.writeLog("manageLog","manageLog%g.log","The peer already left the overlay.","severe");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
        }
        
        try{
            //System.out.println("Quiting TCPServerModule");
            FedLog.writeLog("manageLog", "manageLog%g.log","Quiting TCPServerModule","info");
            quitServer();
        }catch(java.lang.NullPointerException e){
            //System.out.println("The TCPServerModule already quit.");    
            FedLog.writeLog("manageLog","manageLog%g.log","The TCPServerModule already quit.","severe");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(e),"severe");
        }
        
    }

    private String getProperty(String propertyName, String defaultValue) {
        String ret = null;
        if (this.peerProperties != null) {
            ret = this.peerProperties.get(propertyName);
        }
        if (ret == null) {
            ret = System.getProperty(propertyName);
        }
        if (ret == null) {
            ret = defaultValue;
        }
        return ret;
    }

    public MeteorMasterFramework getMaster() {
        return master;
    }
    
    public MeteorWorkerFramework getWorker() {
        return worker;
    }

    public ServerModule getTcp_control_server() {
        return control_server;
    }

    public void setTcp_control_server(ServerModule controlServer) {
        control_server = controlServer;
    }

    public void quitServer() {
        control_server.quit();
    }

    public Hashtable<String, String> getPeerProperties() {
        return peerProperties;
    }

    public static FedMeteorDrtsPeer joinOverlay(Hashtable<String, String> propertyTable) {
        if (propertyTable.get("chord.LOCAL_BOOTSTRAP")==null) {
            //System.err.println("chord.LOCAL_BOOTSTRAP should be set in chord.properties");
            FedLog.writeLog("manageLog","manageLog%g.log","chord.LOCAL_BOOTSTRAP should be set in chord.properties","severe");
            System.exit(1);
        }
            
        //System.out.println("chord.LOCAL_BOOTSTRAP="+propertyTable.get("chord.LOCAL_BOOTSTRAP"));
        FedLog.writeLog("manageLog", "manageLog%g.log","chord.LOCAL_BOOTSTRAP="+propertyTable.get("chord.LOCAL_BOOTSTRAP"),"info");
        FedMeteorDrtsPeer peer = null;

        ServerModule serverModule;
        if(propertyTable.get("chord.NETWORK_CLASS").equals("SSL")){
                serverModule = new SSLServerModule(ControlConstant.TCP_CONTROL_PORT);
                //System.out.println("joinOverlay: Creating SSLServerModule");                
                FedLog.writeLog("manageLog", "manageLog%g.log","joinOverlay: Creating SSLServerModule","info");
                
        }else{
                serverModule = new TCPServerModule(ControlConstant.TCP_CONTROL_PORT);
                //System.out.println("joinOverlay: Creating TCPServerModule");
                FedLog.writeLog("manageLog", "manageLog%g.log","joinOverlay: Creating TCPServerModule","info");
        }

        peer = new FedMeteorDrtsPeer(serverModule);
        peer.peerProperties = propertyTable;
        return peer;
    }
    /**
     * @author Mengsong Zou
     * @param myLog 
     * 
     * This function close all the handlers of a logger
     */
    public void closeLoggerHandler(Logger myLog){
        for(Handler handler:myLog.getParent().getHandlers())
        {
                  handler.close();
        }
    }

}