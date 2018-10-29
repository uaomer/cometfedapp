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
 * MeteorAppWorker.java
 *
 * Created on Nov 26, 2010 9:39:30 PM
 */
package tassl.automate.federation.simpleapi;

import tassl.automate.meteor.application.simpleapi.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.logging.*;

import programming5.io.Debug;
import tassl.automate.network.CMessage;
import tassl.automate.network.ControlConstant;
import tassl.automate.network.MessageType;
import tassl.automate.application.network.Sender;
import tassl.automate.application.network.SendTCP;
import tassl.automate.application.network.SendSSL;
import tassl.automate.application.network.SendClient;
import tassl.automate.application.network.TCPClient;
import tassl.automate.application.network.SSLClient;
import tassl.automate.comet.CometConstants;
import tassl.automate.comet.CometSpace;
import tassl.automate.comet.XmlTuple;
import tassl.automate.comet.xmltuplespace.XmlTupleService;
import tassl.automate.meteor.Meteor;
import tassl.automate.meteor.MeteorNotifier;
import tassl.automate.meteor.application.MeteorWorkerFramework;
import tassl.automate.overlay.OverlayService;
import tassl.automate.programmodel.masterworker.WorkerFramework;
import tassl.automate.util.FedLog;


/**
 * @author Samprita Hegde
 * created on Nov 26, 2010 9:39:30 PM
 *
 */
public abstract class FedMeteorGenericWorker implements MeteorWorkerFramework, MeteorNotifier {

    private Meteor mSpace;
    private int workerid;
    private String peerIP;
    private OverlayService overlays;
    private boolean workerAlive = true;
    public Sender sender = getSender();
    
    //these are to refresh the subscription    
    long srefreshtime=0;
    Thread refreshSubscription=null;
    long monSubPeriod=0;
    boolean subscribed=false;
    private final Object lock = new Object();
    
    Hashtable<String, String> peerProperties; //all properties form files. DONT use system.getproperties for non-static things
    
    private int ttl;
    private int numSubscriptions;
    
    private int initialTaskCount = 1;
    
    private Logger workerLog;
    
    public int getInitialTaskCount(){
        return initialTaskCount;
    }
    public void setInitialTaskCount(int value){
        initialTaskCount=value;
    }
    public String getPeerIP(){
        return peerIP;
    }
    public void setPeerProperties(Hashtable<String, String> properties){ //this method is called from FedMeteorDrtsPeer
        peerProperties=properties;
    }
    public Hashtable<String, String> getPeerProperties(){ //this method is called from FedMeteorDrtsPeer
        return peerProperties;
    }
    public void MeteorGenericWorker() {
    }
    
    public int getWorkerid(){
        return workerid;
    }
    /* (non-Javadoc)
     * @see tassl.automate.meteor.application.MeteorWorkerFramework#setMeteorEnv(tassl.automate.meteor.Meteor, int, java.lang.String, tassl.automate.overlay.OverlayService)
     */
    @Override
        public void setMeteorEnv(Meteor space, int workerID, String peerIPaddr, OverlayService overlay) {
        mSpace = space;
        workerid = workerID;
        peerIP = peerIPaddr;
        overlays = overlay;

        //important for communication with master
        sender.SetWorkerHandle((WorkerFramework) this);
        
       }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.WorkerFramework#computeTask(java.lang.Object)
     */
    @Override
    public Object computeTask(Object obj) {
        FedMeteorGenericTaskTuple taskTup=null;
        Object ret = null;
        try {
            Class taskclass = Class.forName(peerProperties.get("TaskClass"));            
            taskTup = (FedMeteorGenericTaskTuple)taskclass.newInstance();
            
            taskTup.getTaskTuple((XmlTuple) obj); 
            
            Object dataobj = programming5.io.Serializer.deserialize(((XmlTuple) obj).getData());
            ret=computeTaskSpecific(dataobj, taskTup);
            
        } catch (IOException ex) {
            //Logger.getLogger(FedMeteorGenericWorker.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(ex),"severe");
        } catch (ClassNotFoundException ex) {
            //Logger.getLogger(FedMeteorGenericWorker.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
        } catch (InstantiationException ex) {
           // Logger.getLogger(FedMeteorGenericWorker.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
        } catch (IllegalAccessException ex) {
           // Logger.getLogger(FedMeteorGenericWorker.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
        }

        //System.out.println("AppWorker " + peerIP + " consumes taskid " + taskTup.getTaskid() + " from Master "+taskTup.getMasterName());
        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","AppWorker " + peerIP + " consumes taskid " + taskTup.getTaskid() + " from Master "+taskTup.getMasterName(),"info");
        return ret;
    }
    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.WorkerFramework#quit()
     */
    @Override
    public void quit() {
        workerAlive = false;
             
        if (refreshSubscription!=null){
            refreshSubscription.interrupt();
        }
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.WorkerFramework#sendResultToMaster(int, java.lang.Object, java.lang.String, java.lang.String)
     */

    /**
     *
     * @param taskid
     * @param data
     * @param message
     * @param masterName
     */
    
    @Override
        public void sendResultToMaster(int taskid, Object data, String message, String masterName) {
        CMessage cmsg = new CMessage(MessageType.Block_result, taskid, 0);
        cmsg.setPayLoad(data);
        cmsg.setMsg(message);
        sender.sendOut(cmsg, masterName, ControlConstant.TCP_CONTROL_PORT);

    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.WorkerFramework#startWorker()
     */
    @Override
    public void startWorker() {
        //Debug.println("Meteor space has been initialized", "Meteor.app");
        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","Meteor space has been initialized","info");
        System.out.println("StartWorker "+Thread.currentThread().getName());
        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","StartWorker "+Thread.currentThread().getName(),"info");

        
        monSubPeriod=Long.parseLong(getProperty("meteor.RefreshSubscriptionPeriod", "0").trim());
        if (monSubPeriod!=0){
            //Monitor enabled, the subscription is refreshed every X milliseconds
            //This is useful when peers, that insert tasks, join the overlay after the initial subscription is done.
            //Once the subscription is refreshed (unsuscribe and subcribe) everyone in the overlay konws about it.
            RefresSubscriptionThread rfs=new RefresSubscriptionThread();
            refreshSubscription=new Thread(rfs);
            refreshSubscription.start();
        }
            
        numSubscriptions=Integer.parseInt(getProperty("meteor.NumberSubscription", "1").trim());
        ttl=numSubscriptions;
        meteorSubscribe(numSubscriptions);  
        subscribed=true;
        
        startWorkerSpecific();
        
    }

    public void meteorSubscribe(int ttl){
        //get XmlTuple for query
        XmlTuple template;    
        template=getTemplateQuery();
        //The last value is the number of tasks to be consumed before the subscription expires. 0 means infinite.
        mSpace.meteorSubscribe(CometConstants.spacename, template, peerIP, this, ttl);
                 
    }
    
    public void meteorUnsubscribe(){
        //get XmlTuple for query
        XmlTuple template;
        template=getTemplateQuery();
        mSpace.meteorUnsubscribe(CometConstants.spacename, template, peerIP, this);
         
    }
    
    /* (non-Javadoc)
     * @see tassl.automate.meteor.MeteorNotifier#meteorNotifyNode(tassl.automate.comet.XmlTuple)
     */
    @Override
    public synchronized void meteorNotifyNode(XmlTuple tup) {
        
        consumeTask(tup);
                     
        synchronized(lock){  //to prevent executing several tasks at the same time when the subscription is for several tasks     
            if (workerAlive && subscribed && ttl == 0){// && monSubPeriod==0){
                //request for nect task
                //System.out.println("ReSubscribed after consuming Task");

                FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","ReSubscribed after consuming Task","info");
                ttl=numSubscriptions;
                meteorSubscribe(numSubscriptions);
            } 
        }
    }
    
    public void consumeTask(XmlTuple tup){
        synchronized(lock){  //to prevent executing several tasks at the same time when the subscription is for several tasks
            Object result;
            FedMeteorGenericTaskTuple taskTup = null;
            try{
                Class taskclass = Class.forName(peerProperties.get("TaskClass"));            
                taskTup = (FedMeteorGenericTaskTuple)taskclass.newInstance();  

                taskTup.getTaskTuple(tup);
            
                ttl--;
                srefreshtime = System.currentTimeMillis();

                if (taskTup.data[0] == 0) {
                    System.out.println("data is null??");
                    //workerLog.info("data is null??");
                    FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","data is null??","info");
                } else {
                    try {

                        //System.out.println("Start consuming a task " + taskTup.getTaskid());   
                        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","Start consuming task: " + taskTup.getTaskid(),"info");

                        Object obj = programming5.io.Serializer.deserialize(tup.getData());
                        if (obj != null && obj.toString().equals("poisonpill") == true) {
                            //after getting poisonpill, worker will leave the overlay
                            workerLeave(taskTup.getMasterName());

                        } else {
                            

                            //do something with task
                            //  memMon.reset();        
                            result = computeTask(tup);
                            //memMon.getUsedMem();
                            
                            
                            //this has been moved to Subscriber worker
                            /*
                            String msg = "task " + taskTup.getTaskid() + " " + (etime - stime);

                            //send the calculated result back to the master using TCP
                            sendResultToMaster(taskTup.getTaskid(), result, msg, taskTup.getMasterName());
                            * 
                            */
                        }

                    } catch (IOException ex) {
                        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
                    } catch (ClassNotFoundException ex) {
                        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
                    } 

                }
            } catch (ClassNotFoundException ex) {
                FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
            } catch (InstantiationException ex) {
                FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
            } catch (IllegalAccessException ex) {
                FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
            }
        }
    }
    /**
     * Worker leave overlay after receiving poisonpill.
     * @param masterName 
     */
    public void workerLeave(String masterName){
        try {
            FedLog.writeLog(peerIP.split(":")[1], "workerLog%g-" + peerIP.substring(2) + ".log", "worker " + workerid + " get poison pill", "info");

            //send a notification to master for terminating instances
            String myName = getProperty("chord.LOCAL_URI", "//" + InetAddress.getLocalHost().getHostAddress());
            myName = myName.substring(2);
            SendClient client = getClient();
            System.out.println("myName: "+myName+" leave master: "+masterName +":"+ControlConstant.TCP_CONTROL_PORT);
            client.sendMsgToMaster(myName, MessageType.Worker_leave, null, masterName);
            //System.out.println("worker " + workerid + " sends master its leave");
            FedLog.writeLog(peerIP.split(":")[1], "workerLog%g-" + peerIP.substring(2) + ".log", "worker " + workerid + " sends master its leave", "info");

            //leave the overlay
            overlays.leave();
            System.out.println("worker " + workerid + " overlay leave");
            FedLog.writeLog(peerIP.split(":")[1], "workerLog%g-" + peerIP.substring(2) + ".log", "worker " + workerid + " overlay leave", "info");
            quit();
        } catch (UnknownHostException ex) {
            FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log",FedLog.getExceptionStack(ex),"severe");
        }

    }
    
    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.WorkerFramework#setCometEnv(tassl.automate.comet.CometSpace, int, java.lang.String, tassl.automate.overlay.OverlayService)
     */
    @Override
    public void setCometEnv(CometSpace space, int workerID, String peerIPaddr, OverlayService overlay) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    private SendClient getClient(){
	SendClient ret;
        if(getProperty("chord.NETWORK_CLASS", "TCP").equals("SSL")){
            ret = new SSLClient();
            //System.out.println("getClient: Created a SSL client");
        }
        else{
            ret = new TCPClient();
            //System.out.println("getClient: Created a TCP client");
        }
        return ret;
    }

    private Sender getSender(){
        Sender ret;
        if(getProperty("chord.NETWORK_CLASS", "TCP").equals("SSL")){
            ret = new SendSSL();
            //System.out.println("getSender: Created a SSL sender");
        }
        else{
            ret = new SendTCP();
            //System.out.println("getSender: Created a TCP sender");
        }
        return ret;
    }
    
    public String getProperty(String propertyName, String defaultValue) {
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
    
    private class RefresSubscriptionThread implements Runnable {
        public void run() {        
            srefreshtime = System.currentTimeMillis();
            long etime;
            while(workerAlive){                
                etime = System.currentTimeMillis();    
                if (monSubPeriod < (etime - srefreshtime)){
                    //System.out.println("MeteorAppWorker: "+peerIP+" -------> refreshing subscription");             
                    //workerLog.info("MeteorAppWorker: refreshing subscription");
                    FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","MeteorAppWorker: refreshing subscription","info");
                                       
                    subscribed=false;                    
                    synchronized(lock){                    
                        meteorUnsubscribe();                        
                        meteorSubscribe(numSubscriptions);             
                        ttl=numSubscriptions;
                        subscribed=true;
                    }
                    srefreshtime = System.currentTimeMillis();
                }
                try {
                    Thread.sleep(monSubPeriod);
                } catch (InterruptedException ex) {
                    if (workerAlive)
                        //Logger.getLogger(FedMeteorGenericWorker.class.getName()).log(Level.SEVERE, null, ex);
                        FedLog.writeLog(peerIP.split(":")[1],"workerLog%g-"+peerIP.substring(2) +".log","Meteor space has been initialized","info");
                }
            }
        }
    }
    
    //method to overrite if we customize the TaskTuple by extending GenericTaskTuple class
    public XmlTuple getTemplateQuery(){
        XmlTuple template = new XmlTupleService();
        FedMeteorGenericTaskTuple taskTup = new FedMeteorGenericTaskTuple();
        template = taskTup.getQuery();
        return template;
    }
    
    //process the tasks. It is called after computeTask retrieve the data associated with the task
    public abstract Object computeTaskSpecific(Object dataobj, FedMeteorGenericTaskTuple tasktuple);
    //start server thread to listen for appMaster to send back result
    public abstract void startWorkerSpecific();
            
    //close the file handler for log
    public void closeLogHandler(Logger log){
        Handler[] handlers = log.getHandlers();
        
        for(int i=0; i<handlers.length; i++){
                handlers[i].close();
        }
    }    
}