/*
 * Copyright (c) 2009, NSF Center for Autonomic Computing, Rutgers University 
 * All rights reserved..
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
 * MeteorAppMaster.java
 *
 * Created on Nov 26, 2010 7:59:13 PM
 */
package tassl.automate.federation.simpleapi;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.*;
import programming5.io.FileHandler;
import tassl.automate.comet.CometConstants;
import tassl.automate.comet.CometSpace;
import tassl.automate.comet.XmlTuple;
import tassl.automate.comet.xmltuplespace.XmlTupleService;
import tassl.automate.meteor.Meteor;
import tassl.automate.meteor.application.MeteorMasterFramework;
import tassl.automate.meteor.application.MeteorTaskMonitoring;
import tassl.automate.overlay.OverlayService;
import tassl.automate.util.FedLog;


/**
 * @author Samprita Hegde
 * created on Nov 26, 2010 7:59:13 PM
 *
 */
public abstract class FedMeteorGenericMaster extends Thread implements MeteorMasterFramework {

    private Meteor mSpace;
    private int masterid;
    private String peerIP;
    private OverlayService overlays;
    public String masterNetName = null;
    public HashMap<Integer, Integer> taskstatus = new HashMap<Integer, Integer>();
    private int numoftasks; 
    private int numFinishedJob;
    private boolean jobDone;
    private int numPoisonPill = 0;
    private int confirmedPoisonPill = 0;
    private double totalSum = 0;

    private Thread MasterThread; //Figure out who calls this one to control threads.
    private Thread TaskGen; 
    private Thread Monitoring;
    
    Hashtable<String, String> peerProperties; //all properties form files. DONT use system.getproperties for non-static things
    
    public void setPeerProperties(Hashtable<String, String> properties){ //this method is called from FedMeteorDrtsPeer
        peerProperties=properties;
    }
    public Hashtable<String, String> getPeerProperties(){ //this method is called from FedMeteorDrtsPeer
        return peerProperties;
    }
    
    public String getPeerIP(){
        return peerIP;
    }
    
    public Meteor getMspace(){
        return mSpace;
    }
    
    public OverlayService getOverlays(){
        return overlays;
    }
    
    public boolean isJobDone(){
        return jobDone;
    }
    
    public void setJobDone(boolean value){
        jobDone=value;
    }
    
    public void waitMonitoring(){
        if (Monitoring!=null){
            try{
                Monitoring.join();
            }catch(InterruptedException e){
                //System.err.println("Error in joing Monitoring thread.");
                FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(e),"severe");
            }
        }
    }
    public void waitMaster(){
        if (MasterThread!=null){
            try{
                MasterThread.join();
            }catch(InterruptedException e){
               // System.err.println("Error in joing Master thread.");
               FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(e),"severe");
            }
        }
    }
    
    /* (non-Javadoc)
     * @see tassl.automate.meteor.application.MeteorMasterFramework#setMeteorEnv(tassl.automate.meteor.Meteor, int, java.lang.String, tassl.automate.overlay.OverlayService)
     */
    @Override
    public void setMeteorEnv(Meteor space, int masterID, String peerIPaddr, OverlayService overlay) {
        mSpace = space;
        masterid = masterID;
        peerIP = peerIPaddr;
        overlays = overlay;
        masterNetName = (peerIP.split(":")[0]).substring(2);
    }

    @Override
    public void run() {
        //System.out.println("MeteorGenericMaster Run");
        FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","MeteorGenericMaster Run","info");
        
        long stime = System.currentTimeMillis();
        //start task generator
        numFinishedJob = 0;
        TaskGenerateThread taskGen = new TaskGenerateThread();
        TaskGen=new Thread(taskGen);
        TaskGen.start();        

        //set loop condition
        jobDone = false;

        //start task monitor after loop condition is set
        long monPeriod = Long.parseLong(getProperty("TaskMonitoringPeriod","0"));
        MeteorTaskMonitoring mon = new MeteorTaskMonitoring((MeteorMasterFramework)this, mSpace, overlays);
        if (monPeriod > 0) {
            Monitoring=new Thread(mon);
            Monitoring.start();
        }

        //wait for results
        try {
            synchronized(this) {
                wait();
            }
        } catch (InterruptedException ex) {
            //Logger.getLogger(FedMeteorGenericMaster.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(ex),"severe");
        }

        if (monPeriod > 0){
            mon.quit(); //indicate that has to leave
            mon.interrupt(); //interrupt to don't wait until sleeps finishes
        }

        //System.out.println("Totaltime = " + (System.currentTimeMillis() - stime));
        //System.out.println("MeteorGenericMaster quits");
        
        FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","Totaltime = " + (System.currentTimeMillis() - stime),"info");
        FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","MeteorGenericMaster quits","info");

        //sending poison pills
        //System.out.println("inserting kill Jobs");
        FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","inserting kill Jobs","info");
        String workerNum = peerProperties.get("WorkerNum");
        if (workerNum == null) {
            try {
                int pills = 0;

                FileHandler f = new FileHandler("nodeFile", FileHandler.HandleMode.READ);
                String line = null;
                while ((line = f.readLine())!=null) {
                    pills += Integer.parseInt(line.split(":")[1]);
                }
                f.close();

                f = new FileHandler("exceptionFile", FileHandler.HandleMode.READ);
                while((line = f.readLine())!=null) {
                    if (line.contains("comet.NodeType")
                            && !line.split("=")[1].equals("WORKER")) {
                        pills--;
                    }
                }

                workerNum = Integer.toString(pills);
            } catch (IOException ex) {
                //ex.printStackTrace();
                FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(ex),"severe");
            }
        }

        if (workerNum == null) {
            //System.out.println("Fail to send poisonpills");
            FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","Fail to send poisonpills","severe");
        } else {
            removeNodes(Integer.parseInt(workerNum));
            if (Integer.parseInt(workerNum)!=0){  //Javi: just in case we only have isolated workers. Otherwise it waits here forever.
                try {
                    synchronized(this) {
                        wait();
                    }
                } catch (InterruptedException ex) {
                    //Logger.getLogger(FedMeteorGenericMaster.class.getName()).log(Level.SEVERE, null, ex);
                    FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(ex),"severe");
                }
            }
        }

    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#confirmWorkerLeave(java.lang.String)
     */
    @Override
    public void confirmWorkerLeave(String nodeHostName) {
        confirmedPoisonPill++;
        System.out.println("confirmWorkerLeave count=" + confirmedPoisonPill);
        
        if (confirmedPoisonPill == numPoisonPill) {
            synchronized(this){
                notify();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#countFinishedTasks()
     */
    @Override
    public int countFinishedTasks() {
        return numFinishedJob;
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#getData(int)
     */
    @Override
    public Object getData(int taskid) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#getNumOfTasks()
     */
    @Override
    public int getNumOfTasks() {
        return numoftasks;
    }
    
    public void setNumOfTasks(int tasks){
        numoftasks=tasks;
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#getTaskStatus()
     */
    @Override
    public Object getTaskStatus() {
        return taskstatus;
    }

    public int getTaskStatus(int taskid) {
        return (taskstatus.get(taskid)==null)?0:1;
    }
    
    public void setTaskStatus(int taskid) {
        if (taskstatus.get(taskid)==null ||taskstatus.get(taskid)==0){
            taskstatus.put(taskid, 1);
            numFinishedJob++;
            
        } else {
            System.out.println("duplicated task received: taskid = "+taskid);
            return;
        }
    }
    
    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#quit()
     */
    @Override
    public void quit() {
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#reinsertTask(int)
     */
    @Override
    public void reinsertTask(int taskid) {
        System.out.println("MeteorGenericMaster: Reinserting missing task "+taskid);
        insertTask(taskid);

    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#removeNodes(int)
     */
    @Override
    public boolean removeNodes(int numNodes) {
        numPoisonPill = numNodes;
        confirmedPoisonPill = 0;
        if(numNodes != 0){
            System.out.println("total poison pill " + numNodes);

            for (int i = 0; i < numNodes; i++) {
                XmlTuple task = createPoisonTaskTuple(i);

                byte[] DataArr = null;
                try {
                    DataArr = programming5.io.Serializer.serializeBytes("poisonpill");
                    task.setData(DataArr);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

                //Call to Comet API 'Out'
                mSpace.meteorStore(CometConstants.spacename, task, DataArr, peerIP, overlays);
                System.out.println("put poison pill id=" + i);
            }
        }

        return true;
    }
    
    public boolean removeNodes(String destIP, String ports) {
        String[] port = ports.split(":");
        numPoisonPill = port.length;
        confirmedPoisonPill = 0;
        System.out.println("total poison pill " + numPoisonPill);
        
        for (int i = 0; i < numPoisonPill; i++) {
            byte[] DataArr = null;
            XmlTuple task = generatePoisonTuple(i,destIP,port[i],DataArr);
            
            //Call to Comet API 'Out'
            mSpace.meteorStore(CometConstants.spacename, task, DataArr, peerIP, overlays);
            System.out.println("put poison pill id=" + i);
        }

        return true;
    }
    
    public XmlTuple generatePoisonTuple(int id, String destIP, String port, byte[] DataArr){
        XmlTuple task = createPoisonTaskTuple(id, destIP, port);

        try {
            DataArr = programming5.io.Serializer.serializeBytes("poisonpill");
            task.setData(DataArr);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return task;
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#setCometEnv(tassl.automate.comet.CometSpace, int, java.lang.String, tassl.automate.overlay.OverlayService)
     */
    @Override
    public void setCometEnv(CometSpace space, int masterID, String peerIPaddr, OverlayService overlay) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#setResult(int, java.lang.Object, java.lang.String, java.lang.String)
     */
    @Override
    public synchronized void setResult(int taskid, Object data, String message, String sender) {
        
        
        setResultSpecific(taskid, data, message,sender);        
        
        if (jobDone){
            System.out.println("Job Done, notify"); 
            FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","Job Done, notify","info");        
            synchronized(this) {
                notify();
            }
        }
                
    }
       
    /* (non-Javadoc)
     * @see tassl.automate.programmodel.masterworker.MasterFramework#startMaster()
     */
    @Override
    public void startMaster() {
        MasterThread = new Thread(this);
        MasterThread.start();
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
    
    public class TaskGenerateThread implements Runnable {
        
        public void run() {
            //get master name
            /*
            try {
                InetAddress address = InetAddress.getLocalHost();
                masterNetName = address.getHostName();
            } catch (Exception e) {
                e.printStackTrace();
            }
            */     
            generateTasks();
            mSpace.printLoadBalancingResult();
        }
    }

    public void insertTask(int taskid){
        try {
            
            byte[] data = null;
            Object temp;
            XmlTuple task;
            
            //create task XML tuple
            task=createTaskTuple(taskid);
            
            temp = (Object)createTaskData(taskid);
            
            //set data in your task
            data = programming5.io.Serializer.serializeBytes(temp);
            task.setData(data);

            //insert into comet space
            mSpace.meteorStore(CometConstants.spacename, task, data, peerIP, overlays);
            System.out.println("Application taskid " + taskid + " is out");   
            FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log","Application taskid " + taskid + " is out","info");

        } catch (IOException ex) {
            //Logger.getLogger(FedMeteorGenericMaster.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog(peerIP.split(":")[1],"masterLog%g-"+peerIP.substring(2)+".log",FedLog.getExceptionStack(ex),"severe");
        }
    }
    
    //method to overrite if we customize the TaskTuple by extending GenericTaskTuple class
    public XmlTuple createTaskTuple(int taskid){
        XmlTuple task = new XmlTupleService();
        String taskstr = null;
        
        FedMeteorGenericTaskTuple tasktuple = new FedMeteorGenericTaskTuple();
        taskstr = tasktuple.setTaskStr(taskid, masterNetName);
        task.createXMLtuple(taskstr);
        
        return task;
    }
    //method to overrite if we customize the TaskTuple by extending GenericTaskTuple class
    public XmlTuple createPoisonTaskTuple(int id){
        XmlTuple task = new XmlTupleService();                
        FedMeteorGenericTaskTuple tasktuple = new FedMeteorGenericTaskTuple();
        task.createXMLtuple(tasktuple.createPoisonpillTag(masterNetName, id));        
        return task;
    }
    
    public abstract XmlTuple createPoisonTaskTuple(int id, String destIP, String port);
    
    //call after setResults
    public abstract boolean setResultSpecific(int taskid, Object data, String message, String sender);
    
    /*Called when the TaskGenerateThread thread is created
     * Read variables specified in config file and get the number of tasks
     * then start creating tasks by calling insertTask method
     */
    public abstract void generateTasks();
    
    //create the data associated to a task
    public abstract Object createTaskData(int taskid);
    
}
