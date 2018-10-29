    /*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tassl.automate.federation;

import ifc2x3javatoolbox.ifc2x3tc1.ClassInterface;
import ifc2x3javatoolbox.ifcmodel.IfcModel;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.io.FileHandler;
import tassl.automate.comet.XmlTuple;
import tassl.automate.federation.simpleapi.FedMeteorGenericTaskTuple;
import tassl.automate.federation.simpleapi.FedMeteorGenericWorker;
import tassl.automate.util.FedLog;

/**
 *
 * @author Ioan Petri
 */
public class FedMeteorSubscriberWorker extends FedMeteorGenericWorker{
    
    private int testmode;       //mode 0 run task in fedworker, mode 1 send task to appmaster
    int numSimulation = 0;
    private double totalBalance;
    private String totalBalanceFile = "totalBalance";
    private double localMultiplier = 0.5;
    private double externalMultiplier = 2;
    private String associatedPort="";
    private String defaultPort = "-1";
    private String defaultIP = "-1";
        
    private String location="";
    String subscriptionType;
    String localIP;
    
    private ServerSocket serverSocket = null;
    private Socket cSocket = null;
    
    private String eplusAppMasterIP ;
    private String eplusAppMasterPort ;   
    private String cancerAppMasterIP;
    private String cancerAppMasterPort;
    private String recvTaskFromAppMasterPort;   //all apps send to same receive port 
    private int taskID;
    
    private Object lock = new Object();
    private Object taskLock = new Object();
    private final Object socketLock = new Object(); //lock for socket
    
    private HashMap<Integer,Task> taskList;
    
    int infoServicePort;
    /**
     * Start a server for accepting appMaster to send back results.
     */
    @Override
    public void startWorkerSpecific(){
        subscriptionType = this.getProperty("subscription","NULL_TYPE");
        
        getRecvFromAppMasterPort(subscriptionType);
        
        eplusAppMasterIP = this.getProperty("eplusAppMasterIP", "localhost");
        eplusAppMasterPort = this.getProperty("eplusAppMasterPort", "5585");
        
        cancerAppMasterIP = this.getProperty("cancerAppMasterIP", "localhost");
        cancerAppMasterPort = this.getProperty("cancerAppMasterPort", "5585");

        location = this.getProperty("location","rutgers");
        testmode = (this.getProperty("testmode", "enableSeparateExecution")).equals("enableSeparateExecution")? 1 : 0;
        
        taskList = new HashMap<Integer,Task>();
        //init taskID
        taskID = 0;
        String[] info = this.getPeerIP().substring(2).split(":");
        localIP = info[0];
        
        infoServicePort=Integer.parseInt(System.getProperty("infoServicePort"));
        //create server thread to receive local master's computation request or receive result  
        Thread infoRequest = new Thread(new computeTaskRequest());
        infoRequest.start();
    }
    // Read port from property that's used as the port to recv connection from AppMaster. 
    // Different application has diff port num.
    public void getRecvFromAppMasterPort(String subscriptionType){
        recvTaskFromAppMasterPort = "1" + this.getPeerIP().split(":")[1] ;
        /*
        String appType = subscriptionType.split(",")[1];
        if(appType.equals("c4c")){
            recvTaskFromAppMasterPort = this.getProperty("recvTaskFromEplusPort", defaultPort);
        }else if(appType.equals("cancer")){
            recvTaskFromAppMasterPort = this.getProperty("recvTaskFromCancerPort", defaultPort);
        }else{
            recvTaskFromAppMasterPort = this.getProperty("recvTaskFromAppMasterPort", "5584");
        }
        * 
        */
        System.out.println("recvTaskFromAppMasterPort: "+recvTaskFromAppMasterPort);
    }
    
    public int getUpdatedTaskID(){
            int newTaskID;
            synchronized (lock) {
                ++taskID;
                newTaskID = taskID;
            }
            return newTaskID;
    }
    
    public boolean checkTaskFinished(Integer currentTaskID){
        synchronized (taskLock) {
            if(taskList.containsKey(currentTaskID)){
                if(taskList.get(currentTaskID).getResult() != null ){
                    return true;
                }
            }
            return false;
        } 
    }
    
    public void setResultToTaskList(Integer currentTaskID, String resultValue){
        synchronized (taskLock) {
            if(taskList.containsKey(currentTaskID)){
                Task task = taskList.get(currentTaskID);
                task.setResult(resultValue);
            }
            else{
                System.out.println("Return task not exist!");
            }
        }
    }
    public void insertIntoTaskList(Integer currentTaskID, FedTaskTuple tuple, long stime,double cost){
        Task task = new Task(tuple,stime,cost);
        synchronized (taskLock) {
            taskList.put(currentTaskID, task);
        }
    }
    public void removeTaskFromList(Integer currentTaskID){
        synchronized(taskLock){
            if(taskList.containsKey(currentTaskID)){
                taskList.remove(currentTaskID);
            }
        }
    }
    public Task getTaskFromList(Integer currentTaskID){
        synchronized (taskLock) {
            if(taskList.containsKey(currentTaskID)){
                return taskList.get(currentTaskID);
            }
            else
                return null;
        }
    }
   
    /**
     * Simple local computation.
     * @param data, input data for computation.
     * @return 
     */
    public String doLocalComputation(Object data){
        double sum = 0;
        double num = 100.0;

        for (double i = 0; i < num;) {
            sum += i;
            i += 0.1;
        }
        return sum+"";
    }
    
    public void scenarioOpt(String _deadline, long _timestamp, String _taskLocation, String _taskType, int _numSimulation,
            int _taskid, String _scenarioType, String _optType) {
        DataOutputStream out;

        Socket sock;
        try {
            sock = new Socket(localIP, infoServicePort);
            out = new DataOutputStream(sock.getOutputStream());

            String origin;
            if (_taskLocation.equals(location)) {
                origin = "local";
            } else {
                origin = "external";
            }
            out.writeUTF(_scenarioType+"-" + _taskType + "-" + _deadline + "-" + origin + 
                    "-" + _timestamp + "-" + _numSimulation+ "-" +_taskid + "-" + _optType);

        } catch (UnknownHostException ex) {
            Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String scenarioCheck(String _deadline, long _timestamp, String _taskLocation, String _taskType, int _numSimulation,
            String _scenarioType) {
        DataOutputStream out;
        DataInputStream in;
        String answer = "";

        Socket sock;
        try {
            sock = new Socket(localIP, infoServicePort);
            out = new DataOutputStream(sock.getOutputStream());
            in=new DataInputStream(sock.getInputStream());

            String origin;
            if (_taskLocation.equals(location)) {
                origin = "local";
            } else {
                origin = "external";
            }
            out.writeUTF(_scenarioType+"-" + _taskType + "-" + _deadline + "-" + origin + 
                    "-" + _timestamp + "-" + _numSimulation);
            
            answer=in.readUTF();
            out.writeUTF("success");        //similar to send an acknoledge message back for other side to close
        } catch (UnknownHostException ex) {
            Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
       return answer;
    }
    
    
   
    
    @Override
    public Object computeTaskSpecific(Object dataobj, FedMeteorGenericTaskTuple tasktuple) {     
        String result = "";
        long stime = new Date(System.currentTimeMillis()).getHours();
    	      
        //long stime = System.currentTimeMillis();
        ArrayList<String[]> b=(ArrayList<String[]>) dataobj;
       // System.out.println("[Notification-ComputeTask] Recieved syncronization data");
        //Integer currentTaskID = Integer.valueOf(getUpdatedTaskID());
        Integer currentTaskID = ((FedTaskTuple) tasktuple).getTaskid();
        String taskLocation = ((FedTaskTuple) tasktuple).getLocation();
        String taskType = ((FedTaskTuple) tasktuple).gettaskType();
        String tupleTag = ((FedTaskTuple) tasktuple).gettupleTag();
        String info1 = ((FedTaskTuple) tasktuple).getInfo1();
        String info2 = ((FedTaskTuple) tasktuple).getInfo2();
        
        //if the tuple is request message
        String token="c4csecretkey";
        String disc="-none-";
        String c4cRes1="S1";
        String ret="Initialized with no fetch";
        //ArrayList<HashMap<String,String>> models=null;
        //this is the map to store the model
        HashMap<String,String> model1=null;
        String now=C4CHelper.getDate();
        String tag=null;
       //
        //PETRI
        if(tupleTag.equals("REQ")){
           //System.out.println("---We are in worker compute task: " +tupleTag);
            DataOutputStream out;
            DataInputStream in;
            
            //List data = (List) dataobj;
            String retParam = "";
            synchronized(socketLock){
            try {
                Socket sock=new Socket(localIP,infoServicePort);
                out=new DataOutputStream(sock.getOutputStream());
                in=new DataInputStream(sock.getInputStream());
               
                long timestamp=((FedTaskTuple)tasktuple).getTimestamp(); 
                String origin;
                if (taskLocation.equals(location)){
                    origin="local";
                }else{
                    origin="external";
                }
                disc=C4CHelper.getDiscipline(token);
                //System.out.println("[computeTask] call received with:" +info1+"--"+info2 );
                
                tag=info1;
                if(taskType.equals("c4c")){
                  //  System.out.println("[C4C-task][computeTask] received with tag:"+info2);
                    if (info1.contains("EVENT"))
                    {
                        //C4CHelper.setProjectMetaDataOld(info1, info2, token);
                        //System.out.println("[computerTask] EVENT tasks execution:" +info2);
                        if (info2.equals("MET")&&(!disc.substring(0, 1).equals(info1.substring(6, 7)))) {
                        //Thread.sleep(100);
                        System.out.println("[Notification-ComputeTask] Received updated metadata for "+b.size()+" objects from disicpline "+info1.substring(6,7));
                        //System.out.println("[computeTask] MET task non-local, medatadata to add: "+info1);
                        //Thread.sleep(100);
                        for (int i = 0; i < b.size(); i++) {
                            C4CHelper.setProjectMetaData(b.get(i)[0],""+b.get(i)[1],b.get(i)[2],b.get(i)[3], token);
                        }
                       // System.out.println("[computeTask] MET medatadata task completed " +info1);
                        //Thread.sleep(10000);
                        
                        } else if (info2.equals("VER")&&(!disc.substring(0, 1).equals(info1.substring(6, 7)))){
                        //Thread.sleep(100);
                        System.out.println("[Notification-ComputeTask] Received updated versions for "+b.size()+" objects from disicpline "+info1.substring(6,7));
                        //System.out.println("[computeTask] VER task non-local, versions to add: "+info1);
                        //Thread.sleep(100);
                        for (int i = 0; i < b.size(); i++) {
                            C4CHelper.setProjectVersion(b.get(i)[0],""+b.get(i)[1],b.get(i)[2],b.get(i)[3], token);
                        }
                        //System.out.println("[computeTask] VER version task completed " +info1);
                        //Thread.sleep(10000);
                        }
                        ret="EVENT";
                        } else if (info1.equals("FETCH")){
                        String localDiscipline=disc;
                        //System.out.println("[Notification-ComputeTask] Recieved Request for Model Data from:"+info1.substring(6,7));
                           try {
                               // System.out.println("[Notification-computeTask] FETCH calling getMyCurrent model on remote discipline "+info1);
                                model1=C4CHelper.getMyCurrentModel(token);
                                //tag="FETCH";
                               //}// ret=defaultRet;
                                System.out.println("[Notification-ComputeTask] Returning Model of Size:"+model1.size());
                            } catch (Exception ex) {
                                Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
                            }
                         ret="FETCH";
                       // }
                        } else if (info1.equals("CONFIG")){
                        String localDiscipline=disc;
                        System.out.println("[Notification-ComputeTask] Received data from discipline:"+info2);
                           try {
                               // System.out.println("[Notification-computeTask] FETCH calling getMyCurrent model on remote discipline "+info1);
                                for (int i = 0; i < b.size(); i++) {
                                    //C4CHelper.setProjectVersion(b.get(i)[0],""+b.get(i)[1],b.get(i)[2], token);
                                    System.out.println("[Notification-ComputeTask] Config element:"+b.get(i)[0]);
                                }
                                System.out.println("[Notification-ComputeTask] Received config of size:"+b.size());
                            } catch (Exception ex) {
                                Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
                            }
                           ret="CONFIG";
                        }
                        
                    
                    
                 }else{
                    System.out.println("[Notification-computeTask] Discipline is not local, skip:"+info1);
                    retParam = "null";
                }
            } catch (UnknownHostException ex) {
                Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
            }   catch (Exception ex) {
                    Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            //Should return the IPs of real workers instead of "this.getPeerIP().substring(2)" below
            result = tag+","+taskType+","+
                    this.getPeerIP().substring(2).split(":")[0] +","+associatedPort+":"+ this.getPeerIP().split(":")[1] 
                    +","+((FedTaskTuple)tasktuple).getOptID()+","+retParam+","+disc;
                    //+","+((FedTaskTuple)tasktuple).getOptID()+","+_reputation+","+numSimulation;
            
        } else if (tupleTag.equals("EVE"))
        {
            result = "EVENT,"+taskType+","+
                    this.getPeerIP().substring(2).split(":")[0] +","+associatedPort+":"+ this.getPeerIP().split(":")[1] 
                    +","+((FedTaskTuple)tasktuple).getOptID()+","+disc;
        }
        //if the tuple is compute message
        
        else{
            result = "ERR,Unknown,NULL,"+this.getPeerIP().substring(2).replaceFirst(":", ",")+","+((FedTaskTuple)tasktuple).getOptID();
            FedLog.writeLog(this.getPeerIP().split(":")[1],"workerLog%g-"+this.getPeerIP().substring(2).substring(2) +".log", 
                    "FedMeteorSubscriberWorker: computerTaskSpecific--No match message type found!","info");
        }
        //PETRI -- OUTPUT HERE 'WORKER RECEIVED OBJECT ID FROM MASTER ID'
      //  System.out.println("AppWorker " + this.getPeerIP() + " consumes taskID " + currentTaskID + " "+tupleTag+"," + 
        //        " operation id="+((FedTaskTuple)tasktuple).getOptID()+ " from Master "+((FedTaskTuple)tasktuple).getMasterName());
        
        if(!(tupleTag.equals("CPT") && testmode == 1)){
            //PETRI
           //System.out.println("Directly send back result to master: "+currentTaskID+" - "+result);
             System.out.println("[C4C-Notification] Directly send back result to master: "+currentTaskID);
            long etime = System.currentTimeMillis();
            String res=ret;
            String msg = "task " + currentTaskID + " " + (etime - stime);
            System.out.println("[Notification-computeTask] Synchronization Complete");
            sendResultToMaster(currentTaskID, model1, result, ((FedTaskTuple)tasktuple).getMasterName());
                        
            //System.out.println("C4C process is finished!" + currentTaskID+" result" +result+"res" +res+"taskMaster" +((FedTaskTuple)tasktuple).getMasterName() );
        }
        return null;
    }
    
    //Method override due to the change in the TaskTuple
    @Override
    public XmlTuple getTemplateQuery(){
        String property, tupleTag, taskType;
        boolean isSingle = false;
        String localIP, localPort;

        XmlTuple template;
        FedTaskTuple taskTup = new FedTaskTuple();
        //I use the new query I created in FedTaskTuple. This determines which tasks will be available to me.
        property = this.getProperty("subscription","*");
        String[] info = property.split(",");
        tupleTag = info[0];
        
        if(tupleTag.equals("REQ")){  
            associatedPort = info[1];
            taskType = info[2];  
        }
        else if(tupleTag.equals("CPT")){
            taskType = info[1];
            if(info.length == 3){
                if(info[2].equals("SINGLE"))
                    isSingle = true;
            }
        }
        else{
            taskType = "ERR";
        }
        String[] temp = this.getPeerIP().substring(2).split(":");
        localIP = temp[0];
        localPort = temp[1];
        
        FedLog.writeLog(this.getPeerIP().split(":")[1],"workerLog%g-"+this.getPeerIP().substring(2)+".log", "I am " +this.getWorkerid() + " looking for: " + tupleTag +" + "+taskType,"info");
        if(!isSingle)
            //template = taskTup.getQuery(tupleTag, taskType, localIP, localPort,location);  
            template = taskTup.getQuery(tupleTag, taskType, localIP, localPort,"*");
        else
            //template = taskTup.getQuery(tupleTag, taskType, defaultIP, defaultPort,location);
            template = taskTup.getQuery(tupleTag, taskType, defaultIP, defaultPort,"*");
        return template;
    }
    
    public double computeBidPrice(double _cost,double _benefit){
        double result;
        if(_benefit > _cost){
            result = _benefit;
            //totalCurrency += result;
        }
        else{
            result = -1;
        }
        return result;
    } 
    
    public class computeTaskRequest implements Runnable{
        Socket cSocket;
        DataOutputStream output;
        DataInputStream input;
                
        @Override
        public void run() {
            boolean FLAG = true;
            try {
                serverSocket = new ServerSocket(Integer.parseInt(recvTaskFromAppMasterPort));
                serverSocket.setReuseAddress(true);
                
                while (FLAG) {
                    //accpet appMaster connection request for sending back results
                    cSocket = serverSocket.accept();

                    input = new DataInputStream(cSocket.getInputStream());
                    output = new DataOutputStream(cSocket.getOutputStream());

                    String result = input.readUTF();
                    String[] idResult = result.split(":");
                    Integer currentTaskID = Integer.valueOf(idResult[0]);
                    
                    //String resultValue = idResult[1];
                    String[] objValue = idResult[1].split("\\?");
                    String resultValue = objValue[0];
                    String msg = objValue[1];
                    
//                    System.out.println("Return task id: "+currentTaskID+" to "+FedMeteorSubscriberWorker.this.getPeerIP());

                    //not necessary any more
                    setResultToTaskList(currentTaskID, resultValue);

                    //get task from list using id
                    Task currentTask = getTaskFromList(currentTaskID);

                    //get tasktuple and start time using taskid
                    FedTaskTuple taskTup = currentTask.getTaskTuple();
                    long stime = currentTask.getStartTime();
                    long etime = System.currentTimeMillis();

                    //String msg = "task " + currentTaskID + " " + (etime - stime);
                    String ip = FedMeteorSubscriberWorker.this.getPeerIP();

                    //create the appropriate result
                    String returnResult = "FIN," + taskTup.gettaskType() + "," + resultValue + ","
                            + ip.substring(2).replaceFirst(":", ",") + "," + taskTup.getOptID() +",1";

                    //send the calculated result back to the master using TCP
                    sendResultToMaster(taskTup.getTaskid(), returnResult, msg, taskTup.getMasterName());
                    
                    //TODO: update info system based on ( num of simulation executed / num of simulation promised to execute)
                    //out.writeUTF("ScenarioUpdate-"+taskType+"-"+deadline+"-"+origin+"-"+timestamp);
                    int numSimulationFinished = msg.split(";").length;
                    //scenarioOpt(taskTup.getDeadline(),taskTup.getTimestamp(),taskTup.getLocation(),taskTup.gettaskType(),
                            //numSimulationFinished,currentTaskID,"ScenarioUpdate","REPUTATION");

                    //update total currency
                    //updateBalance(taskTup.getTaskid());

                    //remove task from list
                    removeTaskFromList(currentTaskID);

                    output.writeUTF("Result received!");
                    output.flush();

                    input.close();
                    output.close();
                    cSocket.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(FedMeteorSubscriberWorker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    public class Task{
        
        FedTaskTuple tuple;
        long startTime;
        Object result;
        double cost;
        
        public Task(FedTaskTuple _tuple, long _stime, double _cost){
            tuple = _tuple;
            startTime = _stime;
            cost = _cost;
            result = null;
        }
        
        public FedTaskTuple getTaskTuple(){
            return tuple;
        }
        public long getStartTime(){
            return startTime;
        }
        public Object getResult(){
            return result;
        }     
        public double getCost() {
            return cost;
        }
        public void setResult(Object result) {
            this.result = result;
        }
        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }
        public void setTuple(FedTaskTuple tuple) {
            this.tuple = tuple;
        }
    }
}
