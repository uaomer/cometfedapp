/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tassl.automate.federation;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tassl.automate.comet.CometConstants;
import tassl.automate.comet.XmlTuple;
import tassl.automate.comet.xmltuplespace.XmlTupleService;
import static tassl.automate.federation.C4CHelper.message;
import tassl.automate.federation.server.EventClient;
import tassl.automate.federation.server.EventServer;
import tassl.automate.federation.server.EventSocket;
import tassl.automate.federation.server.EventSocket1;
import tassl.automate.federation.simpleapi.FedMeteorGenericMaster;
import tassl.automate.util.FedLog;



/**
 *
 * @author Ioan Petri
 */
public class FedMeteorPublisherMaster extends FedMeteorGenericMaster {

    private int baseTaskID = 0;

    private ServerSocket sSocket = null;
    private Socket cSocket = null;
    private boolean FLAG = true;
    private int serverPort;    
    private int PROC_MAX;
    
    private int optID = 0;
    private String managerInfoServicePort;
    private String location = "";
    
    Boolean winnerTakeAll; // if set to true, master will choose one winner from auction and send all tasks there
                            //if set to false, master will split the tasks to all bidders considering their bid
    
    private FedInfoSystem infoSys;
    
    private final Object lock = new Object();
    
    Map<Integer, ConcurrentHashMap<String,Double>> bidList;
    Map<Integer, List> resultList;
    Map<Integer, List> paramList;
    Map<String, String> associateList;
    Map<String,List> subscriberList;
    int totalnobiding=0;
    int totalRejectedJobs = 0;
    int totalAcceptedJobs = 0;
    
    //only for energyplus:DCCC-SIGCOMM simplicity
    Map<Integer,Integer> jobDistributionMap;
    Map<Integer,Integer> copyJobDistributionMap;
    
    String energyplusPropertyFile = "";
    String loginMachine = "mz228@green.rutgers.edu";
    //model coming from remote
    
    
    
    public int getPROC_MAX() {
        return PROC_MAX;
    }

    public int getServerPort() {
        return serverPort;
    }
    /**
     * This method add an new entry into paraList which stores parameters needed 
     * for create task tuple.
     */
    public void insertIntoParaList(Integer _taskID,Integer _optID, String _tupleTag, String _taskType, String _info1, String _info2, String _destIP,
            String _port,String _location, long _timestamp, Object _appSpecific) {
      
        List entry = new ArrayList();
        entry.add(_optID);
        entry.add(_tupleTag);
        entry.add(_taskType);
        entry.add(_info1);
        entry.add(_info2);
        entry.add(_destIP);
        entry.add(_port);
        entry.add(_location);
        entry.add(_timestamp); 
        entry.add(_appSpecific);
        synchronized (lock) {
            paramList.put(_taskID, entry);
        }        
        //System.out.println("Parameter list: "+paramList);
    }
    
    public void setInfoServicePort(String port){
        managerInfoServicePort=port;
    }
    
    /**
     * Initiate an information system here, including site:subscription list,
     * inside of the subscription list, it includes the tupleTag which defines 
     * the type of the message and taskType which defines the type of tasks that 
     * the site is subscribing.
     */
    public void initInfoSystem(){
        infoSys = new FedInfoSystem();
        subscriberList = new ConcurrentHashMap<String, List>();
        //singleCptNodes = new ArrayList<String>();
        //infoServiceRequest("CPT-UPDATE", null);
        //infoServiceRequest("REQ-UPDATE", null);
    }
    
    public void initDistributionSystem(){
        bidList = new ConcurrentHashMap<Integer, ConcurrentHashMap<String,Double>>();
        resultList = new ConcurrentHashMap<Integer, List>();
        paramList = new ConcurrentHashMap<Integer, List>();
    }
    
    public void removeFromResultList(int currentOptID){
        //System.out.println("remove resultList before: "+resultList);
        if(resultList.containsKey(currentOptID)){
            resultList.remove(currentOptID);
        } 
    }
    /**
     * Connect to infoService server to do operations such as get the up-to-date subscription list
     * or send poison pill request.
     */
    public void infoServiceRequest(String requestType, String param){
 
        InetAddress destAddr = null;
        String[] nodeInfo = null;
        String ip = this.getPeerIP().split(":")[0].substring(2);
        try {
            destAddr = InetAddress.getByName(ip);
        } catch (UnknownHostException ex) {
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(ex), "severe");
        }
        int destPort = Integer.parseInt(managerInfoServicePort);
        
        DataOutputStream output;
        DataInputStream input;
        
        Socket sock = null;
        boolean connected = false;
        while (!connected) {
            try {
                sock = new Socket(destAddr, destPort);
                connected = true;
            } catch (IOException ex) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex1) {
                    FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(ex1), "severe");
                }
            }
        }
        try {
            output = new DataOutputStream(sock.getOutputStream());
            input = new DataInputStream(sock.getInputStream());       
            
            if(requestType.equals("UPDATE")){
                output.writeUTF(requestType+"-LOCAL");
                output.flush();
                String infoServiceList = input.readUTF();
                infoSys.updateInfo(infoServiceList);
            }
            else if(requestType.equals("POISON")){
                output.writeUTF(requestType+"-LOCAL-"+param);
                output.flush();
                infoSys.removeInfo(param);
            }
            else if(requestType.equals("ADD")){
                output.writeUTF(requestType+"-LOCAL-"+param);
                output.flush();
                String infoServiceList = input.readUTF();
                //update local information service list
                infoSys.updateInfo(infoServiceList);
            }
            else if(requestType.equals("CPT-UPDATE")){
                output.writeUTF(requestType);
                output.flush();
                String cptNode = input.readUTF();
                String[] cptNodes = cptNode.split(", ");
                List singleCptList = new ArrayList<String>();
                for(int i = 0 ; i < cptNodes.length; ++i){
                    singleCptList.add(cptNodes[i].substring(1,cptNodes[i].indexOf("]")));
                }
                updateSubscriberList("CPT",singleCptList); 
            }
            else if(requestType.equals("CPT-DELETE")){
                output.writeUTF(requestType+"-"+param);
                output.flush();
                subscriberList.get("CPT").remove(param);
            }
            else if(requestType.equals("REQ-UPDATE")){
                output.writeUTF(requestType);
                output.flush();
                String reqNode = input.readUTF();
                String[] reqNodes = reqNode.split(", ");
                List reqList = new ArrayList<String>();
                for(int i = 0 ; i < reqNodes.length; ++i){
                    reqList.add(reqNodes[i].substring(1,reqNodes[i].indexOf("]")));
                }
                updateSubscriberList("REQ",reqList); 
            }
            else if(requestType.equals("REQ-DELETE")){
                output.writeUTF(requestType+"-"+param);
                output.flush();
                subscriberList.get("REQ").remove(param);
            }
            output.close();
            input.close();
            sock.close();
        } catch (IOException ex) {
             FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(ex), "severe");
        }
    }
    /**
     * Update CPT and REQ list 
     */
    public void updateSubscriberList(String subtype,List subList){
        if (subscriberList.containsKey(subtype)) {
            subscriberList.get(subtype).addAll(subList);
        } else {
            subscriberList.put(subtype, subList);
        }
    }

    /**
     * This method extracts app properties from app.properties file
     * and starts server.
     */
    @Override
    public void generateTasks() {
        int tasks = Integer.parseInt(this.getProperty("numTask", "1000"));
        setNumOfTasks(tasks);
        //TODO:fixed port here !!!! need to change!!!
        serverPort = Integer.parseInt(this.getProperty("reqServerPort", "5560"));         
        PROC_MAX = Integer.parseInt(this.getProperty("reqServerMaxConcurrentClients","10"));
        location = this.getProperty("location", "rutgers");
        energyplusPropertyFile = this.getProperty("energyplusTemplate", "energyplus.property");
        
        winnerTakeAll = Boolean.parseBoolean(this.getProperty("winnerTakeAll","true"));
        jobDistributionMap = new HashMap<Integer,Integer>();
        copyJobDistributionMap = new HashMap<Integer,Integer>();
        //System.out.println("energyplusPropertyFile:"+energyplusPropertyFile);
        
        initInfoSystem();
        initDistributionSystem();
        startFedServer();
    }
    
    /**
     * This method starts server to receive client connection and generate tasks
     * upon requests, There is a maximum number of threads allowed.
     */
    private void startFedServer(){
            
        try {
            System.out.println("Start server to listen for client connection....");
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", "Start server to listen for client connection....", "info");
            sSocket = new ServerSocket(serverPort);
            sSocket.setReuseAddress(true);
            //System.out.println("FedServer is launching");
        } catch (IOException ex) {
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", "Error listening on port " + serverPort, "severe");
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(ex), "severe");
        }
        List<Thread> proc_list = new ArrayList();
        while (FLAG) {
            if (proc_list.size() == PROC_MAX) {
                boolean full = true;
                //check if thread number reach the maximum number, if yes, sleep; 
                while (full) {
                    for (int i = proc_list.size() - 1; i >= 0; i--) {
                        if (!proc_list.get(i).isAlive()) {
                            proc_list.remove(i);
                            full = false;
                        }
                    }
                    if (full) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(ex), "severe");
                        }
                    }
                }
            }
            //if not full, accpet new client connection
            try {
                    cSocket = sSocket.accept();
                    //System.out.println("[C4C-Notification] Accept connection from client");
                    FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", "Accept connection from client", "info");
                    //create new thread to process client request
                    // System.out.println("Accept connection from client -step 1");
                    Thread request = new Thread(new processRequest(cSocket));
                    request.start();
                    proc_list.add(request);
                 } catch (IOException ex) {
                FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(ex), "severe");
            }

        }
        closeFedSockets();
    }
    /**
     * Update baseTaskID and return the value to a temporary ID to be parameter.
     * @return 
     */
    public int getUpdatedTaskID(){
        int resultID;
        synchronized (lock) {
            ++baseTaskID;
            resultID = baseTaskID;
            //insert new parameters to paraList
        }
        return resultID;
    }
   
    /**
    * This method is called to read the application properties and initiate the
    * tasks creation.
    */
    public void createTasks(Integer _optID, String _tupleTag, String _taskType, String _info1, String _info2, String _destIP, String _port, String _location, long timestamp, Object _appSpecific){ 
        //System.out.print("Create tasks");
        boolean flag = false;
        String defaultIP = "-1";
        String defaultPort = "-1";
        
        //use wildcard to let one worker randomly choose the job to run
        //System.out.println("---What is optID: " +_optID);
        if(_destIP.equals("RANDOM")){
            int tempTaskID = getUpdatedTaskID();
           
            insertIntoParaList(tempTaskID, _optID, _tupleTag, _taskType,defaultIP,_info1,_info2,defaultPort,_location, timestamp,_appSpecific);
            flag = true;
            addEntryToResultList(_optID,flag);
            //------THE TASK IS INSERTED HERE
            insertTask(tempTaskID);
        }
        //send tuples to all
        else if(_destIP.equals("ALL")){
            //Iterate through all sites that subscribe to certain types of task  
            //System.out.println("infoSys.getSiteList(): "+infoSys.getSiteList());
            Iterator it = infoSys.getSiteList().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                _destIP = ((String) entry.getKey());
                ConcurrentHashMap<String, String> subscribeType = (ConcurrentHashMap<String, String>) entry.getValue();
                
                //option 2, if there is multiple subscribers of a certain taskType in one site
                Iterator subIt = subscribeType.entrySet().iterator();
                while (subIt.hasNext()) {
                    Map.Entry subEntry = (Map.Entry) subIt.next();
                    String[] typeAndAssociate = subEntry.getValue().toString().split(",");
                    if (typeAndAssociate[0].equals(_tupleTag) && typeAndAssociate[1].equals(_taskType)) {
                        _port = subEntry.getKey().toString();

                        int tempTaskID = getUpdatedTaskID();
                        insertIntoParaList(tempTaskID, _optID, _tupleTag, _taskType,_destIP, _info1,_info2,_port,_location, timestamp,_appSpecific);
                        //System.out.println("Master out: "+"--"+_tupleTag+"--"+_taskType+"--"+_optID+"--"+_destIP+"--"+_budget+"--"+_deadline+"--"+_port);
                        flag = true;
                        addEntryToResultList(_optID,flag);
                        //------THE TASK IS INSERTED HERE
                        insertTask(tempTaskID);
                        
                    }
                }
            }
        }
        //if user choose one specific worker, only send the message to it
        else{ 
            //System.out.println(infoSys.getSiteList());
            //Check if this subscriber exists
            if (infoSys.hasAssociateSubscriber(_destIP, _port)) {
                int tempTaskID = getUpdatedTaskID();
                //System.out.println("Master out: "+"--"+_tupleTag+"--"+_taskType+"--"+_optID+"--"+_destIP+"--"+_budget+"--"+_deadline+"--"+_port);
                insertIntoParaList(tempTaskID, _optID, _tupleTag, _taskType,_destIP,_info1,_info2,_port,_location, timestamp,_appSpecific);
                flag = true;
                addEntryToResultList(_optID,flag);
                insertTask(tempTaskID);
            } else {
                System.out.println("Incorrect destIP or destPort for creating task!");
            }
        }        
        //create entry in resultList
        addEntryToResultList(_optID,flag);
    }
    
    public void addEntryToResultList(int _optID, boolean flag){
        if(resultList.containsKey(_optID))
            return;
        List temp = new ArrayList();
        if (!flag) {
            temp.add(Boolean.valueOf(true));
            temp.add(Double.valueOf(-1));
        } else {
            temp.add(Boolean.valueOf(false));
            temp.add(Double.valueOf(0));
        }
        temp.add("NULL");
        temp.add("NULL");
        synchronized(lock){
            resultList.put(_optID, temp);
            //System.out.println("In createTask: "+resultList);
        }
    }

    /**
     * This method is called from insertTask to create the data associated to
     * the task
     *
     * @param taskid Task identifier that can be use as index to create its
     * data.
     * @return Object input data that will be attached to the task. The object
     * has to be serializable.
     */
    
    // 
    @Override
    @SuppressWarnings("empty-statement")
    public Object createTaskData(int taskid) {        
        List curPara = new ArrayList();
        ArrayList<String[]> regArgs1=new ArrayList<String[]>();
        String [] regArgs2 = null;
        synchronized(lock){
            curPara = paramList.get(taskid); 
        }
        
        String _taskType = curPara.get(2).toString();
        String _info1 = curPara.get(4).toString();
        String _info2 = curPara.get(5).toString();
        if(_taskType.equals("c4c")){ 
            String _tupleTag = curPara.get(1).toString();
            HashMap<String,Object> requestArgs ;
            if (_info2.equals("MET")) {
                try {
                    Thread.sleep(100);
                    System.out.println("[Notification-createTaskData] metadata array to send "+C4CHelper.regArgs2.size());
                    Thread.sleep(100);
                    regArgs1=C4CHelper.regArgs2;
                    Thread.sleep(10000);
                    System.out.println("[Notification-createTaskData] metadata completed");
                    C4CHelper.uploadModel=true;
                 } catch (Exception ex) {
                    Logger.getLogger(FedMeteorPublisherMaster.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if (_info2.equals("VER")){
                try {
                    Thread.sleep(100);
                    System.out.println("[Notification-createTaskData] version array to send "+C4CHelper.regArgs1.size());
                    Thread.sleep(100);
                    regArgs1=C4CHelper.regArgs1;
                    Thread.sleep(10000);
                    System.out.println("[Notification-createTaskData] version completed");
                } catch (InterruptedException ex) {
                    Logger.getLogger(FedMeteorPublisherMaster.class.getName()).log(Level.SEVERE, null, ex);
                }
             } else if (_info1.equals("CONFIG")){
                try {
                    ArrayList<String[]> result=new ArrayList<String[]>();
                    String[] res=new String[10];
                    System.out.println("[Notification-createTaskData] config array to send: "+C4CHelper.resultConfig.length());
                    Thread.sleep(100);
                    Pattern pattern = Pattern.compile("<IPAddress>(.*?)</IPAddress>");
                    Pattern pattern2 = Pattern.compile("<Port>(.*?)</Port>");
                    Matcher matcher = pattern.matcher(C4CHelper.resultConfig);
                    Matcher matcher2 = pattern2.matcher(C4CHelper.resultConfig);
                    while ((matcher.find()) && (matcher2.find())){
                        res[0]=matcher.group(1);
                        res[1]=matcher2.group(1);
                        //System.out.println("[Notification-createTaskData] 1111 config completed "+res[0] +"-"+res[1]);
                        result.add(new String[] {res[0],res[1]});
                        
                    } 
                    regArgs1=result;
                    //System.out.println("[Socket getInterfaceData] configuring ...end "+result.get(0)[0]);
                    //System.out.println("[Socket getInterfaceData] configuring ...end "+result.get(0)[1]);
                    System.out.println("[Notification-createTaskData] config completed");
        
                } catch (InterruptedException ex) {
                    Logger.getLogger(FedMeteorPublisherMaster.class.getName()).log(Level.SEVERE, null, ex);
                }   
            } else { 
                
                System.out.println("[Notification-createTaskData] fetch received");
            }
            return (Object) regArgs1;
        
        }else if(_taskType.equals("C4C")){
            String _tupleTag = curPara.get(1).toString();
            HashMap<String,Object> requestArgs ;
            if(_tupleTag.equals("CPT")){
                requestArgs = (HashMap<String,Object>)curPara.get(9);
            }else{
                regArgs1 = null;
            }
            return (Object) regArgs1;
        }else{
            return null;
        }             
    }
    
    /**
     * This method is called when a worker returns a result
     *
     * @param taskid Task identifier that can be use as index to create its
     * data.
     * @param data Object with the returned data
     * @param message Additional information send by the worker
     * @param sender Name of the worker
     */
    @Override
    public boolean setResultSpecific(int taskid, Object data, String message, String sender) {
        String token="c4csecretkey";
        boolean flag = false;   
        String[] msg = message.split(",");
        HashMap<String,String> model=(HashMap<String,String>)data;
        //System.out.println("[C4C-REPLY] response received with: "+msg[0]+ "--"+msg[1]+"--"+msg[2]);
        String tupleTag = msg[0];
        String taskType = msg[1];
        //double result = Double.valueOf(msg[2]);
        String localdisc=null;
        String port = msg[3];
        String discipline =msg[msg.length-1];
        if(tupleTag.equals("FETCH")){
           synchronized(lock){
                if(taskType.equals("c4c")){                    
                      //----PETRI
                         System.out.println("[C4C-Notification] Received response from "+"" + 
                                 "discipline "+discipline  );
                        try {
                           localdisc=C4CHelper.getDiscipline(token);
                           if (!localdisc.equals(discipline)){ 
                                //System.out.println("[Notification-Reciever] Received "+model.size()+" objects from discipline "+discipline);
                                C4CHelper.models.add(model); 
                                C4CHelper.modelNames.add(discipline.substring(0,1));
                                if (C4CHelper.models.size()>=3) {
                                    System.out.println("[Notification-Reciever] Merging Models");
                                    C4CHelper.finalModels = new ArrayList<HashMap<String,String>>();
                                    C4CHelper.finalModels=C4CHelper.models;
                                    //System.out.println("[C4C-FECTH] merge model activated!");
                              
                            } 
                            }
                     } catch (Exception ex) {
                         ex.printStackTrace();
                            Logger.getLogger(FedMeteorPublisherMaster.class.getName()).log(Level.SEVERE, null, ex);
                        }
              
                
                }
                //end change way to decide winner in auction
            }
            //long stime = System.currentTimeMillis();
            System.out.println("[C4C-Notification] Fetch process finished!");
            
        } else if(tupleTag.contains("EVENT")){
            //long stime = System.currentTimeMillis();
            System.out.println("[C4C-Notification] Event process finished!");
                       
        } else if(tupleTag.contains("CONFIG")){
            //long stime = System.currentTimeMillis();
            System.out.println("[C4C-Notification] Config process finished!");
        }
       
        else{
            System.out.println("Result message type not found!");
        }
        
        //Set task as done and increment completed tasks
        setTaskStatus(taskid);        

        //System.out.println("Result taskid=" + taskid + " received " + countFinishedTasks() + "/" + getNumOfTasks());
        FedLog.writeLog(this.getPeerIP().split(":")[1],"masterLog%g-"+this.getPeerIP().substring(2)+".log","Result taskid=" + taskid + " received " + countFinishedTasks() + "/" + getNumOfTasks(),"info");  
        //System.out.println("ResultList: "+ resultList);
        //check if it gets all results       
       
        return true;
    }

    //Method override due to the change in the TaskTuple. This method is called inside insertTasks
    @Override
    public XmlTuple createTaskTuple(int taskid){
        XmlTuple task = new XmlTupleService();
        String taskstr = null;
        FedTaskTuple tasktuple = new FedTaskTuple();
        //taskstr = tasktuple.setTaskStr(taskid, masterNetName, value);
        List curPara = new ArrayList();
        synchronized(lock){
            curPara = paramList.get(taskid); 
        }
        //System.out.println("createTaskTuple: "+((Integer)curPara.get(0)).intValue()+" "+curPara.get(1).toString()+" "+ curPara.get(2).toString() +" "+curPara.get(3).toString()
        //       +" "+(Double)curPara.get(4)+" "+((Double)curPara.get(5)).doubleValue() + curPara.get(6).toString()+curPara.get(7).toString()+(Long)curPara.get(8));
        taskstr = tasktuple.setTaskStr(taskid, masterNetName, 
                ((Integer)curPara.get(0)).intValue(), curPara.get(1).toString(), curPara.get(2).toString(), curPara.get(3).toString(), 
                curPara.get(4).toString(), curPara.get(5).toString(), curPara.get(6).toString(),curPara.get(7).toString(), (Long)curPara.get(8));
        //System.out.println("createTaskTuple: " +taskstr);
        //taskstr = tasktuple.setTaskStr(taskid, masterNetName, _optID,tupleTag,taskType,destIP,cost,benefit);
        task.createXMLtuple(taskstr);
        
        return task;
    }

    //Method override due to the change in the TaskTuple
    //this method is to create poison pills to tempListkill workes. I could define value depending of the id
    //if I know that there are certain nodes waiting for a specific value.
    @Override
    public XmlTuple createPoisonTaskTuple(int id) {
        XmlTuple task = new XmlTupleService();
        FedTaskTuple tasktuple = new FedTaskTuple();
        
        String valuepoison;

        if (id % 2 == 0) {
            valuepoison = "red";
        } else {
            valuepoison = "black";
        }
        task.createXMLtuple(tasktuple.createPoisonpillTag(masterNetName, id, valuepoison));
        return task;
    }
    @Override
    public XmlTuple createPoisonTaskTuple(int id, String destIP, String port){
        XmlTuple task = new XmlTupleService();
        FedTaskTuple tasktuple = new FedTaskTuple();
        String[] subscribeType;
        if(infoSys.getSiteSubscribe(destIP) != null){
            if(infoSys.getSiteSubscribe(destIP).containsKey(port)){
                subscribeType = infoSys.getSiteSubscribe(destIP).get(port).split(",");               
            }
            else{
                subscribeType = infoSys.getAssociateType(destIP, port).split(",");
            }
            task.createXMLtuple(tasktuple.createPoisonpillTag(masterNetName, id, subscribeType[0],subscribeType[1],destIP,port));
        }
        else{
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", "CreatePoisonTaskTuple Failed!", "info");
        }
        return task;
    }
    /**
     * Close client and server sockets.
     */
    private void closeFedSockets() {
        try {
            cSocket.close();
        } catch (Exception e) {
            if (!FLAG) {
                System.err.println("Error closing Client Socket");
            }
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(e), "severe");
        }
        try {
            sSocket.close();
        } catch (Exception e) {
            if (!FLAG) {
                System.err.println("Error closing Server Socket");
            }
            FedLog.writeLog(this.getPeerIP().split(":")[1], "masterLog%g-" + this.getPeerIP().substring(2) + ".log", FedLog.getExceptionStack(e), "severe");
        }
    }
        
    public void quitFedSocket() {
            FLAG = false;
           // closeFedSockets();
    }
    /**
     * Count the number of available subscribers for a certain type of task.
     * @param tupleTag
     * @param taskType
     * @return 
     */
    public int availableSubscriber(String tupleTag, String taskType){
        int count = 0;
        if(subscriberList.containsKey(tupleTag)){
            List subscribers = subscriberList.get(tupleTag);
            for(int i = 0 ; i < subscribers.size(); ++i){
                String[] info = subscribers.get(i).toString().split(":");
                String[] typeInfo = info[1].split(",");
                if(typeInfo[0].equals(tupleTag) && typeInfo[1].equals(taskType)){
                    count++;
                }
            }
        }
        return count;
    }
    
    public FedInfoSystem getInfoSys(){
        return infoSys;
    }
    /**
     * Federation site should be able to decide where the destination location is.
     * @return 
     */
    public String getLocation(){
        return location;
    }
    /**
     * This class implements the real work of task generating.
     * 
     * @author Mengsong Zou
     */
    public class processRequest implements Runnable{
        Socket cSocket;
        
        public processRequest(Socket clientSocket){
            cSocket = clientSocket;
        }
        
        @Override
        public void run(){
            String message = "";
            String _tupleTag, _taskType, _destIP,_subSite,_port,_numAdd,_location,_appSpecific;
            long _timestamp;
            
            Integer _optID;
            
            DataInputStream input;
            DataOutputStream output;
            
            
            
            try {
                input = new DataInputStream(cSocket.getInputStream());
                output = new DataOutputStream(cSocket.getOutputStream());
                
                boolean more = true;
                while(more){
                    _port = "";
                    String temp="REQ,c4c,null";
                    message = input.readUTF();
                   
                    //PETRI -- to modify the client
                    //System.out.println("Client sent request: " + message);
                    String[] inMsg = temp.split(",");
                    String[] inMsgClient = message.split(",");
                    //System.out.println("[C4C-Notification] Client sent request: " + inMsgClient[3]);
                    
                    //update subscription list before creating REQ task     
                    infoServiceRequest("UPDATE", null);
                   
                    if(inMsg[0].equals("ADD")){
                            _tupleTag = inMsg[1];
                            _taskType = inMsg[2]; 
                            _numAdd = inMsg[3];
                            infoServiceRequest("ADD",_tupleTag+","+_taskType+","+_numAdd);  
                            output.writeUTF("ADD,Add "+_numAdd+" nodes success!" + "," + _tupleTag + "," + _taskType);
                            more = false;
                    }
                    else if(inMsg[0].equals("END")){
                        String associatePort = "";
                        String portSubscribe = "";      //port:cpt,red
                        System.out.println("Terminate site");
                        _tupleTag = inMsg[1];
                        _taskType = inMsg[2];
                        _destIP = masterNetName;
                        
                        //get all subscribers of this type
                        _port = infoSys.getSubscriberByTypeInSite(_destIP,_tupleTag,_taskType);  
                       
                        //remove worker(s) from overlay network
                        if(!_port.isEmpty()){
                            //System.out.println(_destIP+" "+_port);
                            //remove workers from information service list both locally and remotelly
                            infoServiceRequest("POISON",_destIP+","+_port);
                            //infoServiceRequest("CPT-DELETE",portSubscribe);
                            output.writeUTF("DEL,Success delete nodes!" + "," + _destIP + "," + _port);
                        }
                        else{
                            output.writeUTF("ERR,No such nodes exist to delete!," + _destIP + ", NULL");
                        }  
                        more = false;
                        //System.out.println("FedMeteorPublisherMaster: after POISON  "+infoSys.getSiteList());   
                    }
                    else{
                        _tupleTag = inMsg[0];
                        _taskType = inMsg[1];
                        _optID = -1;
                        _location = getLocation();
                        _timestamp = 0; 
                        _appSpecific = "c4c";
                        //Syncronize this with the client
                        String _info1 =inMsgClient[3]; // System.out.println("--xxxx Info1: " + inMsgClient[3]);
                        String _info2=inMsgClient[4]; //System.out.println("--xxxx Info2: " + inMsgClient[4]);
                      
                        //if there exist any worker to do the job  
                        if (infoSys.numSubscribersForType(_tupleTag, _taskType,inMsg) > 0){
                            if (_tupleTag.equals("REQ")) { 
                                System.out.println("[C4C-Notification] Start with message tag: " + _info1 + " and discipline " +_info2);
                                _optID = getUpdatedOptID();
                                createTasks(_optID, _tupleTag, _taskType, _info1, _info2,"ALL", "ALL",_location,_timestamp,_appSpecific);      
                                //System.out.println("----0.Master YESSS-- got here:"+_info2);
                               // EventServer call=new EventServer();
                                //System.err.println("+++++Server");
                                
                                
                            } 
                            
                            more = false;
                        }
                        else{
                            output.writeUTF("ERR,No worker for this task type now!Try later!,NULL,NULL");
                            more = false;
                        }
                    }
                    output.flush();
                }
    
                input.close();
                output.close();
                cSocket.close();
                
            } catch (IOException ex) {
                FedLog.writeLog(FedMeteorPublisherMaster.this.getPeerIP().split(":")[1],"masterLog%g-"+FedMeteorPublisherMaster.this.getPeerIP().substring(2)+".log",FedLog.getExceptionStack(ex),"severe");
            }
        }
        
        public Integer getUpdatedOptID(){
            Integer newOptID;
            synchronized (lock) {
                ++optID;
                newOptID = Integer.valueOf(optID);
            }
            return newOptID;
        }
        
        public boolean checkTaskFinished(int _optID){
            if(resultList.get(_optID).get(0).equals(Boolean.valueOf(true))){
                return true;
            }
            else
                return false;
         
        }
        
    }
}

