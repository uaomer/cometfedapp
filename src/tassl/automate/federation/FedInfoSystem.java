
package tassl.automate.federation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.io.FileHandler;
import tassl.automate.util.FedLog;

/**
 *
 * @author Ioan Petri to update
 */
public class FedInfoSystem {
    
    private ConcurrentHashMap<String, ConcurrentHashMap<String,String>> siteSubscribeList;
    
    //add completion time of each task to workers
    String siteIP ;
    List <Double>localWorkers;
    List <Double>extWorkers;
    HashMap <String,Double>estimatedCompletion;
    HashMap <String,Double>costTasks;
    int numLocalworkers;
    int numExtWorkers;
    double costWorker;
    double costSubscription;
    double benefitLocal;
    double benefitExt;
    double transactionOntime;
    double transactionOfftime;
    
    double TotalReputation;
    double TotalIncome;
    double TotalCost;
    int TotalCompletedSimulation = 0;
    int TotalIncompleteSimulation = 0;
    double TotalCompletionTime = 0.0;
    List <Double>scores;
    
    HashMap<Integer,Integer> estimatedSimulationNum;
    
    HashMap<String,Double> subscriberBalance;
    //keep a list of subscriptions status
    Map subscriptionStatus;        
    
    boolean staticSubscriptions = true; //in app.properties true is static, false is dynamic
    
    Properties properties;
    public FedInfoSystem(){  
        siteSubscribeList = new ConcurrentHashMap<String, ConcurrentHashMap<String,String>>();         
    }
    
    public void loadConfig(){
        estimatedSimulationNum = new HashMap();
        localWorkers=new ArrayList();
        extWorkers=new ArrayList();
        estimatedCompletion=new HashMap();
        costTasks=new HashMap();
        scores=new ArrayList();
        numLocalworkers=Integer.parseInt(properties.getProperty("localWorkers"));
        numExtWorkers=Integer.parseInt(properties.getProperty("extWorkers"));        
        costWorker=Double.parseDouble(properties.getProperty("costEachWorker"));
        benefitLocal=Double.parseDouble(properties.getProperty("benefitLocal"));
        benefitExt=Double.parseDouble(properties.getProperty("benefitExt"));
        transactionOntime=Double.parseDouble(properties.getProperty("transactionOnTime"));
        transactionOfftime=Double.parseDouble(properties.getProperty("transactionOffTime"));
        costSubscription=0; //WE do not use it for now
        
        staticSubscriptions = Boolean.parseBoolean(properties.getProperty("staticSubscriptions"));        
        subscriberBalance = new HashMap();
        subscriptionStatus = new HashMap();
        
        for(int i=0;i<numLocalworkers;i++){
            localWorkers.add(0.0);
        }
        for(int i=0;i<numExtWorkers;i++){
            extWorkers.add(0.0);
        }
        String []estComp=properties.getProperty("estimatedCompletion").split(";");        
        for (String i:estComp){      
            String []part=i.split(":");
            estimatedCompletion.put(part[0], Double.parseDouble(part[1]));
        }
        String []costtasks=properties.getProperty("costTasks").split(";");        
        for (String i:costtasks){      
            String []part=i.split(":");
            costTasks.put(part[0], Double.parseDouble(part[1]));
        }
        TotalReputation=0;
        TotalIncome=0.0;
        TotalCost=numLocalworkers*costWorker+numExtWorkers*costWorker+costSubscription;
        scores.add(1.0);
        
        //create a map of:  {subscription:status} e.g. {RED:false} inactive
        ConcurrentHashMap<String,String> tempMap = getSiteSubscribe(siteIP);
        
        Iterator it = tempMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String[] info = entry.getValue().toString().split(",");
            String appType = info[1];   //now assume no two subscribers subscribe to same type
            subscriptionStatus.put(appType, true);  //true is active
        }
    }

    public double getEstimatedCompletion(String type, int numtask){
        double ret = 0;
        if(type.equals("c4c")){
            ret = estimatedCompletion.get(type);
            /*
            //TODO: Randomly generate time for each task. This random number should fall between some range.
            double low = 34.0;      //min
            double high = 37.0;     //min
            for(int i = 0; i < numtask; ++i){
                Random randomGen = new Random(); 
                double n = randomGen.nextDouble();
                n += (high - low) * n + low;
                ret += n;
            }
            */
        }else if(type.equals("cancer")){
            ret = estimatedCompletion.get(type);
        }else{
            ret = estimatedCompletion.get(type);
        }    
        return ret;
    }
    /**
     * This method calculate how many round of simulation can be finished by local or external workers.
     * This depends on both number of available local/external workers and tasktype.
     * @param _deadline
     * @param _timestamp
     * @param _completeTimePerTask
     * @param taskType
     * @return 
     */
    public double calculateNumSimulation(Double _deadline, long _timestamp, double _completeTimePerTask, String workerType){
        double estimatedNum = 0.0;
        List<Double> workers;
        if(workerType.equals("localWorkers")){
            workers = localWorkers;
        }else{
            workers = extWorkers;
        }
        
        for (int i = 0; i < workers.size(); ++i) {
            double runtime = 0.0;     // the time that this worker can actually run the simulation
            if ((Double) workers.get(i) < _timestamp) {
                runtime = _deadline;
            } else {
                runtime = _deadline + _timestamp - (Double) workers.get(i);
                if (runtime <= 0) {
                    runtime = 0;
                }
            }
            estimatedNum += (int) (runtime / _completeTimePerTask);
        }
        //System.out.println("Estimated simulation numbers: "+estimatedNum);

        return estimatedNum;
    }
    
    public int getEstimatedNumSimulation(Double _deadline, long _timestamp, double _completeTimePerTask, String taskType ){
//        double timestamp = _timestamp.
        int estimatedNum = 0;
        if(taskType.equals("local")){
            //calculate how many round of simulation can local workers finish
            estimatedNum += calculateNumSimulation(_deadline,_timestamp,_completeTimePerTask,"localWorkers");
            
            //calculate how many round of simulation can external workers finish
            estimatedNum += calculateNumSimulation(_deadline,_timestamp,_completeTimePerTask,"extWorkers");   
            
        }else if(taskType.equals("external")){      //external tasks only calculate ext workers
            //calculate how many round of simulation can external workers(only) finish
            estimatedNum += calculateNumSimulation(_deadline,_timestamp,_completeTimePerTask,"extWorkers"); 
        }else{
            System.out.println("Invalid task type!");
        }        
        return estimatedNum;
    }
    /**
     * Not just update the local/ext workers lists, also return the estimated simulation number.
     * @param completeTimePerTask
     * @param deadline
     * @param timestamp
     * @param workerType
     * @return scheduledSimulationNum
     */
    public String updateTaskCompleteTimeList(int numSimulation, Double completeTimePerTask, Double deadline, 
            long timestamp, String workerType){
        List<Double> workers;
        double totalTime = 0.0;
        if(workerType.equals("localWorkers")){
            workers = localWorkers;
        }else{
            workers = extWorkers;
        }
        //find completion times which ends before deadline+timestamp of incoming tasks
        //remove completion time of finished tasks and add new task completion times
        ListIterator<Double> workerIter = workers.listIterator();
        int scheduledSimulationNum = 0;
        double runtime = 0.0;
        while(workerIter.hasNext() && scheduledSimulationNum < numSimulation){            
            Double completeTime = (Double)workerIter.next();
            //System.out.println("completeTime: "+completeTime+" timestamp: "+timestamp+" deadline: "+deadline);
            double startTime = 0.0;
            if( completeTime < timestamp){
                runtime = deadline;
                startTime = timestamp;
            }else if(deadline + timestamp - completeTime >= completeTimePerTask){
                runtime = deadline + timestamp - completeTime;
                startTime = completeTime;
            }
            if(runtime != 0){
                int numPossibleTasks = (int)(runtime / completeTimePerTask);
                
                scheduledSimulationNum += numPossibleTasks;
                if( scheduledSimulationNum > numSimulation ){
                    numPossibleTasks -= (scheduledSimulationNum - numSimulation);
                    scheduledSimulationNum = numSimulation;
                }
                //workerIter.remove(); 
                //workerIter.add(timestamp + (numPossibleTasks * completeTimePerTask) );
                double newTime = startTime + (numPossibleTasks * completeTimePerTask);
                if(newTime > totalTime){
                    totalTime = newTime;
                }
                workerIter.set(newTime);
            }
        }
        Collections.sort(workers);
        //System.out.println("localWorkers: "+localWorkers);
        //System.out.println("extWorkers: "+extWorkers);
        //return the estimated number of total simulation
        //System.out.println("----Estimated numbers: "+scheduledSimulationNum+" TotalTime: "+totalTime + " timestamp: "+timestamp);
        return scheduledSimulationNum+":"+totalTime;
    }
    
    public String updateLocalWorkerC4C(String type, Double deadline, String origin, 
            long timestamp, int numSimulation, int taskid, String operationType) throws IOException{ 
         System.out.println("C4C -: all updates finished;"+origin+";"+"C4C"+";"+numSimulation);
        
    return "Object added";
    }

    public String updateLocalWorkerScoreCost(String type, Double deadline, String origin, 
            long timestamp, int numSimulation, int taskid, String operationType) throws IOException{ 
        String result = "";
        if(operationType.equals("TIMELIST")){
            int estimatedSimulationNumber = 0; 
            double totalTime;
            Double completeTimePerTask = getEstimatedCompletion(type,1);

            if (origin.equals("local")){
                String[] resultLocal = updateTaskCompleteTimeList(numSimulation, completeTimePerTask,deadline,timestamp,"localWorkers").split(":");
                estimatedSimulationNumber += Integer.parseInt(resultLocal[0]);
                if(estimatedSimulationNumber < numSimulation){
                    String[] resultExt = updateTaskCompleteTimeList(numSimulation-estimatedSimulationNumber,completeTimePerTask,deadline,timestamp,"extWorkers").split(":");                
                    estimatedSimulationNumber += Integer.parseInt(resultExt[0]);  
                    totalTime = Math.max(Double.parseDouble(resultLocal[1]), Double.parseDouble(resultExt[1])); 
                }else{
                    totalTime = Double.parseDouble(resultLocal[1]);
                }               
                
            }else{
                String[] resultExt = updateTaskCompleteTimeList(numSimulation, completeTimePerTask,deadline,timestamp,"extWorkers").split(":");
                estimatedSimulationNumber = Integer.parseInt(resultExt[0]);
                totalTime = Double.parseDouble(resultExt[1]);
            }
            estimatedSimulationNum.put(taskid, estimatedSimulationNumber);
            result += estimatedSimulationNumber+":"+totalTime;
            
        }else if(operationType.equals("REPUTATION")){
            //System.out.println("Estimated number: "+(Integer)estimatedSimulationNum.get(taskid)+",Real number:"+numSimulation);
            //change reputation based on estimated simulation number / acutal simulation number
            if(estimatedSimulationNum.containsKey(taskid)){
                int completedTaskNum = (Integer)estimatedSimulationNum.get(taskid);
                if(completedTaskNum == numSimulation){
                    // PETRI
                    //System.out.println("TASK: all simulations finished;"+origin+";"+type+";"+numSimulation);
                    System.out.println("C4C: all updates finished;"+origin+";"+"C4C"+";"+numSimulation);
                    scores.add(transactionOntime); 
                }else if(completedTaskNum < numSimulation){
                    System.out.println("TASK: some simulations not finished;"+origin+";"+type+";"+numSimulation);
                    TotalIncompleteSimulation += numSimulation - completedTaskNum ;
                    scores.add(transactionOfftime);
                }else{  //if workers finished more than estimated number of simulations, bonus!
                    System.out.println("TASK: more simulations are finished;"+origin+";"+type+";"+numSimulation);
                    //scores.add(transactionOntime*2);
                }
                double completeTime = deadline;//this deadline is not the actual deadline, it's the complete time
                
                calculateReputation();                
                TotalCost+= (costTasks.get(type) * completedTaskNum);
                TotalIncome+= ( calculatePriceUser(type,origin) * completedTaskNum);
                TotalCompletedSimulation += completedTaskNum;   
                //PETRI
                System.out.println("------------completeTime:  "+completeTime +" timestamp: "+ timestamp);
                if(completeTime >= timestamp)
                    TotalCompletionTime += (completeTime-timestamp);
                //PETRI
                //System.out.println("REPUTATION:"+TotalReputation+";COST:"+TotalCost+";INCOME:"+TotalIncome+";BALANCE:"+(TotalIncome-TotalCost));                
                //String a1= EnergyplusHelper.getDisciplineById("A","aa");
                /*String[] a2= EnergyplusHelper.getDerivedObjects("28","aaa");
                String a3= EnergyplusHelper.getStage("S","aaa");
                String a4= EnergyplusHelper.getStatus("S1","aaa");
                String a5= EnergyplusHelper.getDiscipline("aaa");
                
                System.out.println("Calling Information Service: The discipline is:"+a5+ "C4C processed started!");                
                System.out.println("Objects:"+a2+";Site Discipline:"+a5+";Stages:"+a3+";Statuses:"+a4);     */           
                 
                FedLog.writeLog(origin, operationType, origin, type);
                
                //long currentTime = (long) (timestamp + completeTime);   
                FedLog.writeLog("fedsitelog","simulations.log",completeTime+":"+TotalCompletionTime+":"+TotalCompletedSimulation+":"+TotalIncompleteSimulation
                        +":"+TotalIncome+":"+TotalCost+":"+TotalReputation,"info");
            }
        }
        
        return result;
        /*
        if(value<=deadline+timestamp){//status task when finishes
            System.out.println("TASK:ontime;"+origin+";"+type+";"+value);
            scores.add(transactionOntime);                    
        }else{
            System.out.println("TASK:offtime;"+origin+";"+type+";"+value);
            scores.add(transactionOfftime);
        }
        calculateReputation();
        System.out.println("REPUTATION:"+TotalReputation+";"+value);
        TotalCost+=costTasks.get(type);
        System.out.println("COST:"+TotalCost+";"+value);
        TotalIncome+=calculatePriceUser(type,origin);
        System.out.println("INCOME:"+TotalIncome+";"+value);
        System.out.println("BALANCE:"+(TotalIncome-TotalCost)+";"+value);
        */      
    }
    // Base cost + extra benefit(local/external) per task
    public Double calculatePriceUser(String type,String origin){
        if(origin.equals("local")){
            return (1 + benefitLocal)*costTasks.get(type);
        }else{
            return (1 + benefitExt )*costTasks.get(type);
        }
    }
    
    public void calculateReputation(){
        Double sum=0.0;
        for(Double i:scores){
            sum+=i;
        }
        TotalReputation=sum/scores.size();
    }
    // Update balance of subscriber 
    public void updateSubscriberBalance(String subscriberAddr, String appType, double balance){
        subscriberBalance.put(subscriberAddr+"-"+appType, balance);
    }
    
    public String checkIfExecute(String type, Double deadline, String origin, long timestamp){
        String status="false:";
        if(origin.endsWith("local")){
            if(localWorkers.get(0)+estimatedCompletion.get(type) <= deadline+timestamp){
                status="true:"+calculatePriceUser(type, origin)+":"+TotalReputation;
            }else{
                status="false:";
            }
        }else{
            if(extWorkers.get(0)+estimatedCompletion.get(type)<=deadline+timestamp){
                status="true:"+calculatePriceUser(type,origin)+":"+TotalReputation;
            }else{
                status="false:";
            }
        }
        return status;
    }
    //For paper with EnergyPlus, numtask indicates the number of all tasks. Each task correspond to one combination of parameters.
    public String checkIfExecute(String appType, Double deadline, String origin, long timestamp, int numtask){
        String status="false:";  
        if(!subscriptionStatus.containsKey(appType)){
            return status;
        }
         
        if (origin.endsWith("local")) {
            int numSimulation = getEstimatedNumSimulation(deadline, timestamp, getEstimatedCompletion(appType, 1), "local");
            if (numSimulation > 0) {
                if (numSimulation > numtask) {
                    numSimulation = numtask;
                }
                status = "true:" + calculatePriceUser(appType, origin) * numSimulation + ":" + TotalReputation + ":" + numSimulation;
            } else {
                status = "false";
            }
        } else {
            int numSimulation = getEstimatedNumSimulation(deadline, timestamp, getEstimatedCompletion(appType, 1), "external");
            if (numSimulation > 0) {
                if (numSimulation > numtask) {
                    numSimulation = numtask;
                }
                status = "true:" + calculatePriceUser(appType, origin) * numSimulation + ":" + TotalReputation + ":" + numSimulation;
            } else {
                status = "false";
            }
        }
        //if subscriber is inactive, still return result,but set to false to pass bid
        if(!(Boolean)subscriptionStatus.get(appType) ){
           status.replace("true","false");
        }
        
        return status;
    }
    
    public void setSiteIP(String localIP) {
        this.siteIP = localIP;
    }    
    public String getSiteIP() {
        return this.siteIP;
    }    
    public void setProperties(Properties p){
        properties=p;
    }
    public void setLocalWorkers(List localWorkers) {
        this.localWorkers = localWorkers;
    }
    public void setExtWorkers(List extWorkers) {
        this.extWorkers = extWorkers;
    }
    public List getLocalWorkers() {
        return localWorkers;
    }
    public List getExtWorkers() {
        return extWorkers;
    }
    public HashMap<String, Double> getEstimatedCompletion() {
        return estimatedCompletion;
    }
    public int getNumLocalworkers() {
        return numLocalworkers;
    }
    public int getNumExtWorkers() {
        return numExtWorkers;
    }
    public double getBenefitLocal() {
        return benefitLocal;
    }
    public double getBenefitExt() {
        return benefitExt;
    }
    public double getTransactionOntime() {
        return transactionOntime;
    }
    public double getTransactionOfftime() {
        return transactionOfftime;
    }
    
    public void updateInfo(String infoServiceMsg){
        //System.out.println("incoming update message: "+infoServiceMsg);
        if(infoServiceMsg == null){
            System.out.println("NULL infoService message!");
            return;
        }
        String[] multiMsg = infoServiceMsg.split(";");
        int outerCount = multiMsg.length - 1;
        while(outerCount >= 0){           
            String[] msg = multiMsg[outerCount].split(":");

            String newIP = msg[0];
            int innerCount = msg.length - 1;
            while (innerCount > 0) {
                String[] tempSiteInfo = msg[innerCount].split(",");
                String port = tempSiteInfo[0];
                String siteType = tempSiteInfo[1];

                if (siteType.equals("publisher")) {
                    //now do nothing here if it's publisher
                } else if (siteType.equals("subscriber")) {
                    addInfo(msg[innerCount],newIP, port);
                    /*
                    String subscribeType = "";
                    if(tempSiteInfo[2].equals("CPT")){
                        subscribeType = tempSiteInfo[2] + "," + tempSiteInfo[3];
                    }
                    else if(tempSiteInfo[2].equals("REQ")){
                        subscribeType = tempSiteInfo[2] + "," + tempSiteInfo[3] + "," + tempSiteInfo[4];
                    }

                    ConcurrentHashMap<String, String> siteSubscribe;
                    //System.out.println("[][][][][][][][]New subsite: "+workerNetName+temp.split(",")[1] + "," + temp.split(",")[2]+ temp.split(",")[3]);
                    if (!siteSubscribeList.containsKey(newIP)) {
                        siteSubscribe = new ConcurrentHashMap<String, String>();
                        siteSubscribe.put(port, subscribeType);
                        siteSubscribeList.put(newIP, siteSubscribe);
                    } else {
                        siteSubscribe = siteSubscribeList.get(newIP);
                        siteSubscribe.put(port, subscribeType);
                    }
                    * 
                    */
                }
                --innerCount;
            }
            --outerCount;
        }
    }
    /**
     * Add one or multiple nodes.Only add one node at a time.
     * @param msg 
     */
    public void addInfo(String msg, String ip, String port){
        String[] message = msg.split(",");
        String tupleTag = message[2];
        String taskType = message[3];
        ConcurrentHashMap<String, String> siteSubscribe;
        String subscribeType = "";
        if (message[2].equals("CPT")) {
            subscribeType = tupleTag + "," + taskType;
        } else if (message[2].equals("REQ")) {
            String associatePort = message[4];
            subscribeType = tupleTag + "," + taskType + "," + associatePort;
        }

        if (!siteSubscribeList.containsKey(ip)) {
            siteSubscribe = new ConcurrentHashMap<String, String>();
            siteSubscribe.put(port, subscribeType);
            siteSubscribeList.put(ip, siteSubscribe);
        } else {
            siteSubscribe = siteSubscribeList.get(ip);
            siteSubscribe.put(port, subscribeType);
        }
    }
    /**
     * Remove one or multiple nodes.
     * @param msg (ip,port1:port2:...)
     */
    public void removeInfo(String msg){
        String[] message = msg.split(",");
        String ip = message[0];
        String[] ports = message[1].split(":");
        for(int i = 0; i < ports.length; ++i ){
            if(siteSubscribeList.containsKey(ip)){
                if(siteSubscribeList.get(ip).containsKey(ports[i])){
                    siteSubscribeList.get(ip).remove(ports[i]);
                }
            }
        }
        //if site is empty after deletion, delete it too
        if(siteSubscribeList.containsKey(ip)){
            if(siteSubscribeList.get(ip).isEmpty())
                siteSubscribeList.remove(ip);
        }        
    }
    //return a string of the entire list as:
    //IP1:port1,subscriber,type:port2,subscriber,type;IP2:port1,subscriber,type
    public String toString(){
        StringBuffer sb = new StringBuffer();
        Iterator it = siteSubscribeList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next(); 
            
            String destIP = entry.getKey().toString();
            sb.append(destIP);
            
            ConcurrentHashMap<String,String> subList = (ConcurrentHashMap<String,String>)entry.getValue();
            Iterator subIt = subList.entrySet().iterator();
            
            while(subIt.hasNext()){
                Map.Entry subEntry = (Map.Entry)subIt.next();
                sb.append(":");
                sb.append(subEntry.getKey().toString());
                sb.append(",subscriber,");
                sb.append(subEntry.getValue().toString());            
            }
            sb.append(";");
        }
        return sb.toString();
    }

    /**
     * This method will return the whole list of sites with their subscription information.
     * 
     * @return siteSubscribeList
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<String,String>> getSiteList() {
        return siteSubscribeList;
    }

    /**
     * The method will return subscription information for each site
     * @param siteName
     * @return 
     */
    public ConcurrentHashMap<String,String> getSiteSubscribe(String siteName) {
        if(siteSubscribeList.containsKey(siteName))
            return siteSubscribeList.get(siteName);
        else
            return null;
    }
    /**
     * Check if there exist an associate subscriber
     * @param destIP
     * @param destPort: associate port.
     * @return 
     */
    public boolean hasAssociateSubscriber(String destIP, String destPort){
        if(getSiteSubscribe(destIP) != null){
            ConcurrentHashMap<String,String> tempMap = getSiteSubscribe(destIP);
            
            Iterator it = tempMap.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry entry = (Map.Entry)it.next(); 
                String[] info = entry.getValue().toString().split(",");
                if( info[0].equals("REQ") ){
                    if(info[2].equals(destPort)){
                        return true;
                    }
                }
            }
            return false;
        }
        else
            return false;
    }
    /**
     * Get subscribers of certain type in certain site.
     * @param destIP
     * @param tupleTag
     * @param taskType
     * @return 
     */
    public String getSubscriberByTypeInSite(String destIP, String tupleTag, String taskType){
        String result = "";
        int count = 0;
        if(getSiteSubscribe(destIP) != null){
            ConcurrentHashMap<String,String> tempMap = getSiteSubscribe(destIP);
            Iterator it = tempMap.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry entry = (Map.Entry)it.next(); 
                String[] info = entry.getValue().toString().split(",");
                if( info[0].equals(tupleTag) && info[1].equals(taskType) ){
                    if(count != 0){
                        result = result.concat(":");
                    }
                    result = result.concat(entry.getKey().toString());
                    if(tupleTag.equals("REQ")){
                        result = result.concat(":"+info[2]);
                    }
                    ++count;
                } 
            }
        }
        return result;
    }
    
    public int numSubscribersForType(String tupleTag, String taskType, String[] msg){
        int count = 0;
        Iterator it = siteSubscribeList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next(); 
            
            ConcurrentHashMap<String,String> subList = (ConcurrentHashMap<String,String>)entry.getValue();
            Iterator subIt = subList.entrySet().iterator();
            
            while(subIt.hasNext()){
                Map.Entry subEntry = (Map.Entry)subIt.next();
                String[] info = subEntry.getValue().toString().split(",");
                if( info[0].equals(tupleTag) && info[1].equals(taskType) ){
                    count++;
                }
                //if searching for CPT task, also consider the corresponding REQ task
                else if(tupleTag.equals("CPT")){
                    if(info[0].equals("REQ")){
                        String port;
                        if (msg.length == 7) {
                            port = msg[5];
                            if(!info[2].isEmpty() && info[2].equals(port)){
                                count++;
                            }
                        }
                    }
                }    
            }
        }  
        return count;
    }
    
    /**
     * Add a site to the subscription list.
     * @param siteName
     * @param siteSubscirbe 
     */
    public void addSite(String siteName, ConcurrentHashMap<String,String> siteSubscirbe){
        if(siteSubscribeList.containsKey(siteName)){
            System.out.println("Add site failed! The site: "+siteName+" already exists!");
        }
        else
            siteSubscribeList.put(siteName, siteSubscirbe);
    }
    
    public int numOfSubscribeSites(String pattern){
        String temp;
        if(pattern.split(",")[0].equals("NOT"))
            temp = pattern.replaceFirst("NOT", "REQ");
        else  if(pattern.split(",")[0].equals("FIN"))
            temp = pattern.replaceFirst("FIN","CPT");
        else
            temp = pattern; 
        
        int count = 0;
        Iterator it = siteSubscribeList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next(); 
            
            ConcurrentHashMap<String,String> subList = (ConcurrentHashMap<String,String>)entry.getValue();
            Iterator subIt = subList.entrySet().iterator();
            
            while(subIt.hasNext()){
                Map.Entry subEntry = (Map.Entry)subIt.next();
                
                if(subEntry.getValue().toString().startsWith(temp)){
                    ++count;
                }
            }
        }
        return count;        
    }
    
    public String getAssociateType(String ip, String port){
        String result = "";
        if(getSiteSubscribe(ip) != null){
            ConcurrentHashMap<String,String> tempMap = getSiteSubscribe(ip);
            Iterator it = tempMap.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry entry = (Map.Entry)it.next(); 
                String[] info = entry.getValue().toString().split(",");
                if( info[0].equals("REQ")){
                    if(info[2].equals(port)){
                        result = "CPT,"+info[1];
                    }
                } 
            }
        }
        return result;
    }
    
}
