/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tassl.automate.federation;

import tassl.automate.comet.CometConstants;
import tassl.automate.comet.XmlTuple;
import tassl.automate.comet.tuplespace.TSConstant;
import tassl.automate.comet.xmltuplespace.XmlTupleService;
import tassl.automate.federation.simpleapi.FedMeteorGenericTaskTuple;

/**
 *
 * @author Javi
 */
public class FedTaskTuple extends FedMeteorGenericTaskTuple{
    
    String sampletag;
    String tupleTag;
    String taskType;
    String port;
    int optID;
    String destNetName;
    String info1;
    String info2;
    String time;
    String location;
    long timestamp;
      
    public int getOptID() {
        return optID;
    }

    public String getInfo1() {
        return info1;
    }

    public String getInfo2() {
        return info2;
    }
    
    public long getTimestamp(){
        return timestamp;
    }
    
    public String gettupleTag() {
        return tupleTag;
    }
        
    public String gettaskType() {
        return taskType;
    }
               
    public String getdestNetName() {
        return destNetName;
    }
    
    public String getLocation(){
        return location;
    }
    
    
    public String getSampletag(){
        return sampletag;
    }
    
    public String setTaskStr(int _taskid, String _masterNetName, String samplevalue) {
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">"+ _taskid +"</"+CometConstants.TaskId+">" +                          //mandatory tag
                "<"+CometConstants.MasterNetName+">"+ _masterNetName + "</"+CometConstants.MasterNetName+">" +    //mandatory tag
                "<sampletag>"+samplevalue+"</sampletag>"+
             "</AppTask>";

        taskid = _taskid;
        masterNetName = _masterNetName;
        sampletag=samplevalue;
        return s;
    }
        
     public String setTaskStr(int _taskid,String _masterNetName,int _optID, String _tupleTag,String _taskType,
             String _destNetName,String _info1,String _info2, String _port, String _location, long _timestamp){
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">"+ _taskid +"</"+CometConstants.TaskId+">" +                          //mandatory tag
                "<"+CometConstants.MasterNetName+">"+ _masterNetName + "</"+CometConstants.MasterNetName+">" +    //mandatory tag
                "<TupleTag>"+ _tupleTag + "</TupleTag>" +                   //job or manage
                "<TaskType>"+ _taskType + "</TaskType>" +                   //detailed tuple type
                "<OptID>"+ _optID + "</OptID>" +                            //optID
                "<DestNetName>"+ _destNetName + "</DestNetName>" +          //destination node ip
                "<Info1>"+ _info1 + "</Info1>" +                               //resources needed for running this task
                "<Info2>"+ _info2 + "</Info2>" +                      //currency gained for running this task
                "<Port>"+ _port + "</Port>" +                               //destination port
                "<Location>"+_location + "</Location>" +                    //location
                "<Timestamp>"+_timestamp+ "</Timestamp>"+
             "</AppTask>";

        taskid = _taskid;
        masterNetName = _masterNetName;
        tupleTag = _tupleTag;
        taskType = _taskType;
        optID = _optID;
        destNetName = _destNetName;
        info1 = _info1;
        info2 = _info2;
        port = _port;
        location = _location;
        timestamp=_timestamp;
        return s;
    }
     
    @Override
    public XmlTuple getQuery(){
        XmlTuple template = new XmlTupleService();
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">*</"+CometConstants.TaskId+">" +  //mandatory tag
                "<sampletag>*</sampletag>"+
                "<"+CometConstants.MasterNetName+">*</"+CometConstants.MasterNetName+">" +    //mandatory tag
                "<"+CometConstants.TaskPriority+">*</"+CometConstants.TaskPriority+">" +      //mandatory tag
             "</AppTask>";
        template.createXMLquery(s);
        return template;
    }
    
    //new query defined that will only get those TaskTuples where sample task is equal to value
    public XmlTuple getQuery(String value){
        XmlTuple template = new XmlTupleService();
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">*</"+CometConstants.TaskId+">" +  //mandatory tag
                "<sampletag>"+value+"</sampletag>"+
                "<"+CometConstants.MasterNetName+">*</"+CometConstants.MasterNetName+">" +    //mandatory tag
                "<"+CometConstants.TaskPriority+">*</"+CometConstants.TaskPriority+">" +      //mandatory tag
             "</AppTask>";
        template.createXMLquery(s);
        return template;
    }
    //new query defined for federation with tupleTag, taskType, destIP
    public XmlTuple getQuery(String _tupleTag, String _taskType,String _destNetName, String _port, String _location){
        XmlTuple template = new XmlTupleService();
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">*</"+CometConstants.TaskId+">" +  //mandatory tag
               // "<sampletag>"+value+"</sampletag>"+
                "<TupleTag>"+_tupleTag+"</TupleTag>" +                   //bid,bidreply or compute,etc
                "<TaskType>"+_taskType+"</TaskType>" +                   //detailed tuple type
                "<OptID>*</OptID>" +                            //Operation ID
                "<DestNetName>"+_destNetName+"</DestNetName>" +   //destNetName should be the same as worker's IP
                "<Port>"+ _port + "</Port>" +                     //destination port
                "<Info1>*</Info1>" +                               //budget to run a task
                "<Info2>*</Info2>" +                        //deadline to complete task
                "<Location>"+_location+"</Location>" +          //location of destination sites
                "<Timestamp>*</Timestamp>" +
                "<"+CometConstants.MasterNetName+">*</"+CometConstants.MasterNetName+">" +          //mandatory tag
                "<"+CometConstants.TaskPriority+">*</"+CometConstants.TaskPriority+">" +            //mandatory tag
             "</AppTask>";
        template.createXMLquery(s);
        return template;
    }
    
    @Override
    public void getTaskTuple(XmlTuple tup){
        String[] s = tup.getkeys();

        //get tags
        for (int i=0;i<s.length;i++){
            if(s[i].equals(CometConstants.TaskId))
                taskid = new Integer(s[i+1]).intValue();
            else  if(s[i].equals(CometConstants.MasterNetName))
                masterNetName = s[i+1];
            else if (s[i].equals("TupleTag"))
                tupleTag = s[i+1];
            else if (s[i].equals("TaskType"))
                taskType = s[i+1];
            else if (s[i].equals("OptID"))
                optID = new Integer(s[i+1]).intValue();
            else if (s[i].equals("DestNetName"))
                destNetName = s[i+1];
            else if (s[i].equals("Info1"))
                info1 = s[i+1];
            else if (s[i].equals("Info2"))
                info2 = s[i+1];
            else if (s[i].equals("Port"))
                port = s[i+1];
            else if (s[i].equals("Location"))
                location = s[i+1];
            else if (s[i].equals("Timestamp"))
                timestamp= new Long(s[i+1]).longValue();
            }     
        //get data
        data = new byte [tup.getData().length];
        data = tup.getData();
    }
    
    public String createPoisonpillTag(String mastername, int poisonId, String value) {
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<TaskPriority>"+ TSConstant.HighPriority + "</TaskPriority>" +
                "<"+CometConstants.TaskId+">"+ poisonId +"</"+CometConstants.TaskId+">" +
                "<"+CometConstants.MasterNetName+">"+ mastername + "</"+CometConstants.MasterNetName+">" +
                "<sampletag>"+value+"</sampletag>"+
             "</AppTask>";
        return s;
    }
    
    public String createPoisonpillTag(String mastername, int poisonId, String tupleTag, String taskType, 
            String destNetName, String port) {
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<TaskPriority>"+ TSConstant.HighPriority + "</TaskPriority>" +
                "<"+CometConstants.TaskId+">"+ poisonId +"</"+CometConstants.TaskId+">" +
                "<"+CometConstants.MasterNetName+">"+ mastername + "</"+CometConstants.MasterNetName+">" +
                "<TupleTag>"+tupleTag+"</TupleTag>" +                   //REQ,BID,CPT,etc
                "<TaskType>"+taskType+"</TaskType>" +                   //detailed tuple type, red, black
                "<DestNetName>"+destNetName+"</DestNetName>" +        //destNetName should be the same as worker's IP
                "<Port>"+ port + "</Port>" +                          //destination port
                "<OptID>"+ 0 + "</OptID>" +                            //Operation ID
                "<Info1>"+ 0 + "</Info1>" +                               //resources needed for running this task
                "<Info2>"+ 0 + "</Info2>" +                      //currency gained for running this task
                "<Timestamp>"+0+"</Timestamp>" +
             "</AppTask>";
        return s;
    }
}
