/*
 * Copyright (c) 2009, NSF Cloud and Autonomic Computing Center, Rutgers University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided 
 * that the following conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and 
 * the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of the NSF Cloud and Autonomic Computing Center, Rutgers University, nor the names of its 
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

package tassl.automate.federation.simpleapi;

import tassl.automate.meteor.application.simpleapi.*;
import tassl.automate.comet.CometConstants;
import tassl.automate.comet.XmlTuple;
import tassl.automate.comet.tuplespace.TSConstant;
import tassl.automate.comet.xmltuplespace.XmlTupleService;
import tassl.automate.programmodel.masterworker.MWConstants;
import tassl.automate.programmodel.masterworker.TaskFramework;

/**
 *
 * @author Javier Diaz
 */

public class FedMeteorGenericTaskTuple implements TaskFramework {
    protected int taskid;
    protected String masterNetName;
    public byte[] data;

    public FedMeteorGenericTaskTuple() {
    }
    
    public String setTaskStr(int _taskid, String _masterNetName) {
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">"+ _taskid +"</"+CometConstants.TaskId+">" +                          //mandatory tag
                "<"+CometConstants.MasterNetName+">"+ _masterNetName + "</"+CometConstants.MasterNetName+">" +    //mandatory tag
             "</AppTask>";

        taskid = _taskid;
        masterNetName = _masterNetName;
        return s;
    }
    
    public void getTaskTuple(XmlTuple tup){
        String[] s = tup.getkeys();
        
        //get tags
        for (int i=0;i<s.length;i++){
            if(s[i].equals(CometConstants.TaskId))
                taskid = new Integer(s[i+1]).intValue();
            else  if(s[i].equals(CometConstants.MasterNetName))
                masterNetName = s[i+1];
        }

        //get data
        data = new byte [tup.getData().length];
        data = tup.getData();
    }

    @Override
    public XmlTuple getQuery(){
        XmlTuple template = new XmlTupleService();
        String s = new String();
        s = "<AppTask>" +       //set your task name
                "<"+CometConstants.TaskId+">*</"+CometConstants.TaskId+">" +  //mandatory tag
                //TODO: add your tags here
                "<"+CometConstants.MasterNetName+">*</"+CometConstants.MasterNetName+">" +    //mandatory tag
                "<"+CometConstants.TaskPriority+">*</"+CometConstants.TaskPriority+">" +      //mandatory tag
             "</AppTask>";
        template.createXMLquery(s);
        return template;
    }

    @Override
    public String createPoisonpillTag(String mastername, int poisonId) {
        String s;
        s = "<AppTask>" +       //set your task name
                "<TaskPriority>"+ TSConstant.HighPriority + "</TaskPriority>" +
                "<"+CometConstants.TaskId+">"+ poisonId +"</"+CometConstants.TaskId+">" +
                "<"+CometConstants.MasterNetName+">"+ mastername + "</"+CometConstants.MasterNetName+">" +
             "</AppTask>";
        return s;
    }

    public int getTaskid() {
        return taskid;
    }

    public String getMasterName() {
        return masterNetName;
    }
}
