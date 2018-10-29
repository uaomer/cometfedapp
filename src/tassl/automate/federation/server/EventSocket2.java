package tassl.automate.federation.server;
import ifc2x3javatoolbox.ifc2x3tc1.ClassInterface;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import tassl.automate.federation.C4CHelper;
import tassl.automate.federation.FedClient;
import tassl.automate.federation.FedClientPoisson;

@ClientEndpoint
@ServerEndpoint(value="/getfilteredmodel/")
//FILTERED SERVICE
public class EventSocket2
{
    String token="c4csecretkey";
    @OnOpen
    public void onWebSocketConnect(Session sess) throws Exception
    {
       // System.out.println("Received C4C client message -- getFilteredModel: ");
         C4CHelper.models=new ArrayList<HashMap<String,String>>();
         C4CHelper.modelNames=new ArrayList<String>();
       // Collection <ClassInterface> mess= C4CHelper.getCurrentModel(token);
        //StringBuilder str=new StringBuilder();
        //for (ClassInterface object : mess) str.append(object.getStepLine()).append("\n");
        try {
            String _info1="FETCH";
            String disc=C4CHelper.getDiscipline(token).substring(0, 1);
            String _info2=disc;
            //String param="EVENT"+"|"+discipline;
            Thread fedClient=new Thread(new FedClient(_info1,_info2));
        
            fedClient.start();
         //   System.out.println("IN SOCKET STARTING.....");
            
            while (C4CHelper.finalModels==null)   Thread.sleep(1000);
            
            //fedClient.start();
            ArrayList<String> suitability=new ArrayList<String>();
            ArrayList<String> disciplines=new ArrayList<String>();
            
            if (sess.getQueryString()!=null) {
                String[] data=sess.getQueryString().split("&");
                for (String d: data) {
                    if (d.startsWith("suitability")) {
                        d=d.replace("suitability=","");
                        String[] s=d.split(",");
                    for (String ss:s) {
                        System.out.println("S:"+ss);
                        suitability.add(ss);
                    }
                } else if (d.startsWith("discipline")) {
                     d=d.replace("discipline=","");
                     String[] s=d.split(",");
                      for (String ss:s) {
                          disciplines.add(ss);
                              System.out.println("D:"+ss);
                      }
                }
            }
            }
            System.out.println("[Notification-Socket] Discipline " +disc+ " called getFilteredModel() "+suitability.toString()+","+disciplines.toString());
            System.out.println("[Notification-Socket] Fetching model from other disciplines");
            
            sess.getBasicRemote().sendText(C4CHelper.getFilteredModel(C4CHelper.finalModels,suitability.toArray(new String[0]),disciplines.toArray(new String[0]),token));
            System.out.println("[Notification-Socket] Returned Merged Filtered Model");
            C4CHelper.finalModels=null;
            
            //Header, Footer, Timestamp 
            //packing the software
            //sess.getBasicRemote().sendText(C4CHelper.getCurrentModel(token));
	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
        }
    }
    
      
    @OnMessage
    public void onWebSocketText(String message)
    {
       // System.out.println("Received TEXT message: " + message);
        
    }
    
    @OnClose
    public void onWebSocketClose(CloseReason reason)
    {
        //System.out.println("Socket Closed: " + reason);
    }
    
    @OnError
    public void onWebSocketError(Throwable cause)
    {
        cause.printStackTrace(System.err);
    }
}
