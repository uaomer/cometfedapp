package tassl.automate.federation.server;
import java.io.IOException;

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
import tassl.automate.federation.FedMeteorPublisherMaster;

//EventSocket -- updatemodel
//EventSocket1 --getmodel
//EventSocket2 --checkifupdate
@ClientEndpoint
@ServerEndpoint(value="/updatemodel/")
public class EventSocket
{
    String token="c4csecretkey";
    @OnOpen
    public void onWebSocketConnect(Session sess) throws Exception 
    {
        //System.out.println("Socket Connected: " + sess);
       
        String discipline=C4CHelper.getDiscipline(token).substring(0, 1); 
        String suitability=sess.getQueryString();
        System.out.println("[Notification-Socket] Discipline "+discipline+" called updateModel() with suitability: "+suitability);
    }
    
      
    @OnMessage
    public void onWebSocketText(String message, Session sess) throws Exception
    {
       
        String suitability=null;
        HashMap<String,String> suitabilities=new HashMap<String,String>();
       if (sess.getQueryString()!=null) {
                String[] data=sess.getQueryString().split("&");
                for (String d: data) {
                    if (d.startsWith("suitability")) {
                        d=d.replace("suitability=","");
                        suitability=d;
                    } else if (d.startsWith("suitabilities")) {
                         d=d.replace("suitabilities=","");
                        String[] s=d.split(",");
                        for (int i=0; i < s.length;i+=2){
                          suitabilities.put(s[i],s[i+1]);
                        }
                    }
                }
       }
       
        System.out.println("[Notification-Socket] IFC Objects Recieved");
        C4CHelper.updateModel(message,suitability, suitabilities,token);
        String discipline=C4CHelper.getDiscipline(token).substring(0, 1);
        String param="EVENT"+"|"+discipline;
        System.out.println("[Notification-Socket] Local Model Updated - Syncronizing Metadata");
        
        //MAKE SURE YOU CHANGE THE DISCIPLINE
        //BUG -- the UPDATE client submits more the 1 call
      
        try {
            
                 //   Thread.sleep(1000);  
                    //System.out.println("Issuing metadata syncronization from [SocketUpdateModel]");
                   // Thread.sleep(100);
                    Thread fedClient=new Thread(new FedClient(param,"MET"));
                    fedClient.start();
                    //System.out.println("IN UPDATE MODEL SOCKET  STARTING.....");
                    while (C4CHelper.uploadModel=false) Thread.sleep(1000);
                    
                    //System.out.println("Issuing version syncronization from socket [SocketUpdateModel]");
                     //Thread.sleep(100);
                    Thread fedClient2=new Thread(new FedClient(param,"VER"));
                    fedClient2.start();
            
        } catch (InterruptedException e) {
        }

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
