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
@ServerEndpoint(value="/getmodel/")
public class EventSocket1
{
    String token="c4csecretkey";
    @OnOpen
    public void onWebSocketConnect(Session sess) throws Exception
    {
        C4CHelper.models=new ArrayList<HashMap<String,String>>();
         C4CHelper.modelNames=new ArrayList<String>();
       // System.out.println("Received C4C client message -- getmodel: ");
       // Collection <ClassInterface> mess= C4CHelper.getCurrentModel(token);
        //StringBuilder str=new StringBuilder();
        //for (ClassInterface object : mess) str.append(object.getStepLine()).append("\n");
        
        try {
            String _info1="FETCH";
            String discipline=C4CHelper.getDiscipline(token).substring(0, 1);
            String _info2=discipline;
            //String param="EVENT"+"|"+discipline;
            Thread fedClient=new Thread(new FedClient(_info1,_info2));
        
            fedClient.start();
            System.out.println("[Notification-Socket] Discipline "+discipline+ "called getModel()");
       //     System.out.println("IN SOCKET STARTING.....");
            System.out.println("[Notification-Socket] Fetching model from other disciplines");
            while (C4CHelper.finalModels==null)   Thread.sleep(1000);
            //StringBuffer str=new StringBuffer();
          //  for (String stepId: C4CHelper.resultModel.keySet()) str.append(stepId).append("=").append(C4CHelper.resultModel.get(stepId)).append("\n");
            //C4CHelper.resultModel=null;
            //return str.toString();
           
            //sess.getBasicRemote().sendText(C4CHelper.getCurrentModel(token));
            C4CHelper.resultModel= C4CHelper.makeMergingModel(C4CHelper.finalModels,null,token);
            sess.getBasicRemote().sendText(C4CHelper.resultModel);
            System.out.println("[Notification-Socket] Created a combined model of "+C4CHelper.finalModels.size()+" models");
            C4CHelper.finalModels=null;
            C4CHelper.resultModel=null;
            System.out.println("[Notification-Socket] Merging process finalized!");
	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
        }
    }
    
      
    @OnMessage
    public void onWebSocketText(String message)
    {
        //System.out.println("Received TEXT message: " + message);
        
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
