package tassl.automate.federation.server;
import ifc2x3javatoolbox.ifc2x3tc1.ClassInterface;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
@ServerEndpoint(value="/getinterfacedata/")
//BRE INTERFACE
public class EventSocket3
{
    String token="c4csecretkey";
    
    @OnOpen
    public void onWebSocketConnect(Session sess) throws Exception
    {
        System.out.println("[Notification-Socket] getInterfaceData call");
        //process the xml 
        //retrive the IPs of the masters
        //retrive the discipline name
        //evaluate what else is important
        try {
            sess.getBasicRemote().sendText("[C4C response] Conected");
	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
        }
    }
    
      
    @OnMessage
    public void onWebSocketText(String message)
    {
        C4CHelper.resultConfig=message;
        ArrayList<String> result=new ArrayList<String>();
        try
        {
            String _info1="CONFIG";
            String discipline=C4CHelper.getDiscipline(token).substring(0, 1);
            String _info2=discipline;
            //String param="EVENT"+"|"+discipline;
            Thread fedClient=new Thread(new FedClient(_info1,_info2));
            fedClient.start();
            System.out.println("[Notification-Socket] Discipline " +discipline+ " called config from client");
        } catch (Exception e)
        {
            System.err.println(e.toString());
        }
        
    }
    
    @OnClose
    public void onWebSocketClose(CloseReason reason)
    {
        System.out.println("Socket Closed: " + reason);
    }
    
    @OnError
    public void onWebSocketError(Throwable cause)
    {
        cause.printStackTrace(System.err);
    }
}
