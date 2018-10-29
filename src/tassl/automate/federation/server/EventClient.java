package tassl.automate.federation.server;

import ifc2x3javatoolbox.ifc2x3tc1.ClassInterface;

import ifc2x3javatoolbox.ifc2x3tc1.IfcLabel;
import ifc2x3javatoolbox.ifc2x3tc1.IfcProject;
import ifc2x3javatoolbox.ifc2x3tc1.IfcRoot;
import ifc2x3javatoolbox.ifcmodel.IfcModel;

import java.net.URI;

import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;







import org.eclipse.jetty.util.component.LifeCycle;

public class EventClient
{
    public static void main(String[] args)
    {
    	//the idea is that the server will initiate the connection
    	String token="c4csecretkey";
        URI uri = URI.create("ws://localhost:8080/events/");
    	//URI uri = URI.create("ws://5.153.52.162:8080/events/");
    	String cmess=null;
    	
        try
        {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
           cmess="Client Works";
           
            try
            {
            	
                // Attempt Connect
                Session session = container.connectToServer(EventSocket.class,uri);
                // Send a message
                
                //session.getBasicRemote().sendText("Hello");
                System.out.println("Sending from client...");
                session.getBasicRemote().sendText(cmess);
               //session.getBasicRemote().             
                //session.getBasicRemote().sendObject(c4co.getStepLine());
              
                // Close session
                session.close();
            }
            finally
            {
                // Force lifecycle stop when done with container.
                // This is to free up threads and resources that the
                // JSR-356 container allocates. But unfortunately
                // the JSR-356 spec does not handle lifecycles (yet)
                if (container instanceof LifeCycle)
                {
                    ((LifeCycle)container).stop();
                }
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
    }
}