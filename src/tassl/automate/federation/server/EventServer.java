package tassl.automate.federation.server;

import java.net.URI;


import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import javax.websocket.server.ServerContainer;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;


//We should be able to respond to client by making a client instance
//eventually server will be a new method in the client and vice-versa
public class EventServer 
{
	
	
	
   public EventServer()
    {
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8080);
        server.addConnector(connector);

        // Setup the basic application "context" for this application at "/"
        // This is also known as the handler tree (in jetty speak)
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
     

        try
        {
            // Initialize javax.websocket layer
            ServerContainer wscontainer = WebSocketServerContainerInitializer.configureContext(context);
            
            // Add WebSocket endpoint to javax.websocket layer
            wscontainer.addEndpoint(EventSocket.class);
            wscontainer.addEndpoint(EventSocket1.class);
            wscontainer.addEndpoint(EventSocket2.class);
            wscontainer.addEndpoint(EventSocket3.class);
            wscontainer.setDefaultMaxTextMessageBufferSize(1000000000);
            server.start();
           // returnMessage();
            System.out.println("~~~~~~~~The server is started ...");
            //server.dump(System.err);
            //server.join();
            
          
            
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        
        
    }
}