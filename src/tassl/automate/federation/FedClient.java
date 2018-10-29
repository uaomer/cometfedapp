
package tassl.automate.federation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.io.ArgHandler;

/**
 *
 * @author Ioan Petri
 */

// ./startFedClientGeneric.sh C4C modObject 0 id1 event

public class FedClient implements Runnable{
    DataOutputStream output;
    DataInputStream in;
    
    String serverIP;
    int serverPort;
    String tupleTag;
    String taskType;
   // double budget = 0;
    //double deadline = 0;
    String messageType;
    long timestamp=0;
    //String appSpecific = "";    //specific information for diff application
    String info1 = "";
    String info2 = "";
    
    public FedClient(String[] args){
        ArgHandler argHandler = new ArgHandler(args);
        serverIP = argHandler.getStringArg("-serverIP");
        
        serverPort = argHandler.getIntArg("-serverPort");  
        tupleTag = argHandler.getStringArg("-tupleTag");
        taskType = argHandler.getStringArg("-taskType");        
        messageType = argHandler.getStringArg("-messageType");
        timestamp= 0;
       //appSpecific = "c4c";
        info1 = argHandler.getStringArg("-info1");
        info2 = argHandler.getStringArg("-info2");
        
        
    }
    //String [] args={"5.153.52.162","5560","OPT","C4C","modObject","0","id1","change"};
    public FedClient(String _info1, String _info2){
        String ipresult=null;
        try {
            ipresult=C4CHelper.getRows(C4CHelper.master).get(0)[0];
             System.out.println("MASTER IP IS: " + ipresult);
        } catch (Exception ex) {
            Logger.getLogger(FedClient.class.getName()).log(Level.SEVERE, null, ex);
        }
       
         //serverIP = "5.153.52.162";
        serverIP = ipresult;
        serverPort = 5560;  
        tupleTag = "OPT";
        taskType = "C4C";        
        messageType = "modObject";
        timestamp= 0;
       //appSpecific = "c4c";
        info1 = _info1;
        info2 = _info2;
        
        
    }
    @Override
    public void run(){
        try {
            connectServer();
        } catch (UnknownHostException ex) {
            Logger.getLogger(FedClient.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("UnknownHostException");
        } catch (IOException ex) {
            Logger.getLogger(FedClient.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("IOException");
        }
    }
        
    public void connectServer() throws UnknownHostException, IOException{  
        
        InetAddress destAddr = InetAddress.getByName(serverIP); 
        Socket sock = new Socket(serverIP, serverPort);
        //System.out.println("[C4C-Notification] Connect to Server");
        
        output = new DataOutputStream(sock.getOutputStream());
        in = new DataInputStream(sock.getInputStream());
        
        if(messageType.equals("END"))
            output.writeUTF(messageType+","+tupleTag+","+taskType);
        else if (messageType.equals("ADD"))
             //this could be changed to add multiple consumers later
            output.writeUTF(messageType+","+tupleTag+","+taskType+",1");
        else{
            output.writeUTF(tupleTag+","+taskType+","+timestamp+","+info1+","+info2);
            //System.out.println(tupleTag+","+taskType+","+timestamp+","+info1+","+info2);
        }
        
        output.flush(); 
  
        boolean more = true;
        
        output.close();
        in.close();
        sock.close();
    }
    
    public static void main(String[] args){
        Thread fedClient=new Thread(new FedClient(args));
        //Thread fedClient=new Thread(new FedClient("tassl02.engr.rutgers.edu",5560,"REQ","red",5,10));
        fedClient.start();   
    }
}
