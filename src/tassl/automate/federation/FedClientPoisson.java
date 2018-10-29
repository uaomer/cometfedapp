
package tassl.automate.federation;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.io.ArgHandler;

/**
 *
 * @author Mengsong Zou
 */
public class FedClientPoisson implements Runnable{
    DataOutputStream output;
    DataInputStream in;
    
    String serverIP;
    int serverPort;
    String tupleTag;
    String taskType;
    double budget = 0;
    double deadline = 0;
    String messageType;
    long timestamp=0;
    String distributionFile;
    String appSpecific = "";
    //FedClient(String newAddress,int newPort, String newTupleTag, String newTaskType, double newCost, double newBenefit){
    FedClientPoisson(String[] args){
        ArgHandler argHandler = new ArgHandler(args);
        serverIP = argHandler.getStringArg("-serverIP");
        serverPort = argHandler.getIntArg("-serverPort");  
        tupleTag = argHandler.getStringArg("-tupleTag");
        taskType = argHandler.getStringArg("-taskType");        
        messageType = argHandler.getStringArg("-messageType");
        if(messageType.equals("OPT")){
            budget = argHandler.getDoubleArg("-budget");
            deadline = argHandler.getDoubleArg("-deadline");
            timestamp= argHandler.getIntArg("-timestamp");
            appSpecific = "null";
        }
        distributionFile=argHandler.getStringArg("-distFile");    
    }
    @Override
    public void run(){
        try {
            
            BufferedReader br = new BufferedReader(new FileReader(distributionFile));
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                String [] parts=sCurrentLine.split(":");
                System.out.println(parts[0]+" "+parts[1]+" "+parts[2]+" "+parts[3]);
                /*
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(FedClientPoisson.class.getName()).log(Level.SEVERE, null, ex);
                }\
                * 
                */
                timestamp=Long.parseLong(parts[0]);
                //taskType=parts[2].split("-")[0];
                taskType="energyplus";
                deadline=Double.parseDouble(parts[3]);
                connectServer();                
            }            
            
        } catch (UnknownHostException ex) {
            Logger.getLogger(FedClientPoisson.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("UnknownHostException");
        } catch (IOException ex) {
            Logger.getLogger(FedClientPoisson.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("IOException");
        }
    }
        
    public void connectServer() throws UnknownHostException, IOException{  
        
        InetAddress destAddr = InetAddress.getByName(serverIP); 
        Socket sock = new Socket(serverIP, serverPort);
        System.out.println("Connect to Server");
        
        output = new DataOutputStream(sock.getOutputStream());
        in = new DataInputStream(sock.getInputStream());
        
        if(messageType.equals("END"))
            output.writeUTF(messageType+","+tupleTag+","+taskType);
        else if (messageType.equals("ADD"))
             //this could be changed to add multiple consumers later
            output.writeUTF(messageType+","+tupleTag+","+taskType+",1");
        else
            output.writeUTF(tupleTag+","+taskType+","+budget+","+deadline+","+timestamp+","+appSpecific);
        
        output.flush(); 
  
        boolean more = true;
        String destPort, bidPort = "";
        /*Commented for paper tests
        while(more){
            String feedback = in.readUTF();
            System.out.println("feedback: "+feedback);
            String resultSite = feedback.split(",")[2];
            String port = feedback.split(",")[3];
            String feedbackType = feedback.split(",")[0];
            
            if (feedbackType.equals("BID")) {
                String optID = feedback.split(",")[4];
                destPort = port.split(":")[0];
                bidPort = port.split(":")[1];
                double priceClient=Double.valueOf(feedback.split(",")[1]).doubleValue();                
                output.writeUTF("CPT,"+taskType+","+budget+","+deadline+","+resultSite+","+destPort+","+optID);
                output.flush();
                
            }
            else if(feedbackType.equals("FIN")){
                System.out.println("Result: "+feedback.split(",")[1]);
                //System.out.println("Send END message");
                //END,IP,port1,port2
                //output.writeUTF("END,"+tupleTag+","+taskType);
                //output.flush();
                more = false;
                System.out.println("Job Finished!");
            }
            else if(feedbackType.equals("ERR") || feedbackType.equals("DEL") || feedbackType.equals("ADD")){
                System.out.println(feedback.split(",")[1]);
                more = false;
            }
        }  
        */
        output.close();
        in.close();
        sock.close();
    }
    
    public static void main(String[] args){
        Thread fedClient=new Thread(new FedClientPoisson(args));
        //Thread fedClient=new Thread(new FedClient("tassl02.engr.rutgers.edu",5560,"REQ","red",5,10));
        fedClient.start();   
    }
}
