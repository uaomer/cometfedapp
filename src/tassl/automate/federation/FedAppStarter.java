/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tassl.automate.federation;

/**
 *
 * @author Javier Diaz
 */
import java.net.URISyntaxException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.io.ArgHandler;
import programming5.io.Debug;
import programming5.net.IncompleteResultException;
import tassl.automate.application.node.FedMeteorDrtsPeer;
import tassl.automate.util.FedLog;


public class FedAppStarter {

    FedMeteorStarter fedMeteorStarter;
    
    public FedAppStarter(String[] args, String nodeFile, String portFile, List bootStrapNodes) {
        Debug.enableDefault();
        ArgHandler argHandler = new ArgHandler(args);
        Vector<String> propertyFiles = argHandler.getMultipleStringArg("-propertyFile");
        Vector<String> exceptionFiles = argHandler.getMultipleStringArg("-exceptionFile");

        
        FedMeteorStarter mt = new FedMeteorStarter(nodeFile, portFile, propertyFiles.toArray(new String[]{}), exceptionFiles.toArray(new String[]{}),bootStrapNodes);

        try {
            //setup meteor infrastructure
            mt.initMeteor();

            //wait for overlay stabilized
            Thread.sleep(Long.parseLong(System.getProperty("OVERLAY_SETUP_WAITING", "0")));

        } catch (InterruptedException ex) {
            //Logger.getLogger(FedAppStarter.class.getName()).log(Level.SEVERE, null, ex);
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (URISyntaxException ex) {
            //System.out.println("Invalid node names in node file");
            FedLog.writeLog("manageLog","manageLog%g.log","Invalid node names in node file","severe");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        } catch (IncompleteResultException ex) {
            //System.out.println("Timed out on ring create");
            FedLog.writeLog("manageLog","manageLog%g.log","Timed out on ring create","severe");
            FedLog.writeLog("manageLog","manageLog%g.log",FedLog.getExceptionStack(ex),"severe");
        }

        appSpecificStartup(mt);

        mt.startApp();

        
    }
    //get publisher master 
    public FedMeteorPublisherMaster getDrtsPeerMaster() {
        return (FedMeteorPublisherMaster)fedMeteorStarter.getDrtsPeer().getMaster();
    }
    //get subscriber worker
    public FedMeteorSubscriberWorker getDrtsPeerWorker() {
        return (FedMeteorSubscriberWorker)fedMeteorStarter.getDrtsPeer().getWorker();
    }
    public void appSpecificStartup(FedMeteorStarter cs) {
        fedMeteorStarter=cs;
    }

    public FedMeteorStarter getFedMeteorStarter(){
        return fedMeteorStarter;
    }
}