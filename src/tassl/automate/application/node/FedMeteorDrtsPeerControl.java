/*
 * Copyright (c) 2009, NSF Center for Autonomic Computing, Rutgers University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and
 * the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of the NSF Center for Autonomic Computing, Rutgers University, nor the names of its
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

/*
 * MeteorDrtsPeerCOntrol.java
 *
 * Created on Nov 24, 2010 1:35:09 PM
 */
package tassl.automate.application.node;


import java.util.Hashtable;
import java.util.logging.Logger;
import tassl.automate.network.ControlConstant;
import tassl.automate.application.network.TCPServerModule;
import tassl.automate.application.network.SSLServerModule;
import tassl.automate.application.network.ServerModule;
import tassl.automate.overlay.OverlayObjectControl;
import tassl.automate.util.FedLog;

/**
 * @author Javier Diaz
 *
 */
public class FedMeteorDrtsPeerControl implements OverlayObjectControl<FedMeteorDrtsPeer>{
    
    protected ServerModule control_server = null;
    //Javi: the peer will request join the overlay instead of assuming it is already joined
    @Override
    public FedMeteorDrtsPeer startInstance(byte[] nodeFile, Hashtable<String, String> properties) throws InstantiationException {
        
        //this is for test
        //System.out.println("MeteorDrtsPeer: masterid system "+System.getProperty("MasterID"));
        //Logger manageLog = Logger.getLogger("manageLog");
        //manageLog.info("MeteorDrtsPeer: masterid system "+System.getProperty("MasterID")); 
        FedLog.writeLog("manageLog","manageLog%g.log","MeteorDrtsPeer: masterid system "+System.getProperty("MasterID"),"info"); 
                
        if(control_server == null){
            //if (System.getProperty("chord.NETWORK_CLASS", "TCP").equals("SSL"))
            String type=properties.get("chord.NETWORK_CLASS");
            if (type!=null && type.equals("SSL"))
                control_server = new SSLServerModule(ControlConstant.TCP_CONTROL_PORT);
            else
                control_server = new TCPServerModule(ControlConstant.TCP_CONTROL_PORT);
        }
        return new FedMeteorDrtsPeer(properties, control_server);
    }
    
    @Override
    public void terminateInstance(FedMeteorDrtsPeer instance) {
        instance.terminateInstance();

    }
}
