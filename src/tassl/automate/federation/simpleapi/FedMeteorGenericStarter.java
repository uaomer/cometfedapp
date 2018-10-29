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
 * MeteorAppStarter.java
 *
 * Created on Nov 26, 2010 9:51:47 PM
 */
package tassl.automate.federation.simpleapi;


import tassl.automate.meteor.application.simpleapi.*;
import tassl.automate.meteor.application.MeteorCommonAppStarter;
import tassl.automate.meteor.application.MeteorStarter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Samprita Hegde
 * created on Nov 26, 2010 9:51:47 PM
 *
 */
public class FedMeteorGenericStarter extends MeteorCommonAppStarter{

    MeteorStarter meteorStarter;
    
    /**
     * @param args
     */
    public FedMeteorGenericStarter(String[] args) {
            super(args);

    }
	
    public static void main(String[] args) {
        new FedMeteorGenericStarter(args);
    }
	

    @Override
    public void appSpecificStartup(MeteorStarter cs) {
        System.out.println("specific startup");
        meteorStarter = cs;
    }
    
    @Override
    public void appSpecificEndup() { 
        System.out.println("Specific endup");
        boolean done=false;
        while (!done){
            done=isJobDone();
            if (!done){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(FedMeteorGenericStarter.class.getName()).log(Level.SEVERE, null, ex);
                }
            }else{
                System.out.println("Job DONE");
            }
        }
                
        System.out.println("Waiting for moinitoring thread");
        ((FedMeteorGenericMaster)meteorStarter.getDrtsPeer().getMaster()).waitMonitoring();
        
        System.out.println("Waiting for Master thread");
        ((FedMeteorGenericMaster)meteorStarter.getDrtsPeer().getMaster()).waitMaster();
       
        System.out.println("Cleaning Overlay");
        meteorStarter.terminateAll();           
        
        System.out.println("The application is ready to leave.");
        
        
    }
    
    public boolean isJobDone() {
        Object value;
        value = meteorStarter.getDrtsPeer().getMaster().isJobDone();
        if (value == null)
            return false;
        else
            return (Boolean)value;
        
    }
        
}
