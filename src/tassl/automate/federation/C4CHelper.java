/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tassl.automate.federation;

import ifc2x3javatoolbox.ifc2x3tc1.ClassInterface;
import ifc2x3javatoolbox.ifc2x3tc1.IfcRoot;
import ifc2x3javatoolbox.ifcmodel.IfcModel;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;
import org.reflections.Reflections;
import tassl.automate.federation.simpleapi.FedMeteorGenericWorker;

/**
 *
 * @author Ioan Petri
 */
public class C4CHelper {
    /**
     * c4chelper
     * @param filePath
     * @return 
     */
        public static String generalPath = "c4c";
        public static String path = "c4c/ifcdata/";
        public static String master = "c4c/master.txt/";
	public static String versions = "versions.txt";
	public static String metadata = "metadata.txt";
	public static String disciplines = "c4c/disciplines.txt";
	public static String disciplinesAll = "c4c/disciplinesAll.txt";
	public static String statuses = "c4c/statuses.txt";
	public static String statusesAll = "c4c/statusesAll.txt";
	public static String stages = "c4c/src/stages.txt";
	public static String stagesAll = "c4c/stagesAll.txt";
	public static String access = "c4c/access.txt";
	public static String credential = "c4c/credential.txt";
	public static String events = "c4c/events.txt";
	public static String objects = "c4c/objects.txt";
        public static String topic = "c4c/topic.txt";
        public static String metadataPath = "c4c/metadata.txt";
        public static String versionPath = "c4c/versions.txt";
        public static String resultModel=null;
        public static boolean uploadModel=false;
        public static ArrayList<HashMap<String,String>> finalModels=null;
        public static ArrayList<String> modelNames=null;
        public static ArrayList<String[]> regArgs1=null;
        public static ArrayList<String[]> regArgs2=null;
        public static ArrayList<HashMap<String,String>> models=null;
         public static String resultConfig=null;
        
    // -------------------AUTHENTICATION METHODS_-----------------------------
	public static String authenticate(String username, String pass)
			throws IOException {
		boolean userExist = false;
		String result = null;
		if (username == null || pass == null) {
			userExist = false;
			username = "";
			pass = "";
		}
		try {
			FileInputStream fis = new FileInputStream(credential);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fis));
			int i = 0;
			while (reader.ready()) {
				String lineIWant = reader.readLine();

				if (lineIWant.contains(username) && (lineIWant.contains(pass))) {
					result = genID();
				} else {
					result = "";
				}
				message(result);
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Error");
		}
		return result;
	}

	public static String isValid(String token) {
		if (token != "")
			return token;
		else
			return token;

	}

	private static String genID() {
		String newID = UUID.randomUUID().toString();
		String newIDs = newID.replace("-", "");
		return newIDs;
	}

	public static String getDate() {
		Date obDate = new Date();
		SimpleDateFormat obDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd-HH:mm:ss");
		return obDateFormat.format(obDate.getTime());
	}

	public static void message(String mes) {
		System.out.println(mes);
	}

	public static void error(String mes) {
		System.err.println("Output:" + mes);
	}

	// -----------------------METADATA MANAGEMENT METHODS----------------------

	// ----------------------Operate on disciplines----------------
	public static String[] getDisciplines(String token) throws IOException {
		String[] res = null;
		if (!isValid(token).equals("")) {
			res = getRowsNew(disciplinesAll);
		}
		return res;
	}
	
	//--- modified after release
	public static String getDisciplineById(String id, String token)
			throws IOException {
		String res = null;
		if (!isValid(token).equals("")) {
			res = getRowNew(disciplinesAll, id);
		}
		return res;
	}
	
	
	public static String getDisciplineId(String token)
			throws Exception {
		String [] res = null;
		String res1 = null;
		if (!isValid(token).equals("")) {
			res = getRowsNew(disciplines);
		}
		//System.out.println("++++++++Local discipline is: "+res[0]);
		return res[0].substring(0, 1);
        }
        
        public static String getSuitability(String token)
			throws Exception {
		String [] res = null;
		if (!isValid(token).equals("")) {
			res = getRowsNew(metadata);
		}
		//System.out.println("++++++++Local discipline is: "+res[0]);
                String fin=res[0].replaceAll("\\s+", "");//remove spaces here
		return fin.substring(24, 25);
        }
	
    /**
     *
     * @param token
     * @return
     * @throws IOException
     */
        public static String getDiscipline(String token)
			throws IOException {
		String [] res = null;
		String res1 = null;
		if (!isValid(token).equals("")) {
			res = getRowsNew(disciplines);
		}
		//System.out.println("++++++++Local discipline is: "+res[0]);
		return res[0];
	}

	public static void addDiscipline(String id, String newValue, String token)
			throws IOException {
		String toWrite = id + "   " + newValue;
		if (!isValid(token).equals("")) {
			addRowNew(disciplinesAll, toWrite);
		}

	}

	public static void modifyDiscipline(String id, String newValue, String token)
			throws IOException {
		String toWrite = id + "   " + newValue;
		if (!isValid(token).equals("")) {
			modifyRowNew(disciplinesAll, id, newValue);
		}

	}

	public static void deleteDiscipline(String id, String token)
			throws IOException {
		if (!isValid(token).equals("")) {
			deleteRowNew(disciplinesAll, id);
		}

	}

	// -----------------OPERATE ON STAGES----------------------------------

	public static String[] getStages(String token) throws IOException {
		String[] res = null;
		if (!isValid(token).equals("")) {
			res = getRowsNew(stagesAll);
		}
		return res;
	}

	public static String getStage(String id, String token) throws IOException {
		String res = null;
		if (!isValid(token).equals("")) {
			res = getRowNew(stagesAll, id);
		}
		return res;
	}

	public static void addStage(String id, String newValue, String token)
			throws IOException {
		String toWrite = id + "   " + newValue;
		if (!isValid(token).equals("")) {
			addRowNew(stagesAll, toWrite);
		}

	}

	public static void modifyStage(String id, String newValue, String token)
			throws IOException {
		String toWrite = id + "   " + newValue;
		if (!isValid(token).equals("")) {
			modifyRowNew(stagesAll, id, newValue);
		}

	}

	public static void deleteStage(String id, String token) throws IOException {
		if (!isValid(token).equals("")) {
			deleteRowNew(stagesAll, id);
		}

	}

	// -----------------OPERATE ON STATUSES----------------------------------

	public static String[] getStatuses(String token) throws IOException {
		String[] res = null;
		if (!isValid(token).equals("")) {
			res = getRowsNew(statusesAll);
		}
		return res;
	}

	public static String getStatus(String id, String token) throws IOException {
		String res = null;
		if (!isValid(token).equals("")) {
			res = getRowNew(statusesAll, id);
		}
		return res;
	}

	public static void addStatus(String id, String newValue, String token)
			throws IOException {
		String toWrite = id + "   " + newValue;
		if (!isValid(token).equals("")) {
			addRowNew(statusesAll, toWrite);
		}

	}

	public static void modifyStatus(String id, String newValue, String token)
			throws IOException {
		String toWrite = id + "   " + newValue;
		if (!isValid(token).equals("")) {
			modifyRowNew(statusesAll, id, newValue);
		}

	}

	public static void deleteStatus(String id, String token) throws IOException {
		if (!isValid(token).equals("")) {
			deleteRowNew(statusesAll, id);
		}
	}

	// ---------------------OPERATE ON PERMISSION LIST-----------------

	public static String getPermissionList(String source, String target,
			String token) throws IOException {
		String res = null;
		if (!isValid(token).equals("")) {
			res = getRowNew(access, source + " " + target);
		}
		return res;
	}

	public static void setPermissionList(String source, String target,
			String list, String token) throws IOException {
		String toWrite = source + " " + target + " " + list;
		if (!isValid(token).equals("")) {
			addRowNew(access, toWrite);
		}
	}

	// --------------------------OPERATE ON USERS-------------------
	public static void addUser(String userName, String password,
			String discipline, String token) throws IOException {
		String toWrite = userName + " " + password + " " + discipline;
		if (!isValid(token).equals("")) {
			addRowNew(credential, toWrite);
		}
	}

	public static void deleteUser(String userName, String token)
			throws IOException {
		if (!isValid(token).equals("")) {
			deleteRowNew(credential, userName);
		}
	}

	public static void modifyUser(String userName, String newPassword,
			String newDiscipline, String token) throws IOException {
		String toWrite = newPassword + "   " + newDiscipline;
		if (!isValid(token).equals("")) {
			modifyRowNew(credential, userName, toWrite);
		}
	}

	// -------------------GENERAL PROJECT METHODS -----------------
	// -------------------OPERATE ON PROJECT, METADATA,MODEL-----------------
	// PLEASE CONSIDER getModel and updateModel
	
		
	public static String getObjectMetadataOld(String guid)
			throws IOException {
		String res = "";
		
			res = getRowNew(metadata, guid).substring(26,28);
			System.out.println("---"+res);

		
		return res;
	}

	public static void setProjectMetaDataOld(String total,
			String token) throws Exception {
		String [] toWrite = total.split("/n");
                System.out.println("Inside old" +toWrite);
		if (!isValid(token).equals("")) {
                    
                        for (int i=0;i<toWrite.length;i++){
                        addRowNew(metadataPath, toWrite[i]);
                        System.out.println("adding a new meta from client" +toWrite[i]);
                        }
		}
		
	}
	
	public static String setProjectVersionOld(String total,
			String token) throws IOException {
		String [] toWrite = total.split("/n");
                
		if (!isValid(token).equals("")) {
                        for (int i=0;i<toWrite.length;i++){
                        addRowNew(disciplines, toWrite[i]);
                        }
		}
		return null;
	}

	public static String[] getAllProjectMetaData(String token)
			throws IOException {
		// key is a pair of metadata type and project name
		String[] res = null;
		;
		if (!isValid(token).equals("")) {
			res = getRowsNew(metadata);
		}
		return res;
	}

	public static String getProjectStage(String stage, String token)
			throws IOException {
		String res = null;
		if (!isValid(token).equals("")) {
			res = getRowNew(stages, stage);
		}
		return res;
	}

	public static void updateProjectStage(String id, String newValue,
			String token) throws IOException {
		if (!isValid(token).equals("")) {
			modifyRowNew(stages, id, newValue);
		}

	}

        
// Object manipulation starts here        
	private static boolean checkObjectVisibility(String string) {
		// TODO Auto-generated method stub
		return true;
	}
	
	
         public static String getObjectGuidFromString(String object) {
            object=object.substring(object.indexOf("(")+1).trim();
            object=object.substring(0,object.lastIndexOf(")")).trim();
            String[] objectParam=object.split(",");
            if (objectParam[0].contains("'")) {
                return objectParam[0].substring(1,objectParam[0].length()-1);
            } else {
                return null;
            }
       }
	
	public static boolean objectCompare(ClassInterface entry,ClassInterface entry1) {
		String object1=entry.getStepLine();
		String object2=entry1.getStepLine();
		
		object2=object2.substring(object2.indexOf("=")+1).trim();
		object1=object1.substring(object1.indexOf("=")+1).trim();
		
		String object1name=object1.substring(0,object1.indexOf("("));
		String object2name=object2.substring(0,object2.indexOf("("));
		if (!object1name.equals(object2name)) return false;
		
		object2=object2.substring(object2.indexOf("(")+1).trim();
		object1=object1.substring(object1.indexOf("(")+1).trim();
		object2=object2.substring(0,object2.lastIndexOf(")")).trim();
		object1=object1.substring(0,object1.lastIndexOf(")")).trim();
		
		String[] object1Param=object1.split(",");
		String[] object2Param=object2.split(",");
		//Ioan
		for (int i=0; i < object1Param.length;i++) {
			for (int j=0; j < object2Param.length;j++) {
				if (object1Param[i].equals(object2Param[j]))
				{
					return true;
				} else return false;
					
			}
		}
	
		System.out.println(object1+"\n"+object2);
		
		return false;
	}
        
        //---New Helper version
        private static String dataPath = "ifcdata";
        
        private static HashSet<String> guidTypes;
	
	private static HashMap<String,ArrayList<String[]>> rows=null;
        
        

   
	private static void init() {
		if (guidTypes==null ) {
			guidTypes=new HashSet<String>();
			Reflections reflections = new Reflections("ifc2x3javatoolbox.ifc2x3tc1");
			Set<Class<? extends IfcRoot>> subTypes = reflections.getSubTypesOf(IfcRoot.class);
			for (Class c: subTypes) guidTypes.add(c.getSimpleName().toUpperCase());
		}
	}
	
	private static String getObjectName(String object) {
		return object.substring(object.indexOf("=")+1,object.indexOf("(")).trim();
	}
	
	private static String getObjectGuid(String object) {
        object=object.substring(object.indexOf("(")+1).trim();
        object=object.substring(0,object.lastIndexOf(")")).trim();
        String[] objectParam=object.split(",");
        if (objectParam[0].contains("'")) {
			return objectParam[0].substring(1,objectParam[0].length()-1);
        } else {
			return null;
        }
    }
	
	static ArrayList<String[]> getRows(String file) {
		if (rows==null) rows=new HashMap<String,ArrayList<String[]>>();
		if (rows.keySet().contains(file)) return rows.get(file);
	
		ArrayList<String[]> nrows = new ArrayList<String[]>();
		String fileName=generalPath+File.separator+file;
		try {
			if ( !(new File(fileName).exists())) return new ArrayList<String[]>();
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			while (reader.ready()) {
				String disc=reader.readLine();
				nrows.add(disc.split("\t"));
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		rows.put(file,nrows);
		return nrows;
	}
	
	private static String implode(String[] data) {
		StringBuffer str=new StringBuffer();
		for (int i=0; i< data.length;i++) str.append(data[i]).append("\t");
		return str.toString();
	}
	
	private static void addRow(String file, String[] toWrite) {
		String fileName=generalPath+File.separator+file;
		File a2 = new File(fileName);
		String data=implode(toWrite);
                if (data==null) System.out.println("ERROR:"+file);
		try {
			if (!a2.exists()) a2.createNewFile();
			BufferedWriter out = new BufferedWriter(new FileWriter(a2, true));
			out.append(data);
			out.newLine();
			//System.out.println("Row added:"+data);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
                //System.out.println("Adding:"+file+"::"+toWrite);
                if (rows==null) rows=new HashMap<String,ArrayList<String[]>>();
                if (!rows.containsKey(file)) rows.put(file, new ArrayList<String[]>());
                rows.get(file).add(toWrite);
	}
	
        private static ArrayList<String> getMetadataObjIds(String discipline) {
		ArrayList<String> left = new ArrayList<String>();
		ArrayList<String[]> data=getRows(metadata);
		for (int i = 0; i < data.size(); i++) if (data.get(i)!=null && data.get(i)[2].equals(discipline)) left.add(data.get(i)[0]);
		return left;
	}
	
	private static ArrayList<String> getMetadataObjIds() {
		ArrayList<String> left = new ArrayList<String>();
		ArrayList<String[]> data=getRows(metadata);
		for (int i = 0; i < data.size(); i++) if (data.get(i)!=null) left.add(data.get(i)[0]);
		return left;
	}
	
	public static void setProjectMetaData(String name, String initialVersion,String discipline,String suitability,String token) {
		addRow(metadata,  new String[] {name, initialVersion,discipline,suitability});
	}
	
	public static void setProjectVersion(String name, String oldVersion,String newVersion,String newSuitability, String token)  {
		addRow(versions, new String[] {name, oldVersion,newVersion,newSuitability});
	}
	
	public static String[] getObjectMetadata(String guid) {
		ArrayList<String[]> b=getRows(metadata);
		for (int i = 0; i < b.size(); i++) {
			if (b.get(i)!=null && b.get(i)[0].equals(guid)) {
				return b.get(i);
			}
		}
		return null;
	}
        
        public static ArrayList<String []> getAllRows(String file) throws IOException {

		FileInputStream fis = null;
		BufferedReader reader = null;
		ArrayList<String []> left = new ArrayList<String []>();
		String disc = "";
		try {
			fis = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fis));
                        System.out.println("---Retrieving from file get....." +file);
			while (reader.ready()) {
				String lineIWant = reader.readLine();
				disc = lineIWant;
				//left.add(disc);
                                left.add(disc.split("\t"));
				
                                System.out.println("Retrieved ....:" +disc);
				}
		} catch (FileNotFoundException ex) {
		}
		//String[] stringArray = left.toArray(new String[left.size()]);
		reader.close();
		fis.close();
		return left;

	}
        
        public static ArrayList<String[]> getAllMetadata(String token) {
                ArrayList<String[]> metadataWait=null;
		try{
                Thread.sleep(10000);
                metadataWait=getAllRows(metadataPath);
                while (metadataWait.isEmpty()) {
                    System.out.println("Waiting in metadata....");
                     //metadataWait=getRows(metadata);
                     Thread.sleep(10000);
                }
                } catch (Exception e){
                    e.printStackTrace();
                }
                
                
            return metadataWait;
	}
        
        public static ArrayList<String[]> getAllVersions(String token) {
	ArrayList<String[]> metadataWait=null;
		try{
                //Thread.sleep(10000);
                metadataWait=getAllRows(versionPath);
                while (metadataWait.isEmpty()) {
                    System.out.println("Waiting in versions....no version yet");
                    break;
                     //metadataWait=getRows(metadata);
                    // Thread.sleep(10000);
                }
                //continue;
                } catch (Exception e){
                    e.printStackTrace();
                }
                
                
            return metadataWait;
	}
	
	
	private static String checkObjectVersion(String guid)  {
		
		String highestVersion=null;
		ArrayList<String[]> data=getRows(versions);
		
		for (int i = 0; i < data.size(); i++) {
			if (data.get(i)!=null && data.get(i)[0].equals(guid)) highestVersion=data.get(i)[2];
		}
		
		if (highestVersion==null) return getObjectMetadata(guid)[1];
		else return highestVersion;
	}
	
	public static String getObjectDiscipline(String guid) throws IOException
	{
		ArrayList<String> left = new ArrayList<String>();
		String [] b=getRowsNew(metadata);
		String res="";
		for (int i = 0; i < b.length; i++) {
			if (b[i]!=null && b[i].contains(guid)) 
			{
				
                            res=b[i].substring(29,30);
                            //System.out.println("---Check disc: "+res);
			}
		}
		
		return res;
	}
        
        public static String getObjectSuitability(String guid) throws IOException
	{
		ArrayList<String> left = new ArrayList<String>();
		String [] b=getRowsNew(metadata);
		String res="";
		for (int i = 0; i < b.length; i++) {
			if (b[i]!=null && b[i].contains(guid)) 
			{
				
                            res=b[i].substring(33,34);
                            //System.out.println("---Check disc: "+res);
			}
		}
		
		return res;
	}
	
		public static void updateModel(String mess,String suitability,HashMap<String,String> suitabilities,String token) throws Exception {
				init();
				File c4cIfcData = new File(generalPath+File.separator+dataPath);
                c4cIfcData.mkdirs();
                File[] files=c4cIfcData.listFiles();
                int[] versionFiles=new int[files.length];
               
                int version;
                String metadataRemote=null;
                String versionRemote=null;
                regArgs1=new ArrayList<String[]>();
                regArgs2=new ArrayList<String[]>();
                String discipline=C4CHelper.getDiscipline(token);
                ArrayList<String> listMetadata = new ArrayList<String>();
                ArrayList<String> listVersion = new ArrayList<String>();
                for (int i=0; i < files.length;i++) versionFiles[i]=Integer.parseInt(files[i].getName());
                if (versionFiles.length==0) {
                    version=0;
                } else {
                    Arrays.sort(versionFiles);
                    version=versionFiles[versionFiles.length-1]+1;
                }
               
		
                try { 
                    BufferedWriter outputFile=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(generalPath+File.separator+dataPath+File.separator+version))));
                    String[] objectsList=mess.split("\n");
                    ArrayList<String> guids=getMetadataObjIds(discipline.substring(0,1));
                    System.out.println("No Guids:"+guids.size());
                    //for (int i=0; i < guids.size();i++) System.out.print(guids.get(i));
                    boolean finished=false;
                    for (String object : objectsList) {
                        if (finished){
                            outputFile.write(object);
                            outputFile.newLine();
                            continue;
                        }
                        if (object.equals("--")) {
                            outputFile.write("--");
                            outputFile.newLine();
                            finished=true;
                            continue;
                        }
						outputFile.write(object);
						outputFile.newLine();
						//if a guid object
						if (guidTypes.contains(getObjectName(object))) {
							String guid=getObjectGuid(object);
							if (guid!=null && guids.contains(guid)) {
								System.out.println("Create new version of "+getObjectName(object)+":"+guid);
								String newVersion=checkObjectVersion(guid);
                                                                String newSuitability=suitability;
                                                                if (suitabilities.containsKey(guid)) newSuitability=suitabilities.get(guid);
                                                                setProjectVersion(guid,newVersion,""+version,newSuitability, token);
                                                                regArgs1.add(new String[]{guid,newVersion,""+version,""+newSuitability});
                                                                
							} else if (guid!=null && guid.length()==22) {
                                                                if (suitabilities.containsKey(guid)) setProjectMetaData(guid,""+version,discipline.substring(0, 1),suitabilities.get(guid), token);
                                                                else setProjectMetaData(guid,""+version,discipline.substring(0, 1),suitability, token);
                                                                regArgs2.add(new String[]{guid,""+version,discipline.substring(0, 1),suitability});
							}
						}
                                               // System.out.println("Received "+objectsList.length +"objects with suitability " +suitability);
                    }
                     
                                                                
                    outputFile.flush();
                    outputFile.close();
                    
                } catch (IOException e) {
                    e.printStackTrace();
                }

	}
	
	private static ArrayList<HashMap<String,String>> readIfcDataFile(String version) {
		ArrayList<HashMap<String,String>> model=new ArrayList<HashMap<String,String>>();
		model.add(new HashMap<String,String>());
		model.add(new HashMap<String,String>());
		try{
			BufferedReader file=new BufferedReader(new InputStreamReader(new FileInputStream(generalPath+File.separator+dataPath+File.separator+version)));
			String line;
			StringBuffer str=new StringBuffer();
			while ( (line=file.readLine())!=null) {
				if (line.contains("IfcProject")) continue;
				if (line.equals("--")) break;
				String[] proc=line.split("=");
				model.get(0).put(proc[0].trim(),proc[1].trim());
			}
			while ( (line=file.readLine())!=null) {
				String[] proc=line.split(":");
				model.get(1).put(proc[0],proc[1]);
			}
		} catch (Exception e) {
			//e.printStackTrace();
                    System.out.println("[Exception] No model on this discipline!");
		}
		return model;
	}
	
	public static String  getCurrentModel(String token) {
				System.out.println("Merging Model..");
				init();
                ArrayList<String> guidList=getMetadataObjIds();
                HashMap<String,HashSet<String>> neededObjects=new HashMap<String,HashSet<String>>();
                
                for (String guid: guidList) {
                    if (checkObjectVisibility(guid)) {
                        String version=checkObjectVersion(guid);
                        if (!neededObjects.containsKey(version)) neededObjects.put(version,new HashSet<String>());
                        neededObjects.get(version).add(guid);
                    }
                }
                String[] versions=neededObjects.keySet().toArray(new String[0]);
                Arrays.sort(versions);
				for (int i=0; i < versions.length;i++) System.out.println("Need version:"+versions[i]);
                
            
                HashMap<String,String> model=null;
				HashSet<String> modelGuidList=new HashSet<String>();
				
				//load in version 0
				try {
					ArrayList<HashMap<String,String>> modelTmp=readIfcDataFile("0");
					model=modelTmp.get(0);
                                        for (String oldModelId:model.keySet()) {
						String modelData=model.get(oldModelId);
                                                if (guidTypes.contains(getObjectName(modelData))){ 
                                                       modelGuidList.add(getObjectGuid(modelData));
                                                }
                                        }
				} catch (Exception e) {
					e.printStackTrace();
				}
			
				
				for (int i=0; i < versions.length;i++) {
					if (versions[i].equals("0")) continue;
					
					System.out.println("PARSING In:"+versions[i]);
					ArrayList<HashMap<String,String>> modelTmp=readIfcDataFile(versions[i]);
					System.out.println("FREAD");
                                        HashMap<String,String> currentModel=modelTmp.get(0);
					HashMap<String,String> ids=modelTmp.get(1);
					String versionHash="#"+versions[i]+"000"+versions[i];
					
					boolean finished=false;
				
					HashMap<String,String> stepIdChanges=new HashMap<String,String>();
					
					//work out what GUID objects we need to link back to in the original model
					for (String oldStepId: model.keySet()) {
						if (guidTypes.contains(getObjectName(model.get(oldStepId)))) {
							String guid=getObjectGuid(model.get(oldStepId));
							if (ids.containsKey(guid)) {
								stepIdChanges.put("#"+ids.get(guid),oldStepId);
								System.out.println("#"+ids.get(guid)+":"+oldStepId);
							}
						}
					}
					System.out.println("S1");
					for (String stepId: currentModel.keySet()) {
						if (guidTypes.contains(getObjectName(currentModel.get(stepId)))) {
							
								//is a guid object
								//find its guid in the old model
								String guid=getObjectGuid(currentModel.get(stepId));
								for (String oldModelId:model.keySet()) {
									String modelData=model.get(oldModelId);
									if (guidTypes.contains(getObjectName(modelData)) && getObjectGuid(modelData).equals(guid)) {
										stepIdChanges.put(stepId,oldModelId);
										model.remove(oldModelId);
										break;
									}
								}
							} else {
								//not a guid object replace the GUID with a new one
								String stepIdNone=stepId.substring(1);
								System.out.println(stepId+":"+versionHash+stepIdNone);
								stepIdChanges.put(stepId,versionHash+stepIdNone);
							}
					}
                                        System.out.println("S2");
					//add the currentModel to the new Model
					HashMap<String,String> newCurrentModel=new HashMap<String,String>();
					for (String stepId: currentModel.keySet()) {
						
						//process the step Id changes
						String data=currentModel.get(stepId);
						for (String oldStepId: stepIdChanges.keySet()) {
							String newStepId=stepIdChanges.get(oldStepId);
							data=data.replace(oldStepId+",",newStepId+",").replace(oldStepId+")",newStepId+")").replace(oldStepId+"=",newStepId+"=");
						}
						if (stepIdChanges.keySet().contains(stepId)) {
							stepId=stepIdChanges.get(stepId);
						}
						if (model.keySet().contains(stepId)) {
							System.out.println("[Error] CLASH!!!");
							continue;
						}
						System.out.println(stepId+"="+data);
						newCurrentModel.put(stepId,data);
					}
                                        System.out.println("S3");
					model.putAll(newCurrentModel);
					
				}
                                System.out.println("SFINAL");
				StringBuffer str=new StringBuffer();
				for (String stepId: model.keySet()) str.append(stepId).append("=").append(model.get(stepId)).append("\n\r");
				return str.toString();	
	}
        
        public static String getRemoteObjectstoCurrentModel(String guid, String token)
        {
            return null;
        }
        
        
        //LINE CHANGED HERE
	public static HashMap<String,String> getMyCurrentModel(String token) throws Exception {
				System.out.println("[C4C-Notification]Merging my model!");
                                Pattern pattern=Pattern.compile("(#[0-9]*)");
				init();
				//LINE CHANGED HERE
				String disciplineId=getDisciplineId(token);
                ArrayList<String> guidList=getMetadataObjIds();
               
                File[] vs=new File(path).listFiles();//neededObjects.keySet().toArray(new String[0]);
                String[] versions=new String[vs.length];
                for (int i=0; i < vs.length;i++) versions[i]=vs[i].getName();
                Arrays.sort(versions);
		for (int i=0; i < versions.length;i++) System.out.println("[C4C-Notification] Need version:"+versions[i]);
                
            
                HashMap<String,String> model=null;
		HashMap<String,String> modelGuidList=new HashMap<String,String>();		
				
				//load in version 0
				try {
					ArrayList<HashMap<String,String>> modelTmp=readIfcDataFile("0");
					model=modelTmp.get(0);
                                        for (String oldModelId:model.keySet()) {
						String modelData=model.get(oldModelId);
                                                if (guidTypes.contains(getObjectName(modelData))){ 
                                                       modelGuidList.put(getObjectGuid(modelData),oldModelId);
                                                }
                                        }
				} catch (Exception e) {
					e.printStackTrace();
                                        return new HashMap<String,String>();
				}
			
				
				for (int i=0; i < versions.length;i++) {
					if (versions[i].equals("0")) continue;
					
					//System.out.println("PARSING In:"+versions[i]);
					ArrayList<HashMap<String,String>> modelTmp=readIfcDataFile(versions[i]);
					HashMap<String,String> currentModel=modelTmp.get(0);
					HashMap<String,String> ids=modelTmp.get(1);
					String versionHash=versions[i];
					
					boolean finished=false;
				
					HashMap<String,String> stepIdChanges=new HashMap<String,String>();
					//System.out.println("S1");
					//work out what GUID objects we need to link back to in the original model
					for (String guid: modelGuidList.keySet()) {
                                               if (ids.keySet().contains(guid)) stepIdChanges.put("#"+ids.get(guid),modelGuidList.get(guid));
					}
					//System.out.println("S2");
					for (String stepId: currentModel.keySet()) {
						if (guidTypes.contains(getObjectName(currentModel.get(stepId)))) {
							for (String oldModelId:modelGuidList.values()) {
								stepIdChanges.put(stepId,oldModelId);
								model.remove(oldModelId);
                                                        }							
							
						} else {
								//not a guid object replace the GUID with a new one
                                                    String stepIdNone=stepId.substring(1);
						    String zeros="";
						if (versionHash.length()+stepIdNone.length()>8) System.out.println("GetMyCurrentModel: ID too Large");
					            for (int x=0; x < (8-versionHash.length()-stepIdNone.length()); x++) zeros+="0";

								//System.out.println(stepId+":"+versionHash+stepIdNone);
                                                    stepIdChanges.put(stepId,"#"+versionHash+zeros+stepIdNone);
						}
					}
                                        //System.out.println("S3");
					//add the currentModel to the new Model
					HashMap<String,String> newCurrentModel=new HashMap<String,String>();
					for (String stepId: currentModel.keySet()) {
						
						//process the step Id changes
						String data=currentModel.get(stepId);
                                                StringBuffer dataNew=new StringBuffer();
						Matcher m=pattern.matcher(data);
                                                while (m.find()) {
                                                    String current=m.group();
                                                    m.appendReplacement(dataNew,stepIdChanges.get(current));
                                                }
                                                m.appendTail(dataNew);
                                                data=dataNew.toString();
						if (stepIdChanges.keySet().contains(stepId)) {
							stepId=stepIdChanges.get(stepId);
						}
						if (model.keySet().contains(stepId)) {
							//System.out.println("[Error] CLASH!!!");
							continue;
						}
						//System.out.println(stepId+"="+data);
						newCurrentModel.put(stepId,data);
					}
                                        //System.out.println("S4");
					model.putAll(newCurrentModel);
					
				}
			
				StringBuffer str=new StringBuffer();
				for (String stepId: model.keySet()) str.append(stepId).append("=").append(model.get(stepId)).append("\n\r");
                                System.out.println("[C4C-Notification] Merging model finalized!");
				//return str.toString();
                                if (model==null) return new HashMap<String,String>();
                                return model;	
	}

       //---end of new Helper version
	
	// FROM -- TO --- REL
	// 20 --19 ---derivation
	// -------------------OBJECTS MANIPULATION METHODS-----------------

	public static ClassInterface getObject(String guid, String path, String token)
			throws Exception {
		String ol="13UFPxW2n5nv7$WNZ07hI9";
		IfcModel ifcModel = new IfcModel();
		File stepFile = new File(path);
		ifcModel.readStepFile(stepFile);
		System.out.println(stepFile);
		ifc2x3javatoolbox.ifc2x3tc1.IfcProject ifcProject = ifcModel
				.getIfcProject();
		SortedMap<Integer, ClassInterface> ifcObjectMap = ifcModel
				.getIfcObjectsSortedByEntityInstanceName();
		if (ifcObjectMap.containsKey(guid))
			System.out.println("GetObject result is:" + ifcObjectMap.get(guid));
		return ifcObjectMap.get(guid);
	}
	
	
	// ------OBJECT METADATA
	public static void setObjectMetadata(int guid, String meta, String token)
			throws IOException {
		String toWrite = guid + "   " + meta;
		if (!isValid(token).equals("")) {
			addRowNew(metadata, toWrite);
		}

	}

	public static String getObjectMetadata(String guid, String token)
			throws IOException {
		String res = "";
		if (!isValid(token).equals("")) {
			res = getRowNew(metadata, guid);
		}
		return res;
	}

	// -------DERIVATION --------
	public static String[] getDerivedObjects(String guid, String token)
			throws IOException {

		String[] res ;
		ArrayList<String> left = new ArrayList<String>();
		if (!isValid(token).equals("")) {
			res = getSpecifcRowsNew(objects, guid);
			for (int i = 0; i < res.length; i++) {
				if (res[i] != null && res[i].contains("derivation")) {
					left.add(res[i].substring(13, 15));
					System.out.println(res[i]);

				}
			}
		}
		String[] stringArray = left.toArray(new String[left.size()]);
		return stringArray;
	}

	// ------VERSIONING --------
	public static String[] getVersionedObjects(String guid, String token)
			throws IOException {
		String[] res ;
		ArrayList<String> left = new ArrayList<String>();
		if (!isValid(token).equals("")) {
			res = getSpecifcRowsNew(objects, guid);
			for (int i = 0; i < res.length; i++) {
				if (res[i] != null && res[i].contains("version")) {
					left.add(res[i].substring(13, 15));
					System.out.println(res[i]);
				}
			}
		}
		String[] stringArray = left.toArray(new String[left.size()]);
		return stringArray;
	}

	// -------EVENTS -------
	public static String setEvent(int guid, String type, String token)
			throws IOException {
		String res = "";
		String toWrite = guid + "   " + type;
		if (!isValid(token).equals("")) {
			addRowNew(events, toWrite);
		}
		return res;

	}

	public static String getEvent(String guid, String token) throws IOException {
		String res = "";
		if (!isValid(token).equals("")) {
			res = getRowNew(events, guid);
		}
		return res;
	}
	
	
	// -------TOPIC -------
	
	public static String getTopic(String guid, String token) throws IOException {
		String res = null;
		if (!isValid(token).equals("")) {
			res = getRowNew(topic, guid);
		}
		return res;
	}

	public static void addTopic(String guid, String topic1, String type, String token) throws IOException {
		String toWrite = guid + "   " + type+ "   " + topic1;
		if (!isValid(token).equals("")) {
			addRowNew(topic, toWrite);
		}

	}

	public static void modifyTopic(String guid, String newTopic, String type, String token) throws IOException {
		String toWrite = type+ "   " + newTopic;
		if (!isValid(token).equals("")) {
			modifyRowNew(topic, guid,toWrite);
		}

	}

	public static void delTopic(String guid, String token) throws IOException {
		if (!isValid(token).equals("")) {
			deleteRowNew(topic, guid);
		}
	}
        
        public static void modifyRowNew(String file, String key, String newValue)
			throws IOException {

		FileInputStream fis = null;
		BufferedReader reader = null;
		ArrayList<String> left = new ArrayList<String>();
		String disc = "";
		fis = new FileInputStream(file);
		reader = new BufferedReader(new InputStreamReader(fis));
		try {

			int i = 0;
			while (reader.ready()) {
				String lineIWant = reader.readLine();
				if (lineIWant.contains(key)) {
					// disc = lineIWant.replace(key, newValue);
					disc = key + "   " + newValue;
					left.add(disc);
					i++;
				} else {
					disc = lineIWant;
					left.add(disc);
					i++;
				}
			}
			File a2 = new File(file);
			BufferedWriter out = new BufferedWriter(new FileWriter(a2, false));
			for (int j = 0; j < left.size(); j++) {
				if (left.get(j) != null) {
					out.append(left.get(j) + "\r\n");
					System.out.println(left.get(j));
				} else
					j++;
			}
			out.close();
			reader.close();
			fis.close();
		} catch (FileNotFoundException ex) {
		}

	}

	public static String deleteRowNew(String file, String key) throws IOException {

		FileInputStream fis = null;
		BufferedReader reader = null;
		ArrayList<String> left = new ArrayList<String>();
		String disc = "";
		try {
			fis = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fis));
			int i = 0;
			while (reader.ready()) {
				String lineIWant = reader.readLine();
				if (lineIWant.contains(key)) {
					disc = "";
					left.add(disc);
					i++;
				} else {
					disc = lineIWant;
					left.add(disc);
					i++;
				}
			}
			File a2 = new File(file);
			BufferedWriter out = new BufferedWriter(new FileWriter(a2, false));
			for (int j = 0; j < left.size(); j++) {
				if ((left.get(j) != null) /* && (a[j]!="") */) {
					out.append(left.get(j) + "\r\n");
					System.out.println(left.get(j));
				} else
					j++;
			}
			
			out.close();
			reader.close();
			fis.close();
		} catch (FileNotFoundException ex) {
		}
		String stringArray = left.toString();
		return stringArray;
	}

	public static void addRowNew(String file, String toWrite) throws IOException {
		File a2 = new File(file);
		if (!a2.exists()) {
			//a2.createNewFile();
                    System.out.println("File not found! \t");
		}
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(a2, true));
			out.append(toWrite + "\r\n");
			System.out.println("Row added \t" + toWrite);
			out.close();
		} catch (IOException e) {
		}
	}

	
	//New implementation for metadata methods
	public static String getRowNew(String file, String key) throws IOException {

		FileInputStream fis = null;
		BufferedReader reader = null;
		ArrayList<String> left = new ArrayList<String>();
		String disc = "";
		try {
			fis = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fis));
			int i = 0;
			while (reader.ready()) {
				String lineIWant = reader.readLine();
				if (lineIWant.contains(key)) {
					disc = lineIWant;
					left.add(disc);
					System.out.println(disc);
					i++;
				} else {
					i++;
				}
			}
			
		} catch (FileNotFoundException ex) {
		}
		String stringArray = left.toString();
		reader.close();
		fis.close();
		return stringArray;
	}

	public static String[] getRowsNew(String file) throws IOException {

		FileInputStream fis = null;
		BufferedReader reader = null;
		ArrayList<String> left = new ArrayList<String>();
		String disc = "";
		try {
			fis = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fis));
			while (reader.ready()) {
				String lineIWant = reader.readLine();
				disc = lineIWant;
				left.add(disc);
				//System.out.println("---Line RETRIEVED--:" +disc);
				}
		} catch (FileNotFoundException ex) {
		}
		String[] stringArray = left.toArray(new String[left.size()]);
		reader.close();
		fis.close();
		return stringArray;

	}

	public static String[] getSpecifcRowsNew(String file, String key)
			throws IOException {

		FileInputStream fis = null;
		BufferedReader reader = null;
		ArrayList<String> left = new ArrayList<String>();
		String disc = "";
		try {
			fis = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fis));
			int i = 0;
			while (reader.ready()) {
				String lineIWant = reader.readLine();
				if (lineIWant.contains(key)) {
					disc = lineIWant;
					left.add(disc);
					System.out.println(disc);
					i++;

				} else {
					i++;
				}
			}
		} catch (FileNotFoundException ex) {
		}
		String[] stringArray = left.toArray(new String[left.size()]);
		reader.close();
		fis.close();
		return stringArray;

	}
	
	public static String getIdFromRow(int from, int to, String str) {
		String res = "";
		res = str.substring(from, to);
		return res;

	}

	
	
        public static String getObjectToString(String guid, String token) throws Exception
	{
		String res=null;
		//String path="C://Users/Ioan/workspace-aws/c4c-api/src/AC11-Institute-Var-2-en-IFC.ifc";
		IfcModel ifcModel1=new IfcModel();
		ifcModel1.readStepFile(new File(path));
                
		ClassInterface listCollection1=ifcModel1.getIfcObjectByID(guid);
		StringBuffer str=new StringBuffer();

		String step=listCollection1.getStepLine();
		System.out.println("Object retrieved-->"+step);
		str.append(step).append("\n");
                //It writes the new object--Modified by Ioan
               // ifcModel1.writeStepfile(new File(path));
		return str.toString();
	}
        
        public static String addObjectFromString(String str, String token) throws Exception
	{
		IfcModel ifcModel2=new IfcModel();
		File ifcFile=new File(path);
		if (ifcFile.exists()) ifcModel2.readStepFile(ifcFile);
		
		ifcModel2.addNewIfcObjectFromString(str.toString());
		ifcModel2.writeStepfile(ifcFile);
		System.out.println("Object added-->"+str.toString());
		return str.toString();
	}
        
        
        //Old helper implementationllll
        
public  static String makeMergingModel(ArrayList<HashMap<String,String>> models, HashSet<String> guidList,String token) throws Exception {        
    init();
    // ArrayList<HashMap<String,String>> models=null; // this is an arraylistof models that have come from the other disciplines...
    StringBuffer finalModel=new StringBuffer();
    
    //find the project ids in the first model
        String mainProjectStepId=null;
        String mainProjectGuid=null;
    //String projectStepId=null;
    //String projectGuid=null;
    for (String stepId: models.get(0).keySet()) {
        
            if (getObjectName(models.get(0).get(stepId)).equals("IFCPROJECT")) {
                mainProjectStepId=stepId;
                mainProjectGuid=getObjectGuid(models.get(0).get(stepId));
                break;
            }
    }

        if (mainProjectStepId==null || mainProjectGuid==null) {
           // System.out.println("[Error] No IfcProject!");
        }
        Pattern pattern=Pattern.compile("(#[0-9]*)");

   // System.out.println("STARTING!!!");
    //we start at 0 as the first model we don't need to merge this model
  for (int i=0; i < models.size();i++) {
      System.out.println("[Notification-Receiver] Model:"+i);
                 HashMap<String,String> newOld=new HashMap<String,String>();
                //for each model merging in... first find the IFC project
                String projectStepId=null;
                String projectGuid=null;
                 if (i!=0) {
                    for (String stepId: models.get(i).keySet()) {
                        if (getObjectName(models.get(i).get(stepId)).equals("IFCPROJECT")) {
                            projectStepId=stepId;
                            newOld.put(stepId,mainProjectStepId);
                            projectGuid=getObjectGuid(models.get(i).get(stepId));
                            break;
                        }
                    }
                if (projectGuid==null) {
                    //System.out.println("[Error] No IfcProject!");
                }
           }

        //now look through all objects updating them and inserting them into the new model
        StringBuffer str=new StringBuffer();         
        for (String stepId: models.get(i).keySet()) {
            if (i!=0 && stepId.equals(projectStepId)) continue;
           
           //change the step Ids
            String newStepId=""+(i+1);
            String oldStepId=stepId.replace("#","");
		if (newStepId.length()+oldStepId.length() > 9 ) System.out.println("ID too long!");
            for (int x=0; x < (9-newStepId.length()-oldStepId.length()); x++) newStepId=newStepId+"0";
           
            newStepId="#"+newStepId+oldStepId;
            newOld.put(stepId,newStepId);
            if (i==0 && mainProjectStepId.equals(stepId)) mainProjectStepId=newStepId;
            String line=models.get(i).get(stepId);
            if (i!=0) line=line.replace("'"+projectGuid+"'","'"+mainProjectGuid+"'");
           String newLine=stepId+"="+line+"\n";
           if (guidList!=null && guidTypes.contains(C4CHelper.getObjectName(newLine))) { 
               String guid=C4CHelper.getObjectGuid(newLine);
               if (guidList.contains(guid)) str.append(newLine);
           } else str.append(newLine);
        }
       
      
       Matcher m=pattern.matcher(str);
        while (m.find()) {
           String current=m.group();
           if (i!=0) {
                  //change any references to the IFCProject

          //  line=line.replace(projectStepId+",",mainProjectStepId+",").replace(projectStepId+")",mainProjectStepId+")");
            //

           }    
           if (newOld.get(current)!=null) m.appendReplacement(finalModel,newOld.get(current));
        }
        m.appendTail(finalModel); 
        
    }
    //System.out.println("END OF MERGE CALL..."); 
    
    return finalModel.toString();
    }
//Discipline, Suitability, Object Type
//What they don't want to see
//What they want to see
//String [] suit -- to filter
//Suit [] disc -- to filter
//comma separeted values



    public  static String getFilteredModel(ArrayList<HashMap<String,String>> models, String [] suit, String [] disc,String token) throws Exception {        
        HashSet<String> guidList=new HashSet<String>();
       
        Iterator<HashMap<String,String>> iter=models.iterator();
        int i=0;
        while (iter.hasNext()) {
            iter.next();
            if (ArrayUtils.contains(disc,C4CHelper.modelNames.get(i))) iter.remove();
            i++;
        }
        
       // ArrayList<String> originalGuids=C4CHelper.getMetadataObjIds();
        // ArrayList<String[]> originalGuids=C4CHelper.getRows(metadata);
   //      System.out.println("ORIGINAL SIZE:"+originalGuids.size());
       // for (int i=0; i < originalGuids.size();i++) {
     //       System.out.println(originalGuids.get(i)[3]+"=="+suit[0]+"::"+originalGuids.get(i)[2]);
         //   if (!ArrayUtils.contains(suit,originalGuids.get(i)[3]) && !ArrayUtils.contains(disc,originalGuids.get(i)[2]))//originalGuids.get(i)[2].trim()))
           //     guidList.add(originalGuids.get(i)[0]+":"+originalGuids.get(i)[2]);
//                /System.out.println("[getFilteredModel()] called from socket");
        //}
       
           // ArrayList<HashMap<String,String>> models=null; // this is an arraylistof models that have come from the other disciplines...
        String result=makeMergingModel(models, null,token);
       // System.out.println(guidList.size());
        return result;
    }
}
