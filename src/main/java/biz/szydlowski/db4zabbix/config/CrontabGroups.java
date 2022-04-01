/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.config;

import static biz.szydlowski.db4zabbix.WorkingObjects.CrontabGroupsList;
import biz.szydlowski.utils.OSValidator;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Dominik
 */
public class CrontabGroups {
  
      
    private String _setting="setting/crontab-groups.setting";
    
    static final Logger logger = LogManager.getLogger("biz.szydlowski.sshubix.config.CrontabGroups");

       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
    public CrontabGroups(){         
         
  
    }
    
    public void initData(){
    
      
         if (OSValidator.isUnix()){
              String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
              _setting = absolutePath + "/" + _setting;
         }

     
	InputStream input = null;
        Properties prop = new Properties();
        CrontabGroupsList = new HashMap<>(); 

	try {

		input = new FileInputStream(_setting );
		// load a properties file
		prop.load(input);
                
                Set<Object> keys = getAllKeys( prop );
                keys.stream().map((k) -> (String)k).forEachOrdered((key) -> {
                    if ( getPropertyValue(prop , key).length()==0){                         
                        
                    } else {
                        CrontabGroupsList.put(key, getPropertyValue(prop , key));
                    }
                }); 
                
                prop.clear();


	} catch (IOException ex) {
		logger.error(ex);
	} finally {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

    }
   
   
    private Set<Object> getAllKeys(Properties prop){
            Set<Object> keys = prop.keySet();
            return keys;
    }

    private String getPropertyValue(Properties prop, String key){
            return prop.getProperty(key);
    }  
              
   
}