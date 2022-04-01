/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix;

/**
 *
 * @author szydlowskidom
 */

import static biz.szydlowski.db4zabbix.WorkingObjects.*;
import biz.szydlowski.db4zabbix.api.DBApi;
import biz.szydlowski.db4zabbix.api.DataSource;
import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import biz.szydlowski.db4zabbix.config.DBParams;
import biz.szydlowski.db4zabbix.config.JdbcDriver;
import biz.szydlowski.db4zabbix.config.WebParams;
import biz.szydlowski.db4zabbix.timers.ActiveAgentTask;
import biz.szydlowski.db4zabbix.timers.DataSourceTask;
import biz.szydlowski.db4zabbix.web.WebServer;
import biz.szydlowski.utils.Constans;
import biz.szydlowski.utils.Memory;
import biz.szydlowski.utils.OSValidator;
import biz.szydlowski.utils.WorkingStats;
import biz.szydlowski.utils.ZabbixStatistics;
import biz.szydlowski.utils.tasks.MaintananceTask;
import biz.szydlowski.utils.tasks.TasksWorkspace;
import static biz.szydlowski.utils.tasks.TasksWorkspace.absolutePath;
import static biz.szydlowski.utils.tasks.TasksWorkspace.ActiveAgentTimer_time;
import static biz.szydlowski.utils.tasks.TasksWorkspace.MaintenanceTimer_time;
import biz.szydlowski.utils.tasks.UpdateServer;
import biz.szydlowski.utils.tasks.WatchDogTask;
import biz.szydlowski.zabbixmon.ZabbixServer;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.sql.SQLException;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import static biz.szydlowski.utils.Constans.UPDATE_STR;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;


//http://www.neilson.co.za/creating-a-java-daemon-system-service-for-debian-using-apache-commons-jsvc/

/**
 * A simple Swing-based client for the capitalization server.
 * It has a main frame window with a text field for entering
 * strings and a textarea to see the results of capitalizing
 * them.
 */


//http://www.neilson.co.za/creating-a-java-daemon-system-service-for-debian-using-apache-commons-jsvc/

/**
 * A simple Swing-based client for the capitalization server.
 * It has a main frame window with a text field for entering
 * strings and a textarea to see the results of capitalizing
 * them.
 */
public class DB4ZabbixDaemon implements Daemon {
    
      
    static {
        try {
            System.setProperty("log4j.configurationFile", getJarContainingFolder(DB4ZabbixDaemon.class)+"/setting/log4j/log4j2.xml");
        } catch (Exception ex) {
        }
    }
    
        
    public static Scheduler scheduler; 
    
    static final Logger logger = LogManager.getLogger(DB4ZabbixDaemon.class);
    private static boolean stop = false;
    
    private WebServer _WebServer = null;

    private ZabbixStatistics  _ZabbixStatistics  = null;
   
    public static Timer ActiveAgentTimer = new Timer("ActiveAgentProcessing", true);
    public static Timer DataSourceTimer = new Timer("ActiveAgentProcessing", true);
    private Timer WatchdogTimer = new Timer("Watchdog", true);
    public static Timer MaintenanceTimer = new Timer("MaintenanceTask", true);
    private  SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss");
 
    public static int ACTIVE_AGENT_TIME = 5000;
    public static String APP_NAME="DEFAULT";   
    
    
    public  DB4ZabbixDaemon (){
    }
    
    public DB4ZabbixDaemon(boolean test, boolean win){
         
          if ( test || win){
            if (!win) System.out.println("****** TESTING MODE  ********"); 
            else System.out.println("****** WINDOWS MODE  ********"); 
            try {
                jInit();
                start();
            } catch (Exception ex) {
                logger.error(ex);
            }
        }
    }
    
    
     public void jInit() {            
                  
          UPDATE_STR = new StringBuilder("</br>NOT CHECKED");
        
           if (OSValidator.isUnix()){
              absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"))+"/";
           } else {
               absolutePath="";
           }
         
          
           new UpdateServer("http://www.update.szydlowski.biz/software/db4Zabbix",Version.getBuild(),Version.getAgentVersion()).start();
            
          try {
              scheduler = new StdSchedulerFactory().getScheduler();
          } catch(SchedulerException e){
              logger.error("SchedulerException" + e);
          }
           
           ActiveAgentTimer_time = System.currentTimeMillis();
           MaintenanceTimer_time = System.currentTimeMillis();
           
           TasksWorkspace.start(Version.DEV_VERSION_EXPIRE_DATE, true);           
           WorkingStats.start();      
           
          
            printStarter();
                 
            JdbcDriverApiList = new JdbcDriver().getJdbcDriverApiList();
        
            zabbixServerApiList = new ZabbixServer(absolutePath +"setting/zabbix-server.xml").getZabbixServerApiList();             
            DBApiList = new ArrayList<>();
            
            DBParams db = new DBParams();
            
           _DBQuartz = new DBQuartz();
                 
             WatchdogTimer.schedule(new WatchDogTask(), 1000, 2000);  
             DataSourceTimer.schedule(new DataSourceTask(), 30000, 60000);
             ActiveAgentTimer.schedule(new ActiveAgentTask(), 1000, ACTIVE_AGENT_TIME);
             MaintenanceTimer.schedule(new MaintananceTask(), 10000, 60000);
             
             unique_hostname = new HashMap<>();
             for (ZabbixServerApi zabbixServerApi : zabbixServerApiList) {
                setUniqZabbixHostnameForAgent(zabbixServerApi.getServerName());
             }
           
             initDataSource();
        
            WebParams _WebParams = new WebParams();
            if (_WebParams.isWebConsoleEnable()){ 
                allowedConn = _WebParams.getAllowedConn();
                _WebServer = new WebServer(_WebParams.getWebConsolePort());
                _WebServer.setMaxConnectionCount(_WebParams.getWebMaxConnectionCount());
                _WebServer.start();
            }
            Memory.start();
    }
     
      private void initDataSource(){
  
        
        for (JdbcDriverApi _JdbcApi : JdbcDriverApiList){   
            if (_JdbcApi.getInterfaceType().equals("pool")){
                logger.info("Adding pool id=" + _JdbcApi.getPoolIndex() + " " + _JdbcApi.getInterfaceName() + " - " + _JdbcApi.getHost() + " : " +  _JdbcApi.getStringPort());
                try {
                
                   DataSource.addInstance(_JdbcApi);
                   
              
                } catch (SQLException | IOException | PropertyVetoException e) {
                    logger.error(e);
                    //e.printStackTrace();
                }
                finally {
                    
                      if (DataSource.getDataSourceSize() > _JdbcApi.getPoolIndex()+1){
                              logger.fatal("getDataSourceSize!=pool_index," +DataSource.getDataSourceSize() +"|"+_JdbcApi.getPoolIndex() );
                              System.exit(5);
                       }
                }
            }
        }
            
    } 
    
    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
            System.out.println("**** Daemon init *****");
                     
            handleCmdLine(daemonContext.getArguments());      
            
            String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
               absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
          
             jInit(); 
            
            logger.debug("Current path " + absolutePath);
            
   }

    @Override
    public void start() throws Exception { 
         logger.info("Starting daemon");
         _DBQuartz.doJob();
         logger.info("Started daemon");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stoppping daemon");
    
        Memory.stop();
        _DBQuartz.stop();
        ActiveAgentTimer.cancel();
        MaintenanceTimer.cancel();
        
        DBApiList.clear();
        logger.info("Stopped daemon");
    }
   
    @Override
    public void destroy() { 
        logger.info("Destroying daemon");
         DBApiList = null;
        _DBQuartz = null;
         ActiveAgentTimer = null;
         MaintenanceTimer = null;
    }
     //https://support.google.com/gsa/answer/6316721?hl=en
     public static void start(String[] args) {
        System.out.println("start");
        new DB4ZabbixDaemon(false, true);
        
        while (!stop) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }
 
    public static void stop(String[] args) {
        System.out.println("stop");
        stop = true;
              
        logger.info("Stoppping daemon");
    
        DBApiList.clear();        
        logger.info("Stopped daemon");  
        
        System.exit(0);
                
    }
 
   
    
    public static void main(String[] args)  {
         
         if (args.length>0){
             if (args[0].equalsIgnoreCase("testing")){
                 new DB4ZabbixDaemon(true, false);
             }

         }
            
    }
    
    
    
    private static void printStarter(){
        logger.info(Constans.STARTER);
        logger.info(new Version().getAllInfo());
    }
  

   
      
     public void handleCmdLine(String[] args) {
      
         for (int i = 0; i < args.length; i++) {
           String arg = args[i];
           if (arg.regionMatches(0, "-", 0, 1))  {
            try {
               switch (arg.charAt(1)) { 
               case 'n':
                 i++;
                 break;
                 
               case 'a' :
                  i++;
                  APP_NAME = args[i];
                  break;             

               default:
                 //printUsage(language);
               }
             }
             catch (ArrayIndexOutOfBoundsException ae)  {
              // printUsage(language);
             }
           }
        }
     }
     
     private void setUniqZabbixHostnameForAgent(String zabbix_host){
                     
        String ag="";
        String name="";
        
        for (DBApi prop : DBApiList){
             ag = zabbix_host+"."+prop.getZabbixHost();
             name = prop.getZabbixServerName();
          
            unique_hostname.put(ag, true);
         
        }
        
    }
     
     public static String getJarContainingFolder(Class aclass) throws Exception {
          CodeSource codeSource = aclass.getProtectionDomain().getCodeSource();

          File jarFile;

          if (codeSource.getLocation() != null) {
            jarFile = new File(codeSource.getLocation().toURI());
          }
          else {
            String path = aclass.getResource(aclass.getSimpleName() + ".class").getPath();
            String jarFilePath = path.substring(path.indexOf(":") + 1, path.indexOf("!"));
            jarFilePath = URLDecoder.decode(jarFilePath, "UTF-8");
            jarFile = new File(jarFilePath);
          }
          return jarFile.getParentFile().getAbsolutePath();
     }
 

}