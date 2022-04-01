/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.web;

import static biz.szydlowski.db4zabbix.DB4ZabbixDaemon.APP_NAME;
import static biz.szydlowski.utils.Constans.UPDATE_STR;
import static biz.szydlowski.db4zabbix.WorkingObjects.*;
import biz.szydlowski.db4zabbix.Version;
import biz.szydlowski.db4zabbix.api.DBApi;
import biz.szydlowski.db4zabbix.api.DataSource;
import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import biz.szydlowski.db4zabbix.sqlType;
import biz.szydlowski.utils.AutorestartConfig;
import biz.szydlowski.utils.ZabbixStatistics;
import static biz.szydlowski.utils.tasks.TasksWorkspace.absolutePath;
import static biz.szydlowski.utils.tasks.TasksWorkspace.autorestartApi;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Socket;
import java.net.URLDecoder;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

/**
 *
 * @author Dominik
 */
public class ServerWorkerRunnable implements Runnable {
   
    static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.web.ServerWorkerRunnable");
     protected Socket clientSocket = null;
     protected SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     private boolean isApi=false;
        
      public ServerWorkerRunnable(Socket clientSocket, boolean api) {
            this.clientSocket = clientSocket;
            this.isApi=api;
      }


      @Override
      public void run() {
        try {
           String address = clientSocket.getInetAddress().getHostAddress();
                
       
          InputStream input = this.clientSocket.getInputStream();
          String userInput = "default";

          BufferedReader stdIn = null;
          try {
                stdIn = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
                userInput = stdIn.readLine();
          } catch (IOException ex) {
                logger.error(ex);
          }
 
          if (userInput == null) userInput = "DEFAULT";

          PrintWriter out = new PrintWriter(this.clientSocket.getOutputStream(), true);
        
          boolean isGet=true;
          boolean isPost=false;
                    
          if (!isApi) {
             logger.info("The following client was rejected: "+ address);
        
             String data="<h1>401 UNAUTHORIZED. Authentication required.</h1><b><br/>Sorry, you are not allowed to access this page.</b><br/>";
             String response = "HTTP/1.1 401 UNAUTHORIZED\r\n" +
                    "Content-Length: "+data.length()+"\r\n" +
                    "Content-Type: text/html\r\n\r\n" +
                    data;
              
             out.print(response);
             out.flush();

             out.close();
             input.close();
          
             return;
         } else {
             
             logger.info("The following client has connected: "+ address);
            
         }
          
          if ( !userInput.contains("GET")){
              isGet=false;
          }  else {
              isGet=true;
          }
          
          String postData = "";
          if ( userInput.contains("POST")){
               isPost=true;
             
               String line;
               int postDataI=0;
                while ((line = stdIn.readLine()) != null && (line.length() != 0)) {
                    logger.debug("HTTP-HEADER: " + line);
                    if (line.contains("Content-Length:")) {
                        postDataI = Integer.parseInt(line.substring(line.indexOf("Content-Length:") + 16, line.length()));
                    }
                }
                postData = "";
                // read the post data
                if (postDataI > 0) {
                    char[] charArray = new char[postDataI];
                    stdIn.read(charArray, 0, postDataI);
                    postData = new String(charArray);
                }
                               
                postData =  replaceURL(postData );                                
                 
                logger.debug("post DATA after replace " + postData); 
             
          } else {
              isPost=false;
          }
          
          if (isGet || isPost){
              out.println("HTTP/1.1 200 OK");
              out.println("Content-Type: text/html");
              out.println("<html>\n");
              out.println("<head><meta charset=\"UTF-8\">");
              out.println("<title>DB4Zabbix</title>");
              out.println("</head>\n");
          }
          
          userInput = userInput.replace("GET", "");
          userInput = userInput.replace("HTTP/1.1", "");
          userInput = userInput.replace("/favicon.ico", "");
          userInput = replaceURL(userInput);
         
          if (userInput.length() == 0){
              return;
          }  
          
          if (isGet){
                if (userInput.contains("hosts")){
                   printHosts(out); 
                } else  if (userInput.contains("jdbc")){
                   printJdbc(out); 
                }  else  if (userInput.contains("config_tests")){
                    printConfigTests(out); 
                }  else  if (userInput.contains("tests")){
                   printTests(out); 
                }   else  if (userInput.contains("jobs")){
                   printJobs(out); 
                }    else  if (userInput.contains("threads")){
                  printAllThreads(out); 
                }   else  if (userInput.contains("version")){
                  printVersion(out); 
                }   else  if (userInput.contains("pid")){
                  printPID(out); 
                }  else if (userInput.contains("ping")){ 
                   if (isGet) out.println(new StringBuilder().append("PONG").append("<br/>").toString());
                   else out.println(new StringBuilder().append("PONG").append("<br/>").toString());
                }  else  if (userInput.contains("jvm")){
                  printJVM(out); 
                }  else if (userInput.contains("statistics")) {
                    ZabbixStatistics _Statistics = new ZabbixStatistics (absolutePath +"setting/stats");
                     _Statistics.loadStats();
                    String stat = _Statistics.readStatsToHTMLString();
                    out.println(stat);
                    printHomeAndBack(out);
                } else if (userInput.contains("autorestart")) {                 
                    AutorestartConfig autorestartConfig = new AutorestartConfig(absolutePath +"setting/autorestart.setting");
                    out.println(autorestartConfig.readAutorestartToHTMLString());
                    printHomeAndBack(out);
                   
                } else if (userInput.replaceAll("\\s+", "").equals("/")){   
                    out.println(new StringBuilder().append("<b>Diagnostic console for ").append(Version.getVersion()).append("</b>").toString());
                    if (!APP_NAME.equals("DEFAULT")) out.println(new StringBuilder().append("<br/><b>App ").append(APP_NAME).toString());
                    out.println(new StringBuilder().append("<br/><br/>> <a href=\"version\">Version</a><br/>").toString());
                    out.println(new StringBuilder().append("> <a href=\"pid\">PID</a><br/>").toString());
                    out.println(new StringBuilder().append("> <a href=\"jvm\">JVM</a><br/>").toString());
                    out.println(new StringBuilder().append("> <a href=\"statistics\">Show statistics</a><br/>").toString());
                    out.println(new StringBuilder().append("> <a href=\"autorestart\">Display autorestart setting</a><br/>").toString());
                    out.println(new StringBuilder().append("> <a href=\"hosts\">Display and change status of HOSTS</a><br/>").toString()); 
                    out.println(new StringBuilder().append("> <a href=\"jdbc\">Display and change status of JDBC</a><br/>").toString()); 
                    out.println(new StringBuilder().append("> <a href=\"tests\">Display and change status of all tests</a><br/>").toString()); 
                    out.println(new StringBuilder().append("> <a href=\"config_tests\">Display configuration of all tests</a><br/>").toString());
                    out.println(new StringBuilder().append("> <a href=\"threads\">Display threads</a><br/>").toString()); 
                    out.println(new StringBuilder().append("> <a href=\"jobs\">Display jobs</a><br/>").toString()); 
                    printRestartAndKill(out);
             } else {
                  out.println(new StringBuilder().append("Page not found").append("<br/>").toString());

              }
               
              out.println("</br> SUPPORT TEAM <b> <a href=\"mailto:support@szydlowski.biz?subject="+ Version.getVersion()+"\">support@szydlowski.biz</a></b>");
                 
          } else if (isPost){
                 //  postData; 
                 if ( userInput.contains("actions.api") ){
                      String[] spliter = postData.split("&");
                       String action="default";
                       String alias="default";             
                       String id="default"; 
                                             
                       if ( spliter.length >= 3 && isPost) {

                          for (String spliter1 : spliter) {
                              String[] _sp = spliter1.split("=");
                              if (_sp.length==2){
                                  if (_sp[0].equalsIgnoreCase("action")){
                                      action = _sp[1];
                                  } else  if (_sp[0].equalsIgnoreCase("alias")){
                                      alias = _sp[1];
                                  }  else  if (_sp[0].equalsIgnoreCase("id")){
                                      id = _sp[1];
                                  } else {
                                      out.write(new StringBuilder().append("ERROR: Server problem detected (E410)<br/>\n").toString());
                                  }
                                  
                              } else {
                              }
                          }

                       }
                       
                       if (action.equalsIgnoreCase("host_reverse")){
                       
                               boolean _active = !unique_hostname.get(alias);
                               unique_hostname.put(alias, _active);
                       
                               int dot = alias.indexOf(".");  
                               String agent = "default";
                               String host="default";
                               if (dot > 0 && dot+1<alias.length()){
                                    agent = alias.substring(0, dot);
                                    host=alias.substring(dot+1, alias.length());
                               }


                               if (dot > 0 && dot+1<alias.length()){
                                   for (DBApi dbApi : DBApiList){
                                       if (dbApi.getZabbixHost().equals(host) && dbApi.getZabbixServerName().equals(agent)){
                                           //System.out.println(dbApi.getZabbixKey());
                                            sqlType type = dbApi.getSqlType();

                                            if (_active) {
                                                   dbApi.setToActiveMode();
                                            } else dbApi.setToInactiveMode();
                                            

                                       }
                                   }

                               } //end if ok

                               if ( !_active ){ 
                                    out.write(new StringBuilder().append("<font color=\"red\">INACTIVE</font>").toString());
                               } else {
                                    out.write(new StringBuilder().append("<font color=\"green\">ACTIVE</font>").toString());
                               }
                       } //end reverse
                       else  if (action.equalsIgnoreCase("jdbc_reverse")){
                          int _id =0;
                          _id = Integer.parseInt(id);
                         
                         String interfaceName = JdbcDriverApiList.get(_id).getInterfaceName();
                          
                           
                          boolean _setToActive =  !JdbcDriverApiList.get(_id).isActiveMode();
                          
                          if (_setToActive){
                              if (JdbcDriverApiList.get(_id).getInterfaceType().equalsIgnoreCase("pool")){
                                  try {
                                      DataSource.retry( JdbcDriverApiList.get(_id));
                                  } catch (Exception e){
                                      logger.error(e);
                                  }
                                  logger.debug("OPEN pool " + interfaceName);
                              }
                              JdbcDriverApiList.get(_id).setToActiveMode();                             
                          } else { 
                              if (JdbcDriverApiList.get(_id).getInterfaceType().equalsIgnoreCase("pool")){
                                  for (DataSource ds  : DataSource.getInstances(JdbcDriverApiList.get(_id).getPoolIndex())){
                                      ds.closePool();
                                  }
                                  logger.debug("Close pool " + interfaceName);
                              }
                              JdbcDriverApiList.get(_id).setToInactiveMode();
                          }
                        
                          
                          for (DBApi dbApi : DBApiList){
                               sqlType type = dbApi.getSqlType();
                                if (dbApi.getJdbc_driverSize()>0){
                                   if (dbApi.getJdbc_driver(0).getInterfaceName().equals(interfaceName)){
                                       //System.out.println(dbApi.getZabbixKey());
                                        if ( _setToActive) {
                                               dbApi.setToActiveMode();
                                        } else dbApi.setToInactiveMode();
                                   }
                                }

                           }
                         
                          
                          if ( ! _setToActive ){ 
                                out.write(new StringBuilder().append("<font color=\"red\">INACTIVE</font>").toString());
                          } else {
                                out.write(new StringBuilder().append("<font color=\"green\">ACTIVE</font>").toString());
                          }
                       }  else if (action.equalsIgnoreCase("db_reverse")){  
                          boolean _active = false;
                         
                          int _id =0;
                            
                          _id = Integer.parseInt(id);
                          
                          sqlType type = DBApiList.get(_id).getSqlType();
                          _active  = !DBApiList.get(_id).isActiveMode();
                     

                           if (_active) {
                               DBApiList.get(_id).setToActiveMode();
                           } else DBApiList.get(_id).setToInactiveMode();
                           

                               
                          if ( !_active ){ 
                                out.write(new StringBuilder().append("<font color=\"red\">INACTIVE</font>").toString());
                          } else {
                                out.write(new StringBuilder().append("<font color=\"red\">INACTIVE</font>").toString());
                          }
                           
                       }                      
                       else if (action.equalsIgnoreCase("db_refresh")){ 
                              int _id =0;
                            
                              _id = Integer.parseInt(id);
                              sqlType type = DBApiList.get(_id).getSqlType();
                             
                              String getout="";
                           
                              getout = _DBQuartz.refreshTask(_id);    
                           
                              out.write(new StringBuilder().append("<font color=\"blue\">").append(getout).append("</font>").toString());
                         
                              
                          
                       } else {
                          out.write(new StringBuilder().append("<font color=\"red\">DO NOTHING</font>").toString());
                       }
                      
                } else if ( userInput.contains("restart.api") ){ 
                        String[] _sp = postData.split("=");
                         if (_sp.length==2){
                               if (_sp[1].equals("restart")){
                                    out.write(new StringBuilder().append("<font color=\"red\">RESTART DAEMON</font>").toString());
                                     out.flush();
                                    autorestartApi.restart();
                               } else if (_sp[1].equals("kill")){
                                    out.write(new StringBuilder().append("<font color=\"red\">KILL DAEMON</font>").toString());  
                                    out.flush();
                                    autorestartApi.kill();
                               } else {
                                   out.write(new StringBuilder().append("<font color=\"red\">DO NOTHING</font>").toString());
                               }

                        } else {
                             logger.error("_sp.length!=2");
                        }
                                      
                  
                } else {
                       if (isGet) out.println(new StringBuilder().append("Page/Command not found").append("<br/>").toString());
                       else out.println(new StringBuilder().append("The command not found").append("<br/>").toString());

                }
          }

     
          if (isGet || isPost){
              out.println("</html>");
          }
          
          out.flush();

          out.close();
          input.close();

          logger.debug("Request processed/completed...");
        }
        catch (IOException e) {
          logger.error(e);
        }
    }
      
    private void printHosts(PrintWriter out)  {
        out.print("<h2>HOSTS</h2>\n");  
        out.println("<style>\n table, th, td {\n border: 1px solid black;\n border-collapse: collapse;\n }\n th, td {\n padding: 5px;\n");
        out.println(" text-align: left;   \n}\n </style>");  
       
       //  out.println("<font color=\"red\"><p id=\"info\"></p></font>");
     
        Set set = unique_hostname.entrySet();
        Iterator iterator = set.iterator();
        
        out.println("<table style=\"width:90%\">");      
        out.println("<tr>\n <th>Server</th>\n  <th>Host</th>\n <th>Status</th>\n  <th>Action</th>\n </tr>");
      
        while(iterator.hasNext()) { 
                 Map.Entry mentry = (Map.Entry)iterator.next();
                 
                 String key = mentry.getKey().toString();
                         
                 int dot = key.indexOf(".");  
                 String server = "default";
                 String host="default";
                 if (dot > 0 && dot+1<key.length()){
                        server = key.substring(0, dot);
                        host= key.substring(dot+1, key.length());
                 }
                
                 out.println("<tr>");
                 out.println("<td>");
                 out.print( server );
                 out.println("</td>");
                 out.println("<td>");
                 out.print( host );
                 out.println("</td>"); 
                
                 out.println("<td id=\""+mentry.getKey()+"\">");
                 if (mentry.getValue().toString().equals("true")){
                    out.println("<font color=\"green\">ACTIVE</font>");  
                 } else {
                    out.println("<font color=\"red\">INACTIVE</font>");  
                 }    
                 
                 out.println("<td><button onclick=\"Action('host_reverse','"+mentry.getKey()+"', 'no')\">Active/Deactive</button><br/></td>");
                
                 out.println("</tr>"); 
                 
        }
        
       
        out.println("</table>");
        printApiFunction(out);
        
        printHomeAndBack(out);
        
    }
    
    private void printJdbc(PrintWriter out)  {
        out.print("<h2>JDBC</h2>\n");  
        out.println("<style>\n table, th, td {\n border: 1px solid black;\n border-collapse: collapse;\n }\n th, td {\n padding: 5px;\n");
        out.println(" text-align: left;   \n}\n </style>");  
       
          
        out.println("<table style=\"width:90%\">");      
        out.println("<tr>\n <th>JDBC Interface</th> <th>Interface type</th>\n <th>Status</th>\n <th>host:port</th>\n <th>Connection timeout</th>\n  <th>Connection retry</th>\n  <th>Pool key's</th>\n <th>Action</th>\n </tr>");
      
        int i=0;
        
        for (JdbcDriverApi jdbcApi : JdbcDriverApiList) { 
               
                 out.println("<tr>");
                 out.println("<td>");
                 out.print(jdbcApi.getInterfaceName());
                 out.println("</td>"); 
                 
                 out.println("<td>");
                 out.print(jdbcApi.getInterfaceType());
                 out.println("</td>");
                
                 out.println("<td id=\""+jdbcApi.getInterfaceName()+"\">");
                 if (jdbcApi.isActiveMode()){
                    out.println("<font color=\"green\">ACTIVE</font>");  
                 } else {
                    out.println("<font color=\"red\">INACTIVE</font>");  
                 }  
                 
                 out.println("<td>");
                 out.print(jdbcApi.getHost()+":"+jdbcApi.getStringPort());
                 out.println("</td>");  
                 
                 out.println("<td>");
                 if (jdbcApi.getInterfaceType().equalsIgnoreCase("single")) out.print(jdbcApi.getLoginTimeout());
                 else out.println("look at pool key"); 
                 out.println("</td>"); 
                 
                 out.println("<td>");                
                 if (jdbcApi.getInterfaceType().equalsIgnoreCase("single"))  out.print(jdbcApi.getConnectionRetry());
                 else out.println("look at pool key"); 
                 out.println("</td>");  
                 
                 out.println("<td>");                
                 if (jdbcApi.getInterfaceType().equalsIgnoreCase("pool")){
                     out.println("MinPoolSize="); 
                     out.println(jdbcApi.getMinPoolSize()); 
                     out.println("</br>"); 
                     out.println("MaxPoolSize="); 
                     out.println(jdbcApi.getMaxPoolSize()); 
                     out.println("</br>");  
                     out.println("MaxStatements="); 
                     out.println(jdbcApi.getMaxStatements()); 
                     out.println("</br>");    
                     out.println("AcquireIncrement="); 
                     out.println(jdbcApi.getAcquireIncrement()); 
                     out.println("</br>"); 
                     out.println("LoginTimeout="); 
                     out.println(jdbcApi.getLoginTimeout()); 
                     out.println("</br>"); 
                     out.println("PoolIndex="); 
                     out.println(jdbcApi.getPoolIndex()); 
                     out.println("</br>");
                 }  else out.println("not pooling"); 
                 out.println("</td>"); 
                 
                 out.println("<td><button onclick=\"Action('jdbc_reverse','"+jdbcApi.getInterfaceName()+"', '"+i+"')\">Active/Deactive</button><br/></td>");
                
                 out.println("</tr>"); 
                 
                 i++;
                 
        }
        
       
        out.println("</table>");
        printApiFunction(out);
        
        printHomeAndBack(out);
        
    }
     
   
    private void printTests(PrintWriter out)  {
        out.print("<h2>TESTS</h2>\n");  
        out.println("<style>\n table, th, td {\n border: 1px solid black;\n border-collapse: collapse;\n }\n th, td {\n padding: 5px;\n");
        out.println(" text-align: left;   \n}\n </style>");  
          
        
        out.println("<table style=\"width:90%\">");      
        out.println("<tr>\n <th>ID</th>\n <th>Zabbix server</th>\n  <th>Zabbix host</th>\n  <th>Zabbix key</th>\n <th>Type</th>\n <th>VALUE</th>\n   <th>Status</th>\n <th>Last execution time</th>\n <th>Action</th>\n <th>Refresh</th>\n  </tr>");
        
        int idx=0;
      
        for (DBApi dbApi : DBApiList){
                     
                 out.println("<tr>"); 
                 
                 out.println("<td>");
                 out.print( idx  );
                 out.println("</td>");
                 
                 out.println("<td>");
                 out.print( dbApi.getZabbixServerName() );
                 out.println("</td>");
             
                 out.println("<td>");
                 out.print( dbApi.getZabbixHost() );
                 out.println("</td>");
                 
                 out.println("<td>");
                 out.print( dbApi.getZabbixKey() );
                 out.println("</td>");
                 
                 out.println("<td>");
                 out.print( dbApi.getSqlType() );                 
                 out.println("</td>");
                 
                 out.println("<td>");
                 
                 boolean discovered=false;
                 boolean multicolumn=false;
                 if (dbApi.getSqlType()==sqlType.DISCOVERY_QUERY ||
                        dbApi.getSqlType()==sqlType.PURE_WITH_MULTIPLE  ||
                        dbApi.getSqlType()==sqlType.DISCOVERY_STATIC ){
                        discovered=true;
                 } else if (dbApi.getSqlType()==sqlType.PURE_WITH_REVERSE_MULTIPLE ){
                        multicolumn=true;
                 }

                                      
                    
                 if (dbApi.isNowExecuting()){
                      out.print( "EXECUTING..." );
                 }  else if (dbApi.isNowRefreshing()){
                      out.print( "REFRESHING..." );
                 } else if (dbApi.getDiagnosticReturnListValue().isEmpty() && !discovered  && !multicolumn){
                      out.print( "RETURNED NOTHING" );
                 }  else if (dbApi.getDiscoveryMap().isEmpty() && discovered  && !multicolumn){
                      out.print( "RETURNED NOTHING" );
                 }  else if (discovered){   
                       List<List<String>> getData = dbApi.getDiscoveryMap();
                       List<String> metaData = dbApi.getDiscoveryMetaData();
                       
                        for(int kk=0; kk<metaData.size(); kk++)  {
                             out.print(metaData.get(kk)); 
                             out.print(" "); 
                                                              
                       }
                       out.print("</br>");
                      
                       for(int kk=0; kk<getData.size(); kk++)  {

                              List <String> keys_value = getData.get(kk);
                            
                               for (int kl=0; kl<keys_value.size();kl++){
                                    out.print(keys_value.get(kl)); 
                                    out.print(" "); 
                               }
                               out.print("</br>");
                       }
                 } else if ( multicolumn){
                       if (dbApi.getDiscoveryMap()!=null){

                                           if (!dbApi.getDiscoveryMap().isEmpty()){
                                                //out.println(dbApi.getDiscoveryMetaData());
                                                //out.println("<br>");
                                                //out.println(dbApi.getDiscoveryMap());

                                                int stat=-1;
                                                String zbkey = dbApi.getZabbixKey();
                                                String zbkeytmp = "";
                                                if (zbkey.contains("{#STAT}")) stat=1;
                                                
                                                if (stat>-1){


                                                    String [] pcolumns = dbApi.getDatabaseProperty("paramsColumn", "DefaultColumn").toUpperCase().split(",");
                                                    int [] pcolumnsMap = new int [pcolumns.length];

                                                    //prepare
                                                    for (int h=0; h<pcolumns.length;h++){
                                                        pcolumnsMap[h]=-1;
                                                    }

                                                    //prepare
                                                    for (int h=0; h<pcolumns.length;h++){
                                                         for (int l=0; l<dbApi.getDiscoveryMetaData().size();l++){
                                                           if (dbApi.getDiscoveryMetaData().get(l).equalsIgnoreCase(pcolumns[h])){
                                                                 pcolumnsMap[h]=l;
                                                                 logger.debug(">> FOUND STAT -> " + l + " " + dbApi.getDiscoveryMetaData().get(l));
                                                                // break;
                                                           }
                                                         }
                                                    }  
                                                  

                                                    for(int i=0; i<dbApi.getDiscoveryMap().size(); i++) { //i-ty wiersz z kolejki

                                                       zbkey = dbApi.getZabbixKey(); //reset

                                                       if (dbApi.getDiscoveryMap().get(i).size()!=dbApi.getDiscoveryMetaData().size()){
                                                                logger.error("Consist error metaData");
                                                                continue;
                                                       }
                                                       String mtd = "";
                                                       //zmiany w kolumach z parametrem statycznym
                                                       for (int h=0; h<pcolumnsMap.length;h++){
                                                          if (pcolumnsMap[h]>-1) {
                                                              mtd="{#"+dbApi.getDiscoveryMetaData().get(pcolumnsMap[h])+"}";                                                              
                                                              zbkey=zbkey.replace(mtd.toUpperCase(), dbApi.getDiscoveryMap().get(i).get(pcolumnsMap[h])); 
                                                          }
                                                       }


                                                       for (int j=0; j<dbApi.getDiscoveryMap().get(i).size();j++){ //każda kolumna j-ta i-ty wiersz

                                                            if (!checkParmsColumn(dbApi.getDiscoveryMetaData().get(j), pcolumns)){ //kolumna params
                                                                                                                             
                                                                 zbkeytmp=zbkey.replace("{#STAT}", dbApi.getDiscoveryMetaData().get(j));                                                               

                                                               
                                                                    out.println(zbkeytmp);
                                                                    out.println(" ");
                                                               
                                                                
                                                                    out.println(dbApi.getDiscoveryMap().get(i).get(j));
                                                                    out.println("<br>");
                                                              
                                                                //System.out.println(dataObject.toString());
                                                                //logger.debug(dataObject.toString());
                                                            } else {
                                                                logger.debug(">> !STAT " + dbApi.getDiscoveryMetaData().get(j));
                                                            }


                                                       }

                                                    }
                                                } else {
                                                    logger.error("Value STAT not found!!!");
                                                }
                                            } else {
                                               out.println("dbApi.getDiscoveryMap isEmpty() ");
                                            }
                       } else {
                              out.println("dbApi.getDiscoveryMap is NULL");
                       }
                      
                 
                 } else {
                 
                      for (int j=0; j<dbApi.getDiagnosticReturnListValue().size(); j++){
                          out.print(dbApi.getDiagnosticReturnListValue().get(j)); 
                      }
                     
                 }
                 
                 out.println("</td>");
                
                 out.println("<td id=\""+idx+"."+dbApi.getAlias()+"\">");
                 if (dbApi.isActiveMode()){
                    out.println("<font color=\"green\">ACTIVE</font>");  
                 } else {
                    out.println("<font color=\"red\">INACTIVE</font>");  
                 }  
                 out.println("</td>");
                 
                 out.println("<td>");
                 out.print( dbApi.getLastExecuteTime() );             
                 out.println("</td>");  
                 
                 out.println("<td><button onclick=\"Action('db_reverse','"+idx+"."+dbApi.getAlias()+"', '"+idx+"')\">Active/Deactive</button><br/></td>");
                 out.println("<td><button onclick=\"Action('db_refresh','"+idx+"."+dbApi.getAlias()+"', '"+idx+"')\">Refresh</button><br/></td>");
                
                 out.println("</tr>"); 
                 
                 idx++;
                 
        }
        
       
        out.println("</table>");
        printApiFunction(out);
        
        printHomeAndBack(out);
        
     }
       private boolean checkParmsColumn(String col, String [] params){
            boolean ret=false;
            for (String s:params){
                if (s.equalsIgnoreCase(col)) {
                    ret=true;
                    break;
                }
            }
            return ret;
        }
        
    private void printConfigTests(PrintWriter out)  {
        out.print("<h2>TESTS CONFIGURATION</h2>\n");  
        out.println("<style>\n table, th, td {\n border: 1px solid black;\n border-collapse: collapse;\n }\n th, td {\n padding: 5px;\n");
        out.println(" text-align: left;   \n}\n </style>");  
          
        
        out.println("<table style=\"width:90%\">");      
        out.println("<tr>\n <th>ID</th>\n <th>Zabbix server</th>\n  <th>Zabbix host</th>\n  <th>Zabbix key</th>\n <th>Jdbc interface</th>\n <th>Type</th>\n <th>Sql query</th>\n   <th>Cron expression</th>\n   <th>Configure errors</th>\n  <th>Filename</th>\n  </tr>");
        
        int i=1;
      
        for (DBApi dbApi : DBApiList){
                     
                 out.println("<tr>"); 
                 
                 out.println("<td>");
                 out.print( i  );
                 out.println("</td>");
                 
                 out.println("<td>");
                 out.print( dbApi.getZabbixServerName() );
                 out.println("</td>");
             
                 out.println("<td>");
                 out.print( dbApi.getZabbixHost() );
                 out.println("</td>");
                 
                 out.println("<td>");
                 out.print( dbApi.getZabbixKey() );
                 out.println("</td>");
                 
               
                 out.println("<td>");
                 if (dbApi.getJdbc_driverSize()>0)  out.print( dbApi.getJdbc_driver(0).getInterfaceName() );
                 else out.print( "-----------" );
                 out.println("</td>"); 

                 out.println("<td>");
                 out.print( dbApi.getSqlType() );
                 out.println("</td>");
            
                 out.println("<td>");
                 out.print( dbApi.getSqlQuery() );             
                 out.println("</td>"); 
                 
                 out.println("<td>");
                 out.print( dbApi.getCronExpression() );             
                 out.println("</td>");  
                 
                 out.println("<td>");
                 out.print( dbApi.printConfigErrorList() );
                 out.println("</td>");
                            
                 
                 out.println("<td>");
                 out.print( dbApi.getSettingFile() );             
                 out.println("</td>"); 
                                                    
                 out.println("</tr>"); 
                 
                 i++;
                 
        }
        
       
        out.println("</table>");
        
        printHomeAndBack(out);
        
     }
    
    private void printAllThreads(PrintWriter out){
       out.print("<h2>THREADS</h2>\n");  
       ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
       int noThreads = currentGroup.activeCount();
       Thread[] lstThreads = new Thread[noThreads];
       currentGroup.enumerate(lstThreads);
       
        out.println("<style>\n table, th, td {\n border: 1px solid black;\n border-collapse: collapse;\n }\n th, td {\n padding: 5px;\n");
        out.println(" text-align: left;   \n}\n </style>");  
          
        
        out.println("<table style=\"width:90%\">");      
        out.println("<tr>\n <th>ID</th>\n <th>Thread ID</th>\n  <th>Thread name</th>\n  <th>Thread state</th>\n  <th>Thread priority</th>\n </tr>");
       
      
        for (int i = 0; i < noThreads; i++){
                     
                 out.println("<tr>"); 
                 
                 out.println("<td>");
                 out.print(i);
                 out.println("</td>"); 
                 
                 out.println("<td>");
                 out.print(lstThreads[i].getId());
                 out.println("</td>"); 
                 
                 
                 out.println("<td>");
                 out.print(lstThreads[i].getName());
                 out.println("</td>"); 
                 
                 out.println("<td>");
                 out.print(lstThreads[i].getState());
                 out.println("</td>"); 
                 
                 out.println("<td>");
                 out.print(lstThreads[i].getPriority());
                 out.println("</td>"); 
               
                 out.println("</tr>"); 
                 
        }
        
       
        out.println("</table>");
        
        printHomeAndBack(out);
      
      
    }
  
    
    private void printJobs(PrintWriter out){
        out.print("<h2>JOBS</h2>\n");  
        out.println("<style>\n table, th, td {\n border: 1px solid black;\n border-collapse: collapse;\n }\n th, td {\n padding: 5px;\n");
        out.println(" text-align: left;  \n}\n </style>");  
          
        
        out.println("<table style=\"width:90%\">");      
        out.println("<tr>\n <th>ID</th>\n <th>job groupName</th>\n  <th>job key</th>\n  <th>Next fire time</th>\n  <th>State</th>\n </tr>");
       
        
        Scheduler scheduler;
        int i=0;
        try {
             scheduler = new StdSchedulerFactory().getScheduler();
         
             for (String groupName : scheduler.getJobGroupNames()) {
               
                 for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                        out.println("<tr>"); 
                        String jobName = jobKey.getName();
                        String jobGroup = jobKey.getGroup();
                       
                        out.print("<td>");
                        out.print(i);
                        out.println("</td>");
                        
                        i++;
                        
                        out.print("<td>");
                        out.print(jobGroup);
                        out.println("</td>");
                        
                        out.print("<td>");
                        out.print(jobName );
                        out.println("</td>"); 
                    
                        if (scheduler.getTriggersOfJob(jobKey).isEmpty()){
                            out.print("<td>");
                            out.print( "Empty job" );
                            out.println("</td>"); 
                            continue;
                        }

                        String triggerStatus = "default";
                         
                        try {
                            triggerStatus = scheduler.getTriggerState(scheduler.getTriggersOfJob(jobKey).get(0).getKey()).toString();
                            Date nextFireTime = scheduler.getTriggersOfJob(jobKey).get(0).getNextFireTime(); 
                         
                            if (nextFireTime==null){
                                out.print("<td>");
                                out.print("Run once");
                                out.println("</td>"); 
                             } else {
                                  out.print("<td>");
                                  out.print(format.format(nextFireTime));
                                  out.println("</td>"); 
                             }
                             
                            out.print("<td>");
                            out.print(triggerStatus);
                            out.println("</td>"); 
                             
                            
                        } catch (SchedulerException e){
                            out.print("<td>");
                            out.print("Error " + e);
                            out.println("</td>"); 
                            logger.error("Error and continue " + e);
                            continue;
                        }
                        
                        out.println("</tr>"); 
                         
                    }
                    
                 } 
         } catch (SchedulerException ex) {
              out.println(ex);
         } 
         
        out.println("</table>");
        
        printHomeAndBack(out);
     
    }
  
    private String replaceURL(String postData){
          
         try {
              postData = URLDecoder.decode(postData, "UTF-8");
          } catch (UnsupportedEncodingException ex) {
              logger.error("replaceURL decoder");
          }
              
          postData = postData.replaceAll("%C4%84", "A");
          postData = postData.replaceAll("%C4%85", "a");
          postData = postData.replaceAll("%C4%87", "C");
          postData = postData.replaceAll("%C4%88", "c");
          postData = postData.replaceAll("%C4%98", "E");
          postData = postData.replaceAll("%C4%99", "e");
          postData = postData.replaceAll("%C5%81", "L");
          postData = postData.replaceAll("%C5%82", "l");

          postData = postData.replaceAll("%C5%83", "N");
          postData = postData.replaceAll("%C5%84", "n");

          postData = postData.replaceAll("%C3%B3", "o");
          postData = postData.replaceAll("%C3%93", "O");

          postData = postData.replaceAll("%C5%9A", "S");
          postData = postData.replaceAll("%C5%9B", "s");
          postData = postData.replaceAll("%C5%B9", "Z");
          postData = postData.replaceAll("%C5%BA", "z");
          postData = postData.replaceAll("%C5%BB", "Z");
          postData = postData.replaceAll("%C5%BC", "z");
          postData = postData.replaceAll("%20", " ");

         
          postData = postData.replaceAll("%5B", "[");
          postData = postData.replaceAll("%5C", "\\");
          postData = postData.replaceAll("%5D", "]");
          postData = postData.replaceAll("%21", "!");
          postData = postData.replaceAll("%22", "\"");
          postData = postData.replaceAll("%23", "#");
          postData = postData.replaceAll("%24", "$");
          postData = postData.replaceAll("%28", "(");
          postData = postData.replaceAll("%29", ")");
          postData = postData.replaceAll("%40", "@");
          postData = postData.replaceAll("%3F", "?");
          postData = postData.replaceAll("%25", "%");

          return postData;          
                
    }
    
    private boolean isApi( PrintWriter out, boolean isGet ){
        if (!isApi){
             if (isGet) {
                 String data="<h1>401 UNAUTHORIZED</h1>";
                 String response = "HTTP/1.1 401 UNAUTHORIZED\r\n" +
                    "Content-Length: "+data.length()+"\r\n" +
                    "Content-Type: text/html\r\n\r\n" +  data;
                 out.println(response);
                 out.println("<b>Sorry, you are not allowed to access this page.</b><br/>");
             }
             else out.println("Sorry, you are not allowed to access this page.\n");
        } 
        
        return isApi;
    }
    
     private void printRestartApiFunction(PrintWriter out){
          out.println("<font color=\"red\"><p id=\"info\"></p></font>");
      
          out.println(new StringBuilder().append("<script>\n").append("function Restart(type) {\n")
                              .append( "    var txt = \'Action\';\n")
                              .append( "    if (type === 'restart') {\n")
                              .append( "        txt = \"Are you sure you want to daemon?\";\n")
                              .append( "    } else if (type === 'kill') {\n")
                              .append( "        txt = \"Are you sure you want to kill daemon?\";\n")
                              .append( "    } \n")
                              .append( "    var r = confirm(txt);\n")
                              .append( "    if (r == true) {\n")
                              .append( "        var xhttp = new XMLHttpRequest();\n")
                              .append( "        xhttp.onreadystatechange = function() {\n")
                              .append( "           if (this.status == 200) {")
                              .append( "               document.getElementById(\"info\").innerHTML = this.responseText;\n")
                              .append( "           }\n")
                              .append( "       };\n")
                              .append( "       xhttp.open(\"POST\", \"restart.api\", true);")
                              .append( "       xhttp.setRequestHeader(\"Content-type\", \"application/x-www-form-urlencoded\");\n")
                              .append( "       xhttp.send(\"action=\"+type);\n ")
                              .append( "    } else {\n")
                              .append( "    }\n")
                              .append(  " }\n")
                              .append( " </script>").toString());
            
      }
    
    private void printApiFunction(PrintWriter out){
          out.println("<font color=\"red\"><p id=\"info\"></p></font>");
      
          out.println(new StringBuilder().append("<script>\n").append("function Action(type, alias, id) {\n")
                              .append( "    var txt = \'Action\';\n")
                              .append( "    if (type === 'host_reverse' ) {\n")    
                              .append( "        txt = \"Are you sure you want active/deactive host \" + alias + \"?\"; \n")
                              .append( "    } ") 
                              .append( "    else if ( type === 'jdbc_reverse') {\n")    
                              .append( "        txt = \"Are you sure you want active/deactive jdbc \" + alias + \"?\"; \n")
                              .append( "    } ")
                              .append( "    else if ( type === 'db_reverse') {\n")    
                              .append( "        txt = \"Are you sure you want active/deactive test \" + alias + \"?\"; \n")
                              .append( "    } ") 
                              .append( "    else if ( type === 'db_refresh') {\n")    
                              .append( "        txt = \"Are you sure you want refresh test \" + alias + \"?\"; \n")
                              .append( "    } ") 
                              .append( "    var r = confirm(txt);\n")
                              .append( "    if (r == true) {\n")
                              .append( "        var xhttp = new XMLHttpRequest();\n")
                              .append( "        xhttp.onreadystatechange = function() {\n")
                              .append( "           if ( type === 'db_reverse' || type === 'db_refresh' || type === 'host_reverse' ||  type === 'jdbc_reverse') {\n")
                              .append( "              if (this.status == 200) {\n")
                              .append( "                  document.getElementById(alias).innerHTML = this.responseText;\n")
                              .append( "              }\n") 
                              .append( "           }\n")
                              .append( "          else {")
                              .append( "              if (this.status == 200) {\n")
                              .append( "                  document.getElementById(id+\".\"+alias).innerHTML = this.responseText;\n")
                              .append( "              }\n") 
                              .append( "           }\n")
                              .append( "       };\n")
                              .append( "       xhttp.open(\"POST\", \"actions.api\", true);\n")
                              .append( "       xhttp.setRequestHeader(\"Content-type\", \"application/x-www-form-urlencoded\");\n")
                              .append( "       xhttp.send(\"action=\"+type+\"&alias=\"+alias+\"&id=\"+id);\n ")
                              .append( "    } else {\n")
                              .append( "    }\n")
                              .append(  " }\n")
                              .append( " </script>").toString());
            
      } 
    
    private void printRestartAndKill(PrintWriter out){         
          
          out.println("");
          out.println("<br/><button onclick=\"Restart('restart')\">Restart DAEMON</button><br/>");
          out.println("");
          out.println("<button onclick=\"Restart('kill')\">Kill DAEMON</button><br/>");
          
          printRestartApiFunction(out);
     }
  
      private void printHomeAndBack(PrintWriter out){
          
          out.println("<br/><button onclick=\"goHome()\">HOME</button>\n");
          out.println("  "); 
          out.println("<script>\n");
          out.println("function goHome() {\n");
          out.println("   window.location = '/';\n");
          out.println("}\n" );
          out.println( "</script>");
          
          out.println("<button onclick=\"goBack()\">Go Back</button><br/>\n");
          out.println("\n");
          out.println("<script>\n");
          out.println("function goBack() {\n");
          out.println("    window.history.back();\n");
          out.println("}\n" );
          out.println( "</script>");
     }
      
      private void printPID(PrintWriter out) {
        String processName = ManagementFactory.getRuntimeMXBean().getName();

        out.println("<b> CURRENT PID ");
        out.println(Long.parseLong(processName.split("@")[0]));
        out.println(" </b><br/>");
        printHomeAndBack(out);
      }
    

      private void printVersion(PrintWriter out)  {
        out.println("<b><h3> VERSION " +Version.getName()+ " </h3>" );
        out.println(" </b>");
        out.println(Version.getVersion());
        out.println("<br/>");
        out.println(Version.getAuthor());
        out.println("<br/>");
        out.println("DEV_VERSION_EXPIRE_STR: ");
        out.println(Version.DEV_VERSION_EXPIRE_STR);
        out.println("<br/>");
        out.println(UPDATE_STR.toString());    
        printHomeAndBack(out);
      }
      
      private void printJVM(PrintWriter out)  {
        Runtime runtime = Runtime.getRuntime(); 
        
        
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMem = allocatedMemory - freeMemory;
        long totalMem = runtime.totalMemory();
        long mb=1024;
        
        out.println("<b><h3>Heap utilization statistics [MB] </h3>" );
        out.println(" </b>");
        out.println("Total Memory: " + totalMem / mb);
        out.println("<br/>");
        out.println("Free Memory:  " + freeMemory / mb);
        out.println("<br/>");
        out.println("Used Memory:  " + usedMem / mb);
        out.println("<br/>");
        out.println("Max Memory:   " + maxMemory / mb);
        out.println("<br/>");
       
        out.println("<b><h3>SystemMXBean statistics</h3></b>" );
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
         for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (method.getName().startsWith("get") 
                && Modifier.isPublic(method.getModifiers())) {
                    Object value;
                try {
                    value = method.invoke(operatingSystemMXBean);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    value = e;
                } // try
                out.println(method.getName() + " = " + value);
                out.println("<br/>");
            } // if
          } // for
        
        printHomeAndBack(out);
      }
    

      
}