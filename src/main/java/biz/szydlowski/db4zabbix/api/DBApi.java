/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.api;

import biz.szydlowski.db4zabbix.sqlType;
import com.alibaba.fastjson.JSON;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Dominik
 */
public class DBApi {
    
    public static int ZBX_STATE_NORMAL=0;
    public static int ZBX_STATE_NOTSUPPORTED=1;
    
    private String complementary_query;
    private boolean isComplementary;
    private boolean isComplementaryMapFromFile;
    private long lastTimeComplementary;
    private long complementaryUpdateInterval;
    private List<List<String>> complementaryMap;
        
    private boolean activeMode; 
    private String zabbix_server_name;
    private String alias;
    private String zabbix_key;
    private String zabbix_host;
    private sqlType sql_type;
    private int isolation_level;
    private String sql_query; 
    private String discovery_query;
    private boolean fullquery; 
    private boolean is_html; 
    private boolean show_count; 
    private boolean is_integer; 
    private boolean is_string;
    private boolean is_float; 
    private boolean is_long;  
    private boolean timing;
    private boolean isReadyToSend; 
    private boolean fireAtStartup; 
    private boolean isNowExecuting; 
    private boolean isNowRefreshing;
    private boolean isDiscoveryMetadataFromFile;
    private boolean isDiscoveryMapFromFile;
    private boolean isDiscoveryParamsTypeFromFile;
    private boolean no_data;
    private int column;
    private int row;
    private int more_stmt;
    private String cron_expression;
    private final Properties prop;
    private long lastTimeDiscovery;
    private long discoveryUpdate;  
    private String lastExecuteTime;
    private String settingFile;  
    private boolean sendTozbx;    
    private final List<String> discoveryParamsType;
    private List<String> discoveryMetaData;
    private List<List<String>> discoveryMap;
    private List<List<String>> latestdiscoveryMap;
    private final List <String> returnValue ;
    private final List <String> configErrorList;
    private final List <Integer> state;
    private final SimpleDateFormat sdf;
    private List <JdbcDriverApi>  jdbc_driver; 
    private String latestValue;
    private boolean hasLatestValue;
    private long latestValueTimestamp;  
    private boolean onlyFirstDatabase;
    private String pg_database;
    private boolean prepareQuery;
    private String generatePrepareQuery;
    
    public DBApi(){
        complementaryMap = new ArrayList<>();
        isComplementary=false;
        onlyFirstDatabase=false;
        pg_database="default";
        generatePrepareQuery="default";
        prepareQuery = false;
          
        discoveryParamsType = new ArrayList<>();
        discoveryMetaData = new ArrayList<>();
        discoveryMap = new ArrayList<>();
        latestdiscoveryMap = new ArrayList<>();
        returnValue = new ArrayList<>();
        configErrorList = new ArrayList<>();
        jdbc_driver = new ArrayList<>();
        state = new ArrayList<>();
        sdf = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss");
        prop = new Properties();
      
        this.hasLatestValue=false;
        this.latestValue="default";
        this.activeMode=true;
        this.zabbix_server_name="name";
        this.alias="alias";
        this.zabbix_key="zkey";
        this.zabbix_host="zhost";
        this.sql_type=sqlType.PURE;
        this.sql_query="SELECT -5";
        this.is_long=false; 
        this.is_integer=true; 
        this.is_float=false;
        this.is_string=false;
        this.timing=false;
        this.isReadyToSend=false;
        this.isNowExecuting=false;
        this.isNowRefreshing=false;
        this.fullquery=false;
        this.cron_expression = "0 0 0 * * ?";
        this.isolation_level=-1;
        this.no_data=false;
        this.lastExecuteTime = "NONE";
      //  this.db_interface="not required"; 
        this.is_html=false; 
        this.show_count=true;
        this.fireAtStartup=false;
       // interface_type="single";
      
        lastTimeDiscovery = 0L;
        discoveryUpdate=1000L;  
    
        this.column = 1;
        this.row=1;
        this.more_stmt=1;

        
        this.discovery_query = "select 1";
        
          
        this.discovery_query = "select 1";
        this.isDiscoveryMetadataFromFile=false;
        this.isDiscoveryMapFromFile = false;
        this.isDiscoveryParamsTypeFromFile=false; 
        
         settingFile = "default";
        
        sendTozbx=true;
        
        this.complementary_query = "select 1";
        this.isComplementaryMapFromFile=false;
        lastTimeComplementary = 0L;
        complementaryUpdateInterval=30000L;
    } 
    
    
     public boolean isSendToZbx(){
       return  sendTozbx;
    }     
      
    
    public void setSentToZbx(String set){
         sendTozbx=Boolean.parseBoolean(set);    
    }
    
    public void setSettingFile(String settingFile){
        this.settingFile=settingFile;
    }
    
    public void setToActiveMode(){
        activeMode=true;
    }
    
    public void setToInactiveMode(){
        activeMode=false;
    } 
    
    public boolean isActiveMode(){
       return activeMode;
    } 
   
    public boolean isFireAtStartup(){
       return  fireAtStartup;
    }     
    
    public void setFireAtStartup(String set){
         fireAtStartup=Boolean.parseBoolean(set);    
    }     
      
    /*public void setDbPoolId(int set){
       db_pool_id=set;
    } 
    
    public int getDbPoolId(){
       return db_pool_id;
    } */
    
    
    public void setZabbixServerName(String set){
        zabbix_server_name=set;
    } 
    
    public void setAlias(String set){
        alias=set;
    } 
    
    public void setZabbixKey(String set){
        zabbix_key=set;
    } 
    
    public void setZabbixHost(String set){
        zabbix_host=set;
    } 
    
    public void setSqlType(sqlType set){
         sql_type=set;
    } 
    
    public void setSqlTypeFromString(String set){
       sql_type = sqlType.valueOf(set);
    }
    
    public void setIsolationLevel(String set){
        try {
            isolation_level=Integer.parseInt(set);
        } catch (Exception ignore){
        }
    }
    
    public void setSqlQuery(String set){
        sql_query=set;
    }  
    
    public void setDiscoveryQuery(String set){
        discovery_query=set;
    } 
    
    public void setIsString(String set){
         is_string=Boolean.parseBoolean(set);
         if (is_string){
              is_integer=false;
              is_long=false;
              is_float=false;
         }      
    }  
    //deprecated
    public void setIsNumeric(String set){
         is_integer=Boolean.parseBoolean(set);
         if (is_integer) is_string=false;
    }  
    
    public void setIsInteger(String set){
         is_integer=Boolean.parseBoolean(set);
         if (is_integer){
             is_string=false;
             is_long=false;
             is_float=false;
         }
         
    }    
    
 
    
     public void setIsLong(String set){
         is_long=Boolean.parseBoolean(set);
         if (is_long){
             is_string=false;
             is_integer=false;
             is_float=false;
         }
        
    }   
    
    
    public void setIsFloat(String set){
         is_float=Boolean.parseBoolean(set);  
         if (is_float){
             is_string=false;
             is_integer=false;
             is_long=false;
         }
    }   
    
    public void setIsTiming(String set){
        timing=Boolean.parseBoolean(set);
    } 
    
    public void setNoData(String set){
        no_data=Boolean.parseBoolean(set);
    } 
    
    public void setFullQuery(String set){
        fullquery=Boolean.parseBoolean(set);
        if (fullquery){
             is_string=false;
             is_integer=false;
             is_long=false;
             is_float=false;
        }
    }  
        
    public void setColumn(String set){
        try {
            column=Integer.parseInt(set);
        } catch (Exception ignore){
           column=1;
        }
    }
   
    public void setRow(String set){
        try {
            row=Integer.parseInt(set);
        } catch (Exception ignore){
            row=1;
        }
    }
   
    public void setMoreStmt(String set){
        try {
            more_stmt=Integer.parseInt(set);
        } catch (Exception ignore){
            more_stmt=1;
        }
    }  
    
    public void setCronExpression(String set){
        if (org.quartz.CronExpression.isValidExpression(set)){
           cron_expression=set;
        } else {
            cron_expression = "0 0 0 * * ?"; 
        }
    } 

  
   /* public void setDbDefaultDatabase(String set){
       db_default_database=set;
    }
   
    public void setDbUrl(String set){
      db_url=set;
    } 
    
    public void setJdbcClassName(String set){
        jdbc_className=set;
    }
   
    public void setDbHost(String set){
       db_host=set;
    }
    public void setDbPort(String set){
       db_port=set;
    }
    
    public void setDbUser(String set){
       db_user=set;
    } 
    
    public void setDbPassword(String set){
       db_password=set;
    }   
    
    public void setConnRetry(int set){
       db_conn_retry=set;
    } 
    
    public void setLoginTimeout(int set){
       db_conn_timeout=set;
    }
    */
    
    public void setJdbcDriver(JdbcDriverApi jdbc_driver){
         /* setDbHost(jdbc_driver.getHost());
          setDbPort(jdbc_driver.getStringPort());
          setDbUser(jdbc_driver.getUser());
          setDbPassword(jdbc_driver.getPassword());
          setDbUrl(jdbc_driver.getUrl()); 
          setDBInterface(jdbc_driver.getInterfaceName());
          setJdbcClassName(jdbc_driver.getClassName());
          setDbDefaultDatabase(jdbc_driver.getDefaultDatabase());
          setConnRetry(jdbc_driver.getConnectionRetry());
          setLoginTimeout(jdbc_driver.getLoginTimeout());
          setInterfaceType(jdbc_driver.getInterfaceType());
          setDbPoolId(jdbc_driver.getPoolIndex());*/
          this.jdbc_driver.add(jdbc_driver);
    }
   
   /* public void setInterfaceType(String set){
       interface_type=set;
    } 

    public String getInterfaceType(){
       return interface_type;
    }*/ 
    
    public void setDatabaseProperty(String key, String value){
        prop.put(key, value);
    }
    
    public void setReadyToSendToTrue(){
        isReadyToSend=true;
    } 
    
    public void setReadyToSendToFalse(){
        isReadyToSend=false;
    }
   
    public void setIsNowExecuting (){
        isNowExecuting = true;
        lastExecuteTime = sdf.format(new Date());
    }  
    
    public String getLastExecuteTime(){
        return lastExecuteTime;
    }
    
    public void setIsNowRefreshing (){
        isNowRefreshing= true;
    } 
 
 
    public void setIsNowExecutingToFalse (){
        isNowExecuting = false;
    } 
    
    public void setIsNowRefreshingToFalse (){
        isNowRefreshing = false;
    }   
    
   /* public void setDBInterface(String set){
       db_interface=set;
    }*/    
    
    public void setHtml(String set){
        is_html=Boolean.parseBoolean(set);
    } 
    
    public void setShowCount(String set){
        show_count=Boolean.parseBoolean(set);
    }
    
 
    public void clearReturnAndStateValue(){
        returnValue.clear();
        state.clear();
    }
     
    public void addNormalReturnValue(String value){
        addReturnValue(value);
        state.add(ZBX_STATE_NORMAL);
    }
    public void addErrorReturnValue(String value){
        addReturnValue(value);
        state.add(ZBX_STATE_NOTSUPPORTED);
    }
    private void addReturnValue(String value){
        returnValue.add(value);
    }
    
    public void setDiscoveryMapAndMetaData(List<List<String>> _discoveryMap){
        discoveryMap.clear();
        setTimeDiscovery();
        if (!_discoveryMap.isEmpty()){
            if (isDiscoveryMetadataFromFile) {
                 _discoveryMap.remove(0);
            } else {
                //dodaj meata data
                discoveryMetaData.clear();
                for (String f : _discoveryMap.get(0)){
                   discoveryMetaData.add(f);
                } 
                _discoveryMap.remove(0);
            }
        }
        for (List<String> s : _discoveryMap){
           discoveryMap.add(s);
        }
        
    }  
    
    public void setDiscoveryMap(List<List<String>> _discoveryMap){
        discoveryMap.clear();
        setTimeDiscovery();
        if (!_discoveryMap.isEmpty()){
            for (List<String> s : _discoveryMap){
               discoveryMap.add(s);
           }
        }
       
        
    }
    
    public void setDiscoveryMapAndMetaData(String spl){
      isDiscoveryMapFromFile = true; 
      discoveryMap.clear();
      setTimeDiscovery();
      String [] lines = spl.split(";;");
      setDiscoveryMetaData(lines[0]);
      for (int i=1; i<lines.length;i++){
            List<String> discoveryline = new ArrayList<>();
            String [] tmp = lines[i].split(";");
            discoveryline.addAll(Arrays.asList(tmp));
            discoveryMap.add(discoveryline);
       }
    }      
   
    public void setDiscoveryMap(String spl){
      isDiscoveryMapFromFile = true;
      discoveryMap.clear();
       setTimeDiscovery();
       String [] lines = spl.split(";;");
       for (String line : lines){
            List<String> discoveryline = new ArrayList<>();
            String [] tmp = line.split(";");
            discoveryline.addAll(Arrays.asList(tmp));
            discoveryMap.add(discoveryline);
        }
    } 
    
    public void setDiscoveryMetaData(String spl){
        isDiscoveryMetadataFromFile = true;
        discoveryMetaData.clear();
        String [] adder = spl.split(";");
        discoveryMetaData.addAll(Arrays.asList(adder));
    }  
    
    public void setDiscoveryMetaData(List<String> meta){
        isDiscoveryMetadataFromFile=false;
        discoveryMetaData.clear();
        discoveryMetaData = meta;
    } 
    

    public void setDiscoveryParamsType(String spl){
        isDiscoveryParamsTypeFromFile=true;
        discoveryParamsType.clear();
        String [] adder = spl.split(";");
        discoveryParamsType.addAll(Arrays.asList(adder));
    } 
    
    public String getSettingFile(){
        return this.settingFile;
    }
    
    public Properties getDatabaseProperties(){
        return prop;
    }  
    
    public String getDatabaseProperty(String key, String default_value){
        return prop.getProperty(key, default_value);
    }
   
    public String getAlias(){
        return alias;
    } 
    
    public sqlType getSqlType(){
         return sql_type;
    }  
    
    public String getSqlQuery(){
        return sql_query;
    }  
    
     public String getDiscoveryQuery(){
        return discovery_query;
    }  
     
     public List<List<String>> getDiscoveryMap(){
        return discoveryMap;
    }   
     
    public List<String> getDiscoveryMetaData(){
        return discoveryMetaData;
    }     
    
    public List<String> getDiscoveryParamsType(){
        return discoveryParamsType;
    } 
     
   
    public int getIsolationLevel(){
        return isolation_level;     
    }
       
   
    public String getZabbixKey(){
        return zabbix_key;
    }   
    
    public String getZabbixHost(){
        return zabbix_host;
    } 
   
    public String getZabbixServerName(){
        return zabbix_server_name;
    }   
    
    public String getCronExpression(){
        return cron_expression;
    }   
 
    
    public boolean isReadyToSend(){
        return isReadyToSend;
    } 
           
    public boolean isNowExecuting(){
        return isNowExecuting;
    }
   
    public boolean isNowRefreshing(){
        return isNowRefreshing;
    }
    
    public boolean isFullQuery(){
        return fullquery;
    }  
    
    public boolean isString(){
        return is_string;
    }  
    
    public boolean isInteger(){
        return is_integer;
    }  
    
    public boolean isLong(){
        return is_long;
    }  
    public boolean isFloat(){
        return is_float;
    }  
    
    public boolean isTiming(){
        return timing;
    } 
    
    public boolean isNoData(){
        return no_data;
    }        
         
    public int getColumn(){
        return column;
    }
   
    public int getRow(){
        return row;
      
    }
   
    public int getMoreStmt(){
       return more_stmt;
    }    
    
    public List <String>  getReturnListValue(){
        if (returnValue.isEmpty()){
            state.clear();
            addErrorReturnValue("ERROR in getReturnListValue");
        }
        return returnValue;
    }     
    
    public List <String>  getDiagnosticReturnListValue(){
        return returnValue;
    }   
      
    
    public List <Integer>  getStateList(){
        if (state.isEmpty()){
            returnValue.clear();
            addErrorReturnValue("ERROR in getReturnListState");
        }
        return state;
    }
    
    public long getDiscoveryUpdateInterval(){
        return discoveryUpdate;
    }  
    
    public void setDiscoveryUpdateInterval(long v){
        discoveryUpdate=v;
    } 
    
    private void setTimeDiscovery(){
        lastTimeDiscovery = System.currentTimeMillis() ;
    }             
   
    public boolean isDiscoveryParamsTypeFromFile (){
       return isDiscoveryParamsTypeFromFile;
    }
  
    public boolean isDiscoveryMapFromFile(){
        return isDiscoveryMapFromFile;
    } 
    
    public boolean mustDiscoveryNow (){
        return (System.currentTimeMillis() - lastTimeDiscovery) > discoveryUpdate;
    }
  
    public boolean isHtml(){
        return is_html;
    }  
    public boolean isShowCount(){
        return show_count;
    }
    
    public void addErrorToConfigErrorList(String error){
       configErrorList.add(error);
    }  
  
   
    public String printConfigErrorList(){
        StringBuilder sb = new StringBuilder();
         sb.append("<font color=\"red\">");
        configErrorList.forEach((e) -> {
            sb.append(e).append("<br>");
        }); 
        sb.append("</font>");
        return sb.toString();
    }
    
    
     @Override
      public String toString() {
		return JSON.toJSONString(this);
     }

    /**
     * @return the jdbc_driver
     */
    public JdbcDriverApi getJdbc_driver(int indx) {
       if (jdbc_driver.size()<=indx){
          System.out.println("Error x 2555 jdbcDriver");
          System.exit(90);
          return null;
        } else return jdbc_driver.get(indx);
    }
    
     public int getJdbc_driverSize() {
        return jdbc_driver.size();
    }
    
     public void setComplementaryMapAndMetaData(String spl){
      setTimeComplementary(); 
      isComplementaryMapFromFile = true;
      isComplementary=true;
      complementaryMap.clear();
      String [] lines = spl.split(";;");
      for (int i=0; i<lines.length;i++){
            List<String> compline = new ArrayList<>();
            String [] tmp = lines[i].split(";");
            compline.addAll(Arrays.asList(tmp));
            complementaryMap.add(compline);
       }
    } 
     
    private void setTimeComplementary(){
        this.lastTimeComplementary = System.currentTimeMillis() ;
    }
  
    public boolean mustComplementaryNow (){
        if (isComplementaryMapFromFile) {
            return false;
        } else {
             long timer = (System.currentTimeMillis() - lastTimeComplementary)/1000;
            return timer > complementaryUpdateInterval;
        }
    }
    
      /**
     * @return the complementMap
     */
    public List<List<String>> getComplementaryMap() {
        return complementaryMap;
    }

    /**
     * @param complementMap the complementMap to set
     */
    public void setComplementaryMap(List<List<String>> complementMap) {
        setTimeComplementary();
        isComplementaryMapFromFile=false;
        this.complementaryMap = complementMap;
    }
    
     /**
     * @return the complement_query
     */
    public String getComplementary_query() {
        return complementary_query;
    }

    /**
     * @param complement_query the complement_query to set
     */
    public void setComplementary_query(String complement_query) {
        this.complementary_query = complement_query;
         setIsComplementary(true);
    }

    /**
     * @return the isComplement
     */
    public boolean isComplementary() {
        return isComplementary;
    }
    
    

    /**
     * @param isComplement the isComplement to set
     */
    public void setIsComplementary(boolean isComplement) {
        this.isComplementary = isComplement;
    }

    /**
     * @return the isComplementaryMapFromFile
     */
    public boolean isComplementaryMapFromFile() {
        return isComplementaryMapFromFile;
    }

    /**
     * @return the lastTimeComplementary
     */
    public long getLastTimeComplementary() {
        return lastTimeComplementary;
    }


    /**
     * @return the complementaryUpdateInterval
     */
    public long getComplementaryUpdateInterval() {
        return complementaryUpdateInterval;
    }

    /**
     * @param complementaryUpdateInterval the complementaryUpdateInterval to set
     */
    public void setComplementaryUpdateInterval(long complementaryUpdateInterval) {
        this.complementaryUpdateInterval = complementaryUpdateInterval;
    }

    /**
     * @return the discoveryMapLast
     */
    public List<List<String>> getLatestDiscoveryMap() {
        return latestdiscoveryMap;
    }

    /**
     * @param latestdiscoveryMap
     * @param latestValueTimestamp
     */
    public void setLatestDiscoveryMap(List<List<String>> latestdiscoveryMap, long latestValueTimestamp) {
        if (latestdiscoveryMap!=null){
            this.latestdiscoveryMap.clear();
            this.latestdiscoveryMap=null;
            this.latestdiscoveryMap  = new ArrayList<>();
            latestdiscoveryMap.forEach(b -> {
                this.latestdiscoveryMap.add(new ArrayList<>(b));
            });
            this.latestValueTimestamp=latestValueTimestamp;
            this.hasLatestValue=true;
        }
        
    }

    /**
     * @return the latestValue
     */
    public String getLatestValue() {
        return latestValue;
    }

    /**
     * @param latestValue the latestValue to set
     * @param latestValueTimestamp
     */
    public void setLatestValue(String latestValue, long latestValueTimestamp) {
        this.latestValue = latestValue;
        this.latestValueTimestamp=latestValueTimestamp;
        this.hasLatestValue=true;
    }

    /**
     * @return the hasLatestValue
     */
    public boolean hasLatestValue() {
        return hasLatestValue;
    }

    /**
     * @return the latestValueTimestamp
     */
    public long getLatestValueTimestamp() {
        return latestValueTimestamp;
    }

    /**
     * @return the onlyFirstDatabase
     */
    public boolean isOnlyFirstDatabase() {
        return onlyFirstDatabase;
    }

    /**
     * @param onlyFirstDatabase the onlyFirstDatabase to set
     */
    public void setOnlyFirstDatabase(boolean onlyFirstDatabase) {
        this.onlyFirstDatabase = onlyFirstDatabase;
    }

    /**
     * @return the pg_database
     */
    public String getPg_database() {
        return pg_database;
    }

    /**
     * @param pg_database the pg_database to set
     */
    public void setPg_database(String pg_database) {
        this.pg_database = pg_database;
    }

    /**
     * @return the prepareQuery
     */
    public boolean isPrepareQuery() {
        return prepareQuery;
    }

    /**
     * @param prepareQuery the prepareQuery to set
     */
    public void setPrepareQuery(boolean prepareQuery) {
        this.prepareQuery = prepareQuery;
    }

    /**
     * @return the generatePrepareQuery
     */
    public String getGeneratePrepareQuery() {
        return generatePrepareQuery;
    }

    /**
     * @param generatePrepareQuery the generatePrepareQuery to set
     */
    public void setGeneratedPrepareQuery(String generatePrepareQuery) {
        this.generatePrepareQuery = generatePrepareQuery;
    }
    
    public boolean isGeneratedPrepareQuery() {
        return !this.generatePrepareQuery.equals("default");
    }


 
    
}
