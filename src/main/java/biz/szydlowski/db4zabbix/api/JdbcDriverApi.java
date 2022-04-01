/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.api;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author szydlowskidom
 */
public class JdbcDriverApi {

    private String interface_name="interface";
    private String host="host";
    private int port=9000;
    private String s_port="9000";
    private String user="user";
    private String password="pswd";
    private String vendor="vendor";
    private String url_template="url_template";
    private String url="url"; 
    private String default_database="database"; 
    private String _class="class";
    private boolean activeMode=true; 
    private String interface_type="single";
     
    private int conn_retry=1;
    private int pool_index=0;
    
    private int minPoolSize=5;
    private int maxPoolSize=10;
    private int maxStatements=180; 
    private int acquireIncrement=2;
    private int loginTimeout = 10;
    private String extendedOption="default";
    private List<String> database = new ArrayList<>();
   
    public void setToActiveMode(){
        activeMode=true;
    }
    
    public void setToInactiveMode(){
        activeMode=false;
    } 
    
    public boolean isActiveMode(){
       return activeMode;
    } 
    
    public void setInterfaceName(String set){
        interface_name=set;
    } 
    
    public void setDriverVendor(String type){
        this.vendor=type;
    }
    
    public void setUrlTemplate(String url_template){
        this.url_template=url_template;
    }  
      
    public void setDefaultDatabase(String set){
        default_database=set;
    }  
    
    public void setClassName(String set){
        _class=set;
    }
  
    public void setInterfaceType(String set){
        interface_type=set;
    }

    public void setPoolIndex(int _pool_index){
        pool_index=_pool_index;
    }
    
    public void setHost(String set){
        host=set;
    }
    
    public void setPort(String set){
        try {
            port=Integer.parseInt(set);
        } catch (Exception ignore){}
        s_port=set;
    }
    
    public void setUser(String set){
        user=set;
    }
     
    public void setPassword(String set){
        password=set;
    }   
    
     public void setMinPoolSize(String set){
        try {
            minPoolSize=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
    
     public void setMaxPoolSize(String set){
        try {
            maxPoolSize=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
     
    public void setMaxStatements(String set){
        try {
            maxStatements=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
   
    public void setLoginTimeout(String set){
        try {
            loginTimeout=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
        if (loginTimeout<0) loginTimeout=1;
    }
     
     public void setAcquireIncrement(String set){
        try {
            acquireIncrement=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
    
 
    public void setConnectionRetry(String set){
        try {
            conn_retry=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
        if (conn_retry<0) conn_retry=0;
    } 
    
    
    public void generateUrl(){
      url=url_template;
      url=url.replace("{port}", s_port);
      url=url.replace("{host}", host);
      url=url.replace("{database}", default_database);
      url=url.replace("{user}", user);
      url=url.replace("{password}", password);
    }
    
    public void generateUrl(String db){
      url=url_template;
      url=url.replace("{port}", s_port);
      url=url.replace("{host}", host);
      url=url.replace("{database}", db);
      url=url.replace("{user}", user);
      url=url.replace("{password}", password);
    }
   
    public String getInterfaceName(){
        return interface_name;
    }  
    
    public String getDriverVendor(){
        return vendor;
    }
    
    public String getUrlTemplate(){
        return url_template;
    }  
    
    public String getUrl(){
        return url;
    }   
    
    public String getDefaultDatabase(){
        return default_database;
    }
    
    public String getClassName(){
        return _class;
    }
   
    public String getHost(){
        return host;
    }
    
    public int getPort(){
        return port;
    }
    
    public String getStringPort(){
        return s_port;
    }
    
    public String getUser(){
        return user;
    }
     
    public String getPassword(){
        return password;
    }  
    
    public int getConnectionRetry(){
        return conn_retry;
    }
    
    public int getLoginTimeout(){
        return loginTimeout;
    }   
    
    public String getInterfaceType(){
        return interface_type;
    }
    
    public int getMinPoolSize(){
        return minPoolSize;
    }
    
    public int getMaxPoolSize(){
        return maxPoolSize;
    }
    
    public int getMaxStatements(){
        return maxStatements;
    }  
    
    public int getAcquireIncrement(){
        return acquireIncrement;
    }  
    
    public int getPoolIndex(){
        return pool_index;
    }

    /**
     * @return the extendedOption
     */
    public String getExtendedOption() {
        return extendedOption;
    }

    /**
     * @param extendedOption the extendedOption to set
     */
    public void setExtendedOption(String extendedOption) {
        this.extendedOption = extendedOption;
    }

    /**
     * @return the database
     */
    public List<String> getDatabase() {
        return database;
    }

    /**
     * @param database the database to set
     */
    public void setDatabase(List<String> database) {
        this.database = database;
    }
    
    public void addDatabase(String db) {
        this.database.add(db);
    }
    
    public void cleanDatabase() {
        this.database.clear();
    }
    
}
