/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.api;

import java.util.Properties;

/**
 *
 * @author szydlowskidom
 */
public class QueryApi {
    private String name="ifn";
    private String query_string="query";
    private Properties prop = new Properties();
    
    public void setName(String set){
        name = set;
    }
    
    public void setQuery(String set){
        query_string = set;
    } 
    
    public void setQueryProperty(String key, String value){
        prop.put(key, value);
    }
    
    public String getName(){
        return name;
    }
    
    public String getQuery(){
        return query_string;
    }
    
    public Properties getQueryProperties(){
        return prop;
    }  
    
    public String getQueryProperty(String key, String default_value){
        return prop.getProperty(key, default_value);
    }
    
}
