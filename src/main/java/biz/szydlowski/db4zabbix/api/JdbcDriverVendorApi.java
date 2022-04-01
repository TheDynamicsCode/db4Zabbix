/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.api;

/**
 *
 * @author szydlowskidom
 */
public class JdbcDriverVendorApi {
    
    private String driver_type="ifn";
    private String url_template="url";
    private String _class="class";
     
    public void setDriverType(String set){
        driver_type=set;
    }
    
    public void setUrlTemplate(String set){
        url_template=set;
    }
    
    public void setClassName(String set){
        _class=set;
    }
    
   
    public String getDriverType(){
        return driver_type;
    }
   
    public String getUrlTemplate(){
        return url_template;
    }
    
    
    public String getClassName(){
        return _class;
    }
        
    
}
