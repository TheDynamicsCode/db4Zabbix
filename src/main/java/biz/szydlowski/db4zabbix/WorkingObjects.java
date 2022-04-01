/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix;

import biz.szydlowski.db4zabbix.api.DBApi;
import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Dominik
 */
public class WorkingObjects {
    public static DBQuartz _DBQuartz=null;
    public static List<DBApi> DBApiList = null;
    public static List<JdbcDriverApi> JdbcDriverApiList = null;
    public static List <ZabbixServerApi> zabbixServerApiList = null;
    public static List<String> allowedConn = null;
    public static Map<String, String> CrontabGroupsList = null;   
    public static HashMap<String, Boolean> unique_hostname  = null;
}
