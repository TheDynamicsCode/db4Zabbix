/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.config;

import static biz.szydlowski.db4zabbix.WorkingObjects.CrontabGroupsList;
import static biz.szydlowski.db4zabbix.WorkingObjects.DBApiList;
import static biz.szydlowski.db4zabbix.WorkingObjects.JdbcDriverApiList;
import static biz.szydlowski.db4zabbix.WorkingObjects.zabbixServerApiList;
import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import biz.szydlowski.db4zabbix.api.QueryApi;
import biz.szydlowski.db4zabbix.api.DBApi;
import biz.szydlowski.db4zabbix.sqlType;
import biz.szydlowski.utils.OSValidator;
import biz.szydlowski.utils.WorkingStats;
import biz.szydlowski.utils.template.TemplateFile;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author dominik
 */
public class DBParams {
   

   private List<QueryApi> queryList =  new ArrayList<>(); 
   
   static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.config.DBParams");
    
       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
     public DBParams(){
           
     
        queryList =  new SqlQuery().getQueryApiList();
       
        Properties filenames = new DBParamsFiles().getProperties();
        Set<Object> keys = getAllKeys(filenames);
        for(Object k:keys){
            String key = (String)k; 
            String set_filename = getPropertyValue(filenames, key);
            if (OSValidator.isUnix()){
              String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
               set_filename = absolutePath + "/setting/" + set_filename;
            } else {
                set_filename =  "setting/" + set_filename;
            }
          
            addPropsFromFile(set_filename);
            //logger.debug("props size = "  + props.size());
        }
        
        TemplateFile _DBTemplate = new TemplateFile("default");
        for (String file : _DBTemplate.getFilenames()){
            addPropsFromFile(file);
        }
        
        logger.info("Total DBApi size = "  + DBApiList.size());
     
    }
     
    
    private void addPropsFromFile(String filename){
        try {
                        
		File fXmlFile = new File(filename);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
               
                String prefix1 = doc.getElementsByTagName("params").item(0).getAttributes().getNamedItem("prefix").getNodeValue();
             
		logger.info("Read db-to-zabbix " + filename);
                           
		NodeList  nList = doc.getElementsByTagName("DbToZabbix");
                Node nNode;
                		 
		for (int temp = 0; temp < nList.getLength(); temp++) {
                
		   nNode = nList.item(temp);
		   if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                          
                          //String agent = nNode.getAttributes().getNamedItem("agent").getNodeValue();
                          String itemhost =  "";// nNode.getAttributes().getNamedItem("host").getNodeValue();
                          String prefix2 =  "";//nNode.getAttributes().getNamedItem("prefix").getNodeValue(); 
                          String active="true";
                          
                          String  zabbix_server_host="default";
                          String  zabbix_server_port="0";
                          String  zabbix_server_name="default";
                          
                         if (nNode.hasAttributes()){

                                NamedNodeMap  baseElmnt_attr = nNode.getAttributes();
                                for (int i = 0; i <  baseElmnt_attr.getLength(); ++i)
                                {
                                    Node attr =  baseElmnt_attr.item(i);

                                    if (attr.getNodeName().equalsIgnoreCase("host")){
                                        itemhost =  nNode.getAttributes().getNamedItem("host").getNodeValue();
                                    } else if (attr.getNodeName().equalsIgnoreCase("prefix")){
                                        prefix2 =   nNode.getAttributes().getNamedItem("prefix").getNodeValue();
                                    } else if (attr.getNodeName().equalsIgnoreCase("zabbix_server")){
                                        zabbix_server_name  =   nNode.getAttributes().getNamedItem("zabbix_server").getNodeValue();
                                    } else if (attr.getNodeName().equalsIgnoreCase("active")){
                                        active =   nNode.getAttributes().getNamedItem("active").getNodeValue();
                                    }
                           
                                }
                        }  
                          
                         boolean isZabbixServer = false;

                         for (ZabbixServerApi zabbix_server1 :zabbixServerApiList) {
                               if (zabbix_server1.getServerName().equalsIgnoreCase(zabbix_server_name)) {
                                   zabbix_server_host=zabbix_server1.getHost();
                                   zabbix_server_port=zabbix_server1.getStringPort();
                                   isZabbixServer = true;
                                   break;
                               }
                          }  

                        if (!isZabbixServer){
                              zabbix_server_host="localhost";
                              zabbix_server_port="10500";
                              zabbix_server_name="not_exist";                               
                              logger.error("!isZabbixServer " + zabbix_server_name);
                        }
                        
                   
                        readItems(nNode, active, zabbix_server_name, itemhost, prefix1, prefix2, filename);
                        
                                          
		   }
		}
                
                logger.info("Read db-to-zabbix from " + filename + " done");
       
                
         }  catch (ParserConfigurationException | SAXException | IOException | NumberFormatException e) {            
                logger.fatal("db-to-zabbix::XML Exception/Error:", e);
                System.exit(-1);
				
	  }
    } 
 
    
    private void readItems(Node nNode, String active, String zabbix_server_name, String itemhost, String prefix1, String prefix2, String filename){
     
              Element eElementMain = (Element) nNode;
              NodeList  nListItem = eElementMain.getElementsByTagName("item");

              for (int it = 0; it < nListItem.getLength(); it++) {
                   Node nNodeItem = nListItem.item(it);
                                     
                    if (nNodeItem.getNodeType() == Node.ELEMENT_NODE) {
                         Element eElementItem = (Element) nNodeItem;

                         String itemKey = eElementItem.getAttributes().getNamedItem("key").getNodeValue();

                         logger.info("Processing " + zabbix_server_name+"."+itemhost+"."+prefix1+prefix2+itemKey);

                         DBApi DBApi = new DBApi();

                         DBApi.setAlias(zabbix_server_name+"."+itemhost+"."+prefix1+prefix2+itemKey);
                         DBApi.setZabbixKey(prefix1+prefix2+itemKey);
                         DBApi.setZabbixHost(itemhost);                                     
                         DBApi.setSettingFile(filename);
                         if (active.equals("false")) DBApi.setToInactiveMode();

                         DBApi.setZabbixServerName(zabbix_server_name);
                         sqlType type = sqlType.PURE;     

                        String _type = "PURE";
                        boolean skipLineSplit=false;

                        if (eElementItem.getElementsByTagName("sql-query").item(0).hasAttributes()){
                            
                            NamedNodeMap  baseElmnt_attr = eElementItem.getElementsByTagName("sql-query").item(0).getAttributes();
                            for (int i = 0; i <  baseElmnt_attr.getLength(); ++i) {
                                Node attr =  baseElmnt_attr.item(i);
                                logger.debug(attr.getNodeName() + " = " + attr.getNodeValue());
                                if (attr.getNodeName().equals("type")){
                                    try {
                                         _type = eElementItem.getElementsByTagName("sql-query").item(0).getAttributes().getNamedItem("type").getNodeValue().toUpperCase();
                                          type = sqlType.valueOf(_type);
                                     } catch (Exception e){
                                          DBApi.addErrorToConfigErrorList("No enum constant " + _type);
                                         logger.error("No enum constant " + _type);
                                     }

                                } else  if (attr.getNodeName().equals("skipLineSplit") && attr.getNodeValue().equalsIgnoreCase("true")){
                                    skipLineSplit=true;
                                } 

                            }
                            
                             
                        } else {
                             logger.warn("sql-query has not attributes !!!!");
                        } 

                        if (logger.isDebugEnabled()) {
                           logger.debug("Query type " + type.toString());
                        }


                        DBApi.setSqlType(type);
                        DBApi.setSqlQuery("SELECT 1");

                        String cron_expression = "";

                      if (eElementItem.getElementsByTagName("cron_expression").item(0).hasAttributes()){

                            NamedNodeMap  baseElmnt_attr = eElementItem.getElementsByTagName("cron_expression").item(0).getAttributes();
                            for (int i = 0; i <  baseElmnt_attr.getLength(); ++i) {
                                Node attr =  baseElmnt_attr.item(i);
                                logger.debug(attr.getNodeName() + " = " + attr.getNodeValue());
                                if (attr.getNodeName().equals("group") && attr.getNodeValue().equalsIgnoreCase("true")){
                                    String group = getTagValue("cron_expression", eElementItem);
                                    if (CrontabGroupsList.containsKey(group)){
                                        cron_expression = CrontabGroupsList.get(group);
                                        logger.debug("Set crontab expression " + cron_expression + " -> " + group);
                                    } else { 
                                        DBApi.addErrorToConfigErrorList("Crontab group not found " + group);
                                        logger.error("Crontab group not found " + group);
                                        cron_expression = "0 0 * * * ?";
                                    }
                                }

                            }
                         }  else {
                             cron_expression = getTagValue("cron_expression", eElementItem);
                         }  

                        DBApi.setCronExpression(cron_expression);


                        if (type != sqlType.MAINTENANCE && type!= sqlType.COMPARE  && type!= sqlType.DISCOVERY_STATIC){
                              boolean isJdbcSource = false;
                              String [] jdbc_interfaces=getTagValue("jdbc_interface", eElementItem).split(",");
                              for (String interfaceS:jdbc_interfaces){

                                  for (JdbcDriverApi jdbc_driver :  JdbcDriverApiList) {
                                       if (jdbc_driver.getInterfaceName().equalsIgnoreCase(interfaceS)) {
                                           DBApi.setJdbcDriver(jdbc_driver);
                                           isJdbcSource = true;
                                           break;
                                       }
                                   }

                                  if (!isJdbcSource){
                                      DBApi.addErrorToConfigErrorList("!isJdbcSource " + getTagValue("jdbc_interface", eElementItem));
                                      logger.error("!isJdbcSource " + getTagValue("jdbc_interface", eElementItem));
                                      System.exit(5);
                                  } 
                              }
                       } 

                       if (logger.isDebugEnabled()) logger.debug("sql-param size " + eElementItem.getElementsByTagName("sql-param").getLength());

                       for (int count = 0; count < eElementItem.getElementsByTagName("sql-param").getLength(); count++) {


                              if (eElementItem.getElementsByTagName("sql-param").item(count).hasAttributes()){

                                    NamedNodeMap  baseElmnt_attr = eElementItem.getElementsByTagName("sql-param").item(count).getAttributes();
                                    for (int i = 0; i <  baseElmnt_attr.getLength(); ++i)
                                    {
                                        Node attr =  baseElmnt_attr.item(i);

                                        if (attr.getNodeName().equalsIgnoreCase("key")){
                                            logger.debug(attr.getNodeValue() + " = " + eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            if (attr.getNodeValue().equalsIgnoreCase("fullquery")) {
                                                DBApi.setFullQuery(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("fireatstartup")) {
                                                DBApi.setFireAtStartup(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("sendtozabbix")) {
                                                DBApi.setSentToZbx(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("string")) {
                                                DBApi.setIsString(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("numeric")) {
                                                DBApi.setIsNumeric(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("integer")) {
                                                DBApi.setIsInteger(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("float")) {
                                                DBApi.setIsFloat(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("long")) {
                                                DBApi.setIsLong(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("column")) {
                                                DBApi.setColumn(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("row")) {
                                                DBApi.setRow(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("timing")) {
                                                DBApi.setIsTiming(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("show_count")) {
                                                DBApi.setShowCount(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("html")) {
                                                DBApi.setHtml(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("more_stmt")) {
                                                DBApi.setMoreStmt(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("isolation_level")) {
                                                DBApi.setIsolationLevel(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("no_data")) {
                                                DBApi.setNoData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("complementary")) {
                                                DBApi.setComplementary_query(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("complementaryMapAndMetadata")) {
                                               DBApi.setComplementaryMapAndMetaData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryComplementary")) {
                                                long delay=1000;
                                                try {
                                                   delay=Long.parseLong(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                } catch (Exception iii){}
                                                DBApi.setComplementaryUpdateInterval(delay);
                                            } else if (attr.getNodeValue().equalsIgnoreCase("discovery")) {
                                                DBApi.setDiscoveryQuery(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("discoveryMapAndMetadata")) {
                                                DBApi.setDiscoveryMapAndMetaData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("discoveryMetadata")) {
                                                DBApi.setDiscoveryMetaData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryMap")) {
                                                DBApi.setDiscoveryMap(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryParams")) {
                                                DBApi.setDiscoveryParamsType(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            } else if (attr.getNodeValue().equalsIgnoreCase("discoveryUpdate")) {
                                                long delay=1000;
                                                try {
                                                   delay=Long.parseLong(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                } catch (Exception iii){}
                                                DBApi.setDiscoveryUpdateInterval(delay);
                                            } else if (attr.getNodeValue().equalsIgnoreCase("onlyFirstDatabase")) {
                                                DBApi.setOnlyFirstDatabase(Boolean.parseBoolean(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent()));
                                            } else if (attr.getNodeValue().equalsIgnoreCase("prepareQuery")) {
                                                DBApi.setPrepareQuery(Boolean.parseBoolean(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent()));
                                            } else if (attr.getNodeValue().equalsIgnoreCase("pg_database")) {
                                                DBApi.setPg_database(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }  else {                                                        
                                                DBApi.setDatabaseProperty(attr.getNodeValue(), eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                            }
                                        } else {
                                             DBApi.addErrorToConfigErrorList("unknown attribute " + attr.getNodeName());
                                            logger.error("unknown attribute " + attr.getNodeName());
                                        }

                                    }
                              } else { 
                                  DBApi.addErrorToConfigErrorList("sql-param has not attributes !!!!");
                                  logger.error("sql-param has not attributes !!!!");
                              } 
                        }

                       
                       StringBuilder sb_query = new StringBuilder();
                       String full_query = "default";

                      // ssh_command = getTagValue("ssh_command", eElementItem);

                       NodeList nListCmd =  eElementItem .getElementsByTagName("sql-query");

                       for (int replaceIndx = 0; replaceIndx < nListCmd.getLength(); replaceIndx++) {
                              NodeList nListReplace = nListCmd.item(replaceIndx).getChildNodes();
                              Node nValue = (Node) nListReplace.item(0);
                              if (skipLineSplit) sb_query.append(nValue.getNodeValue()).append(";;");
                              else sb_query.append(nValue.getNodeValue());
                       }
                       
                       full_query=sb_query.toString();

                       switch (type) {
                            case PURE:  
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     break; 


                            case PURE_WITH_MULTIPLE:  
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     break;   

                            case PURE_WITH_REVERSE_MULTIPLE:  
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     break;  

                            case MAINTENANCE:  
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     //DBApi.setDBInterface("");
                                     break; 


                            case DISCOVERY_QUERY: 
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     break; 

                            case DISCOVERY_STATIC: 
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     break;    

                            case EMBEDDED: 
                                    // _query = getTagValue("sql-query", eElementItem);
                                     DBApi.setSqlQuery(full_query);
                                     break;  

                            case PREDEFINED: 
                                    boolean isqueryType = false;
                                    //_query = getTagValue("sql-query", eElementItem);
                                    for (QueryApi query : queryList) {
                                          if (query.getName().equalsIgnoreCase(full_query)) {
                                              full_query = query.getQuery();
                                              String _param = query.getQueryProperty("txt_params", "NO");
                                              String _params [] = _param.split(",");
                                              //podmiana w zapytaniu
                                              if (!_param.equalsIgnoreCase("NO")){
                                                  for (String param : _params) {
                                                      String value = DBApi.getDatabaseProperty(param, "NO_SETTING_VALUE");
                                                      if (value.equalsIgnoreCase("NO_SETTING_VALUE")){
                                                          logger.warn("NO_SETTING_VALUE for " + param);
                                                      } else {
                                                          full_query = full_query.replaceAll(param, value);
                                                      }
                                                  }
                                              }
                                              logger.info("full query: " + full_query);
                                              DBApi.setSqlQuery(full_query); 

                                               //po nowemu
                                              Set<Object> keys = query.getQueryProperties().keySet();
                                              for(Object k:keys){
                                                     String key = (String)k;
                                                     boolean isKey=false;
                                                     logger.debug(key + " = " + query.getQueryProperties().getProperty(key));
                                                     if (key.equalsIgnoreCase("fullquery")) {
                                                            DBApi.setFullQuery(query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                     }  else if (key.equalsIgnoreCase("fireatstartup")) {
                                                            DBApi.setFireAtStartup(query.getQueryProperties().getProperty(key));
                                                            isKey=true; 
                                                     }  else if (key.equalsIgnoreCase("sendtozabbix")) {
                                                            DBApi.setSentToZbx(query.getQueryProperties().getProperty(key));
                                                            isKey=true; 
                                                     } else if (key.equalsIgnoreCase("string")) {
                                                            DBApi.setIsString(query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                     } else if (key.equalsIgnoreCase("numeric")) {
                                                            DBApi.setIsNumeric( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                     } else if (key.equalsIgnoreCase("integer")) {
                                                            DBApi.setIsInteger( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("float")) {
                                                            DBApi.setIsFloat( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("long")) {
                                                            DBApi.setIsLong( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("column")) {
                                                            DBApi.setColumn( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("row")) {
                                                            DBApi.setRow( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("timing")) {
                                                            DBApi.setIsTiming( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("more_stmt")) {
                                                            DBApi.setMoreStmt( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("isolation_level")) {
                                                            DBApi.setIsolationLevel( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("no_data")) {
                                                            DBApi.setNoData( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("show_count")) {
                                                            DBApi.setShowCount(query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("html")) {
                                                            DBApi.setHtml(query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("discovery")) {
                                                           DBApi.setDiscoveryQuery( query.getQueryProperties().getProperty(key));
                                                           isKey=true;
                                                    } else if (key.equalsIgnoreCase("discoveryMapAndMetadata")) {
                                                            DBApi.setDiscoveryMapAndMetaData( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("discoveryMetadata")) {
                                                            DBApi.setDiscoveryMetaData( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("discoveryMap")) {
                                                            DBApi.setDiscoveryMap( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    }  else if (key.equalsIgnoreCase("discoveryParams")) {
                                                            DBApi.setDiscoveryParamsType( query.getQueryProperties().getProperty(key));
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("discoveryUpdate")) {
                                                            long delay=1000;
                                                            try {
                                                               delay=Long.parseLong( query.getQueryProperties().getProperty(key));
                                                            } catch (Exception iii){}
                                                            DBApi.setDiscoveryUpdateInterval(delay);
                                                            isKey=true;
                                                    } else if (key.equalsIgnoreCase("txt_params")) {
                                                            //do nothing
                                                            isKey=true;
                                                    } else {                                                        
                                                            DBApi.setDatabaseProperty(key,  query.getQueryProperties().getProperty(key));
                                                    } 
                                                    if (!isKey){
                                                        logger.debug("Set database property " + key + " = " + query.getQueryProperties().getProperty(key));
                                                    }
                                                }

                                                isqueryType = true;
                                                break;
                                          }
                                      }     
                                      if (!isqueryType)  {
                                          DBApi.addErrorToConfigErrorList("!isqueryType for " + getTagValue("sql-query", eElementItem));
                                         logger.error("!isqueryType for " + getTagValue("sql-query", eElementItem));
                                      }

                                      break;
                            case COMPARE: 
                                  boolean isJdbcSource1 = false;
                                  boolean isJdbcSource2 = false;

                                  for (JdbcDriverApi jdbc_driver  : JdbcDriverApiList) {
                                       if (jdbc_driver.getInterfaceName().equalsIgnoreCase(getTagValue("jdbc_master", eElementItem))) {
                                           DBApi.setDatabaseProperty("jdbc_host_master", jdbc_driver.getHost());
                                           DBApi.setDatabaseProperty("jdbc_port_master", jdbc_driver.getStringPort());
                                           DBApi.setDatabaseProperty("jdbc_user_master", jdbc_driver.getUser());
                                           DBApi.setDatabaseProperty("jdbc_password_master", jdbc_driver.getPassword()); 
                                           DBApi.setDatabaseProperty("jdbc_database_master", jdbc_driver.getDefaultDatabase());
                                           DBApi.setDatabaseProperty("jdbc_url_master", jdbc_driver.getUrl());
                                           DBApi.setDatabaseProperty("jdbc_class_master", jdbc_driver.getClassName());
                                           isJdbcSource1 = true;
                                           break;
                                       }
                                   }

                                  if (!isJdbcSource1){
                                      DBApi.setDatabaseProperty("jdbc_host_master", "0.0.0.0");
                                      DBApi.setDatabaseProperty("jdbc_port_master", "0");
                                      DBApi.setDatabaseProperty("jdbc_user_master", "usr");
                                      DBApi.setDatabaseProperty("jdbc_password_master", "pwd");
                                      DBApi.setDatabaseProperty("jdbc_database_master", "master");
                                      DBApi.setDatabaseProperty("jdbc_url_master", "url");
                                      DBApi.setDatabaseProperty("jdbc_class_master", "class");
                                      DBApi.addErrorToConfigErrorList("!isJdbcSource1 " + getTagValue("jdbc_master", eElementItem));
                                      logger.error("!isJdbcSource1 " + getTagValue("jdbc_master", eElementItem));
                                  }  

                                  for (JdbcDriverApi jdbc_driver2  : JdbcDriverApiList) {
                                       if (jdbc_driver2.getInterfaceName().equalsIgnoreCase(getTagValue("jdbc_slave", eElementItem))) {
                                           DBApi.setDatabaseProperty("jdbc_host_slave", jdbc_driver2.getHost());
                                           DBApi.setDatabaseProperty("jdbc_port_slave", jdbc_driver2.getStringPort());
                                           DBApi.setDatabaseProperty("jdbc_user_slave", jdbc_driver2.getUser());
                                           DBApi.setDatabaseProperty("jdbc_password_slave", jdbc_driver2.getPassword());
                                           DBApi.setDatabaseProperty("jdbc_database_slave", jdbc_driver2.getDefaultDatabase());
                                           DBApi.setDatabaseProperty("jdbc_url_slave", jdbc_driver2.getUrl());
                                           DBApi.setDatabaseProperty("jdbc_class_slave", jdbc_driver2.getClassName());
                                           isJdbcSource2 = true;
                                           break;
                                       }
                                   }

                                  if (!isJdbcSource2){
                                      DBApi.setDatabaseProperty("jdbc_host_slave", "0.0.0.0");
                                      DBApi.setDatabaseProperty("jdbc_port_slave", "0");
                                      DBApi.setDatabaseProperty("jdbc_user_slave", "usr");
                                      DBApi.setDatabaseProperty("jdbc_password_slave", "pwd");
                                      DBApi.setDatabaseProperty("jdbc_database_slave", "master");
                                      DBApi.setDatabaseProperty("jdbc_url_slave", "url");
                                      DBApi.setDatabaseProperty("jdbc_class_slave", "class");
                                      DBApi.addErrorToConfigErrorList("!isJdbcSource2 " + getTagValue("jdbc_slave", eElementItem));
                                      logger.error("!isJdbcSource2 " + getTagValue("jdbc_slave", eElementItem));
                                  }


                                  DBApi.setDatabaseProperty("sql-query_master", getTagValue("sql-query_master", eElementItem));
                                  DBApi.setDatabaseProperty("sql-query_slave", getTagValue("sql-query_slave", eElementItem));

                                  isqueryType = true;
                                     break;
                            default: //;  
                                DBApi.setSqlQuery("SELECT -2");
                                DBApi.addErrorToConfigErrorList("Unknown query type");
                                logger.error("Unknown query type");
                                break;
                        }
                        WorkingStats.initLockForQueue();
                                            
                        DBApiList.add(DBApi);
                        
                    }
          }
    }
  
   private static String getTagValue(String sTag, Element eElement) {
	
        try {
            NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
            Node nValue = (Node) nlList.item(0);
            return nValue.getNodeValue();
        } catch (Exception e){
            logger.error("getTagValue error " + sTag + " "+ e);
            return "ERROR";
        }

	
}    
   
    public Set<Object> getAllKeys(Properties prop){
        Set<Object> keys = prop.keySet();
        return keys;
    }

    public String getPropertyValue(Properties prop, String key){
        return prop.getProperty(key);
    }
  
 
}