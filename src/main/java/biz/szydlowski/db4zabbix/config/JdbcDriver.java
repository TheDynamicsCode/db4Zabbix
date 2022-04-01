/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.config;

import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import biz.szydlowski.db4zabbix.api.JdbcDriverVendorApi;
import biz.szydlowski.utils.OSValidator;
import biz.szydlowski.utils.template.TemplateFile;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
public class JdbcDriver {
 
   private List<JdbcDriverApi> jdbcDriverApiList = new ArrayList<>();
   private List<JdbcDriverVendorApi> jdbcDriverTypeApiList = new ArrayList<>();
    
   private String _setting="setting/jdbc-driver.xml";
   private int pool_index;
   
   static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.config.JdbcDriver");

       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
     public  JdbcDriver (){
         
         if (OSValidator.isUnix()){
              String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
              _setting = absolutePath + "/" + _setting;
         }
        
         jdbcDriverTypeApiList  = new  JdbcDriverVendor().getJdbcTypeApiList();
       
         pool_index=0;
         
         addPropsFromFile(_setting);
         
         TemplateFile _InterTemplate = new TemplateFile("interfaces");
         for (String file :  _InterTemplate.getFilenames()){
            addPropsFromFile(file);
        }
     }
     
     
    private void addPropsFromFile(String filename){ 
          
         try {
                        
		File fXmlFile = new File(filename);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
             
		logger.info("Read jdbc-driver " + filename);
                
                
                NodeList  nList = doc.getElementsByTagName("jdbc_interface");
              
                     
		for (int temp = 0; temp < nList.getLength(); temp++) {
                
		   Node nNode = nList.item(temp);
		   if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                          Element eElement = (Element) nNode;
                                                
                          JdbcDriverApi jdbcDriverApi = new JdbcDriverApi();
                          
                          jdbcDriverApi.setInterfaceName( getTagValue("interface_name", eElement));
                          jdbcDriverApi.setInterfaceType( getTagValue("interface_type", eElement));
                         
                          String drv_type = getTagValue("driver_type", eElement);
                                                    
                          jdbcDriverApi.setDriverVendor(drv_type);
                          
                          boolean isDriver=false;
                          
                          for (JdbcDriverVendorApi jdbcDriverTypeApi : jdbcDriverTypeApiList){
                              if (jdbcDriverTypeApi.getDriverType().equalsIgnoreCase(drv_type)){
                                  isDriver=true;
                                  jdbcDriverApi.setUrlTemplate(jdbcDriverTypeApi.getUrlTemplate());
                                  jdbcDriverApi.setClassName(jdbcDriverTypeApi.getClassName());
                              }
                          }
                         
                          if (!isDriver) {
                              logger.error ("Driver type not found " + drv_type);
                              continue;
                          }
                         
                          if ( getTagValue("interface_type", eElement).equalsIgnoreCase("single") ){
                               jdbcDriverApi.setConnectionRetry(getTagValue("connectionRetryCount", eElement));
                               jdbcDriverApi.setLoginTimeout(getTagValue("loginTimeout", eElement));
                          } else {
                               jdbcDriverApi.setPoolIndex(pool_index);
                               pool_index++;
                          }
                          
                          for (int count = 0; count < eElement.getElementsByTagName("pool-param").getLength(); count++) {
                                 if (eElement.getElementsByTagName("pool-param").item(count).hasAttributes()){

                                        NamedNodeMap  baseElmnt_attr = eElement.getElementsByTagName("pool-param").item(count).getAttributes();
                                        for (int i = 0; i <  baseElmnt_attr.getLength(); ++i)
                                        {
                                            Node attr =  baseElmnt_attr.item(i);

                                            if (attr.getNodeName().equalsIgnoreCase("key")){
                                                logger.debug("pool-praram " + attr.getNodeValue() + " = " + eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                if (attr.getNodeValue().equals("minPoolSize")){
                                                   jdbcDriverApi.setMinPoolSize(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("maxPoolSize")){
                                                   jdbcDriverApi.setMaxPoolSize(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("maxStatements")){
                                                   jdbcDriverApi.setMaxStatements(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("acquireIncrement")){
                                                   jdbcDriverApi.setAcquireIncrement(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("loginTimeout")){
                                                   jdbcDriverApi.setLoginTimeout(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("extendedOption")){
                                                   jdbcDriverApi.setExtendedOption(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                }
                                            } else {
                                                logger.error("unknown attribute " + attr.getNodeName());
                                            }

                                        }
                                  } else {
                                        logger.error("sql-param has not attributes !!!!");
                                  }
                          }
                          
                          
                          jdbcDriverApi.setDefaultDatabase (getTagValue("database", eElement));
                          jdbcDriverApi.setHost(getTagValue("host", eElement));
                          jdbcDriverApi.setPort(getTagValue("port", eElement));
                          jdbcDriverApi.setUser(getTagValue("user", eElement));
                          jdbcDriverApi.setPassword(getTagValue("password", eElement));
                          jdbcDriverApi.generateUrl();
                          
                          jdbcDriverApiList.add(jdbcDriverApi);
                        
		   }
		}
                
                logger.info("Read jdbc-driver done");
                                
         }  catch (ParserConfigurationException | SAXException | IOException e) {         
                logger.fatal("jdbc-driver.xml::XML Exception/Error:", e);
                System.exit(-1);
				
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
  
  public  List<JdbcDriverApi> getJdbcDriverApiList(){
          return  jdbcDriverApiList;
  }

}
