/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.config;

import biz.szydlowski.db4zabbix.api.QueryApi;
import biz.szydlowski.utils.OSValidator;
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
public class SqlQuery {
  
   private List<QueryApi> queryApiList  = new ArrayList<>();
   private String _setting="setting/sql-query.xml";
   
               
   static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.config.SqlQuery");
    
       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
    public  SqlQuery(){
         
          if (OSValidator.isUnix()){
              String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
              _setting = absolutePath + "/" + _setting;
          }
            
          
          try {
                        
		File fXmlFile = new File(_setting);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
 
		logger.debug("Read sql_query from " +_setting);
                
                NodeList   nList = doc.getElementsByTagName("sql_query");
                Node  nNode;
                
	        for (int temp = 0; temp < nList.getLength(); temp++) {
                    nNode = nList.item(temp);
                 
                    if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    
                        Element eElement = (Element) nNode;
                        
                        QueryApi queryApi = new QueryApi ();
                        
                        queryApi.setName(getTagValue("name", eElement));
                        queryApi.setQuery(getTagValue("query_string", eElement));
                                               
                        logger.debug(getTagValue("name", eElement) + " = " + getTagValue("query_string", eElement));
                         
                        if (eElement.getElementsByTagName("query_string").item(0).hasAttributes()){
                            
                            NamedNodeMap  baseElmnt_attr = eElement.getElementsByTagName("query_string").item(0).getAttributes();
                            for (int i = 0; i <  baseElmnt_attr.getLength(); ++i) {
                                Node attr =  baseElmnt_attr.item(i);
                                logger.debug(attr.getNodeName() + " = " + attr.getNodeValue());
                                queryApi.setQueryProperty(attr.getNodeName(), attr.getNodeValue());
                            }
                        } else {
                            logger.error("query_string has not attributes !!!!");
                        }  
                        
                        queryApiList.add(queryApi);
                              
                       
                    }
                 
                     
		 }
	       
                logger.debug("Read sql_query done");
		
         }  catch (ParserConfigurationException | SAXException | IOException e) {          
                logger.fatal("sql_query::XML Exception/Error:", e);
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

   
  public List<QueryApi> getQueryApiList (){
          return queryApiList;
  }
  
    
}  
