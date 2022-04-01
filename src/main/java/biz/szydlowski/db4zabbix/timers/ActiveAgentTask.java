/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.timers;


import static biz.szydlowski.db4zabbix.WorkingObjects.DBApiList;
import static biz.szydlowski.db4zabbix.WorkingObjects.zabbixServerApiList;
import biz.szydlowski.db4zabbix.sqlType;
import biz.szydlowski.zabbixmon.DataObject;
import biz.szydlowski.zabbixmon.SenderResult;
import biz.szydlowski.zabbixmon.ZabbixSender;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;
import biz.szydlowski.utils.WorkingStats;
import static biz.szydlowski.utils.tasks.TasksWorkspace.ActiveAgentTimer_lock;
import static biz.szydlowski.utils.tasks.TasksWorkspace.ActiveAgentTimer_time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 *
 * @author Dominik
 */
public class ActiveAgentTask extends TimerTask  {
    
        static final Logger logger = LogManager.getLogger(ActiveAgentTask.class);

        //new
        List<DataObject> dataObject_s = new ArrayList<>();
        List<DataObject> dataObject_nsender = new ArrayList<>();
        int maxSenderPacketSize = 200;
        int qSender=0;

        StringBuilder k = new StringBuilder();
        SenderResult result; 
        String zbkey = "zbkey";

        List<ZabbixSender> zabbixSenderList = new ArrayList<>();
        boolean firstRun = true;

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


        @Override
        public void run() {

             ActiveAgentTimer_time = System.currentTimeMillis();

             if (firstRun){
                logger.info("Setting zabbixServerApi in ActiveAgentTask....");
                firstRun=false;
                for (ZabbixServerApi zabbixServerApi : zabbixServerApiList){

                       ZabbixSender zabbixSender = new ZabbixSender(zabbixServerApi.getHost(), zabbixServerApi.getPort(), zabbixServerApi.getConnectTimeout(), zabbixServerApi.getSocketTimeout());
                       zabbixSenderList.add(zabbixSender);

                }
            }
            if (ActiveAgentTimer_lock){
                logger.info("ActiveAgentTimer is locked");
                return;
            } else {
                ActiveAgentTimer_lock=true;
            }

            try {
                for (int listServer=0; listServer<zabbixServerApiList.size(); listServer++){

                   boolean setAgent=false;
                   if (dataObject_s!=null) dataObject_s.clear();

                   for (int queue=0; queue<DBApiList.size(); queue++){

                      if (!DBApiList.get(queue).getZabbixServerName().equalsIgnoreCase(zabbixServerApiList.get(listServer).getServerName())){
                          continue;
                       } else {
                           setAgent=true;

                       }   


                      boolean pure_multiple = false;
                      boolean discovery = false;
                      boolean pure_reverse_multiple = false; 

                      if (DBApiList.get(queue).getSqlType()==sqlType.PURE_WITH_MULTIPLE){
                          pure_multiple=true;
                      }  

                      if (DBApiList.get(queue).getSqlType()==sqlType.PURE_WITH_REVERSE_MULTIPLE){
                          pure_reverse_multiple=true;
                      }

                      if (DBApiList.get(queue).getSqlType()==sqlType.DISCOVERY_QUERY || DBApiList.get(queue).getSqlType()==sqlType.DISCOVERY_STATIC){
                           discovery=true;
                      }

                      if ( DBApiList.get(queue).isReadyToSend()){
                          if (logger.isDebugEnabled()) logger.debug("Prepare to sending throw active agent using " + DBApiList.get(queue).getZabbixKey());

                          try {

                              if (discovery){ //logger.debug("**** DISCOVERY ****");

                                     //   List<List<String>> getData = DBApiList.get(queue).getDiscoveryMap();

                                        if (DBApiList.get(queue).getDiscoveryMap()!=null){
                                           if (DBApiList.get(queue).getDiscoveryMap().size()>0){

                                             // List<String> metaData = DBApiList.get(queue).getDiscoveryMetaData();

                                              DataObject dataObject = new DataObject();
                                              dataObject.setHost(DBApiList.get(queue).getZabbixHost());
                                              dataObject.setKey(DBApiList.get(queue).getZabbixKey());
                                              JSONObject data = new JSONObject();

                                              List<JSONObject> aray = new LinkedList<>();


                                              for(int i=0; i<DBApiList.get(queue).getDiscoveryMap().size();i++) {

                                                   // List <String> keys_value = getData.get(i);
                                                    if (DBApiList.get(queue).getDiscoveryMap().get(i).size()!=DBApiList.get(queue).getDiscoveryMetaData().size()){
                                                        logger.error("Consist error metaData");
                                                        continue;
                                                    }

                                                    JSONObject xxx = new JSONObject();

                                                    for (int j=0; j<DBApiList.get(queue).getDiscoveryMetaData().size();j++){                                                   

                                                       k.delete(0, k.length());
                                                       k.append("{#").append(DBApiList.get(queue).getDiscoveryMetaData().get(j)).append("}"); 
                                                       xxx.put(k.toString(), DBApiList.get(queue).getDiscoveryMap().get(i).get(j));

                                                    }  

                                                    aray.add(xxx);

                                               }  

                                              data.put("data", aray);

                                              logger.debug(data.toJSONString());

                                              dataObject.setValue(data.toJSONString());
                                              dataObject.setClock(System.currentTimeMillis()/1000);

                                              dataObject_s.add(dataObject); 

                                           }

                                        }

                                  }  else  if (pure_multiple){
                                      //  List<List<String>> getData = DBApiList.get(queue).getDiscoveryMap();
                                        if (DBApiList.get(queue).getDiscoveryMap()!=null){
                                            if (DBApiList.get(queue).getDiscoveryMap().size()>0){
                                               // List<String> metaData = DBApiList.get(queue).getDiscoveryMetaData();

                                                int valcol=-1;

                                                for (int j=0; j<DBApiList.get(queue).getDiscoveryMetaData().size();j++){
                                                   if (DBApiList.get(queue).getDiscoveryMetaData().get(j).equalsIgnoreCase("VALUE")){
                                                       valcol=j;
                                                       break;
                                                   }
                                                }

                                                if (valcol>-1){

                                                    for(int i=0; i<DBApiList.get(queue).getDiscoveryMap().size(); i++) {


                                                        if (DBApiList.get(queue).getDiscoveryMap().get(i).size()!=DBApiList.get(queue).getDiscoveryMetaData().size()){
                                                                logger.error("Consist error metaData");
                                                                continue;
                                                        }

                                                       String zbkey = DBApiList.get(queue).getZabbixKey();
                                                       for (int j=0; j<DBApiList.get(queue).getDiscoveryMap().get(i).size();j++){
                                                            if (j==valcol) continue;
                                                            String mtd = "{#"+DBApiList.get(queue).getDiscoveryMetaData().get(j)+"}";
                                                            zbkey=zbkey.replace(mtd.toUpperCase(), DBApiList.get(queue).getDiscoveryMap().get(i).get(j));
                                                       }

                                                        DataObject dataObject = new DataObject();
                                                        dataObject.setHost(DBApiList.get(queue).getZabbixHost());
                                                        dataObject.setKey(zbkey);
                                                        dataObject.setValue(DBApiList.get(queue).getDiscoveryMap().get(i).get(valcol));
                                                        dataObject.setState(0);
                                                        dataObject.setClock(System.currentTimeMillis()/1000);

                                                        dataObject_s.add(dataObject);
                                                        logger.debug(dataObject.toString());

                                                    }
                                                } else {
                                                    logger.error("Value data not found!!!");
                                                }
                                            }
                                        } else {
                                              logger.error("getData is null");  
                                        }
                               } else  if (pure_reverse_multiple){
                                       if (DBApiList.get(queue).getDiscoveryMap()!=null){

                                           if (!DBApiList.get(queue).getDiscoveryMap().isEmpty()){

                                                int stat=-1;
                                                String zbkey = DBApiList.get(queue).getZabbixKey();
                                                String zbkeytmp = "";
                                                if (zbkey.contains("{#STAT}")) stat=1;


                                                if (stat>-1){


                                                    String [] pcolumns = DBApiList.get(queue).getDatabaseProperty("paramsColumn", "DefaultColumn").toUpperCase().split(",");
                                                    int [] pcolumnsMap = new int [pcolumns.length];

                                                    //prepare
                                                    for (int h=0; h<pcolumns.length;h++){
                                                        pcolumnsMap[h]=-1;
                                                    }

                                                    //prepare
                                                    for (int h=0; h<pcolumns.length;h++){
                                                         for (int l=0; l<DBApiList.get(queue).getDiscoveryMetaData().size();l++){
                                                           if (DBApiList.get(queue).getDiscoveryMetaData().get(l).equalsIgnoreCase(pcolumns[h])){
                                                                 pcolumnsMap[h]=l;
                                                                 logger.debug("FOUND STAT -> " + l + " " + DBApiList.get(queue).getDiscoveryMetaData().get(l));
                                                                // break;
                                                           }
                                                         }
                                                    }  
                                                  

                                                    for(int i=0; i<DBApiList.get(queue).getDiscoveryMap().size(); i++) { //i-ty wiersz z kolejki

                                                       zbkey = DBApiList.get(queue).getZabbixKey(); //reset

                                                       if (DBApiList.get(queue).getDiscoveryMap().get(i).size()!=DBApiList.get(queue).getDiscoveryMetaData().size()){
                                                                logger.error("Consist error metaData");
                                                                continue;
                                                       }
                                                       String mtd = "";
                                                       //zmiany w kolumach z parametrem statycznym
                                                       for (int h=0; h<pcolumnsMap.length;h++){
                                                          if (pcolumnsMap[h]>-1) {
                                                              mtd="{#"+DBApiList.get(queue).getDiscoveryMetaData().get(pcolumnsMap[h])+"}";                                                              
                                                              zbkey=zbkey.replace(mtd.toUpperCase(), DBApiList.get(queue).getDiscoveryMap().get(i).get(pcolumnsMap[h])); 
                                                          }
                                                       }


                                                       for (int j=0; j<DBApiList.get(queue).getDiscoveryMap().get(i).size();j++){ //każda kolumna j-ta i-ty wiersz

                                                            if (!checkParmsColumn(DBApiList.get(queue).getDiscoveryMetaData().get(j), pcolumns)){ //kolumna params
                                                                logger.debug("STAT -> " + DBApiList.get(queue).getDiscoveryMetaData().get(j));
                                                                zbkeytmp=zbkey.replace("{#STAT}", DBApiList.get(queue).getDiscoveryMetaData().get(j));                                                               

                                                                DataObject dataObject = new DataObject();
                                                                dataObject.setHost(DBApiList.get(queue).getZabbixHost());
                                                                dataObject.setKey(zbkeytmp);
                                                                dataObject.setValue(DBApiList.get(queue).getDiscoveryMap().get(i).get(j));
                                                                dataObject.setState(0);
                                                                dataObject.setClock(System.currentTimeMillis()/1000);

                                                                dataObject_s.add(dataObject);
                                                                //System.out.println(dataObject.toString());
                                                                //logger.debug(dataObject.toString());
                                                            } else {
                                                                logger.debug("!STAT " + DBApiList.get(queue).getDiscoveryMetaData().get(j));
                                                            }


                                                       }

                                                    }
                                                } else {
                                                    logger.error("Value STAT not found!!!");
                                                }
                                            } else {
                                               logger.error("getDiscoveryMap().isEmpty()");
                                           }
                                        } else {
                                              logger.error("getDiscoveyMap is null");  
                                        }
                               } else {

                                  /*if(DBApiList.get(queue).getStateList().get(0)!=0){
                                      System.out.println("Not supported item " + DBApiList.get(queue).getZabbixKey());
                                  }*/

                                  DataObject dataObject = new DataObject();
                                  dataObject.setHost(DBApiList.get(queue).getZabbixHost());
                                  dataObject.setKey(DBApiList.get(queue).getZabbixKey());
                                  dataObject.setValue(DBApiList.get(queue).getReturnListValue().get(0));
                                  dataObject.setState(DBApiList.get(queue).getStateList().get(0));
                                  dataObject.setClock(System.currentTimeMillis()/1000);

                                  dataObject_s.add(dataObject);
                            }

                             DBApiList.get(queue).setReadyToSendToFalse();
                        } catch (Exception e){
                                logger.error("KERNEL PANIC " + e.getMessage() + " for " +  DBApiList.get(queue).getZabbixKey()); 
                                DBApiList.get(queue).setReadyToSendToFalse();
                                WorkingStats.kernelPanicPlus();
                        } finally {
                                 DBApiList.get(queue).setReadyToSendToFalse();
                        }

                     } //done

                }

                if (dataObject_s.size()>0 && zabbixServerApiList.get(listServer).isEnabled()) {

                        if (setAgent){

                            maxSenderPacketSize = zabbixServerApiList.get(listServer).getMaxSenderPacketSize();
                            if (maxSenderPacketSize==-1) maxSenderPacketSize=dataObject_s.size(); //unlimited

                            logger.debug("dataObject to send size - " + dataObject_s.size());

                            qSender=0;
                            while (qSender<dataObject_s.size()){

                                for (int w=1; w<=maxSenderPacketSize; w++){
                                    logger.trace("add object to send nb - " + qSender);
                                    dataObject_nsender.add(dataObject_s.get(qSender));
                                    qSender++;
                                    if (qSender==dataObject_s.size()) break;
                                }

                                logger.trace("current parts to send size - " + dataObject_nsender.size());

                               if (dataObject_nsender.size()>0) {

                                       logger.debug("dataObject_nsender:" + dataObject_nsender.toString());
                                        result = zabbixSenderList.get(listServer).send(dataObject_nsender);

                                        WorkingStats.zabbixProcessedCountPlus(result.getProcessed());
                                        //#BUG20170903
                                        WorkingStats.zabbixFailedCountPlus(result.getFailed());
                                        WorkingStats.zabbixTotalCountPlus(result.getTotal());


                                        if (result.isConnError()){
                                            WorkingStats.connErrorPlus();
                                        }


                                }

                                dataObject_nsender.clear();
                            }

                        } else {
                            logger.error("Problem with set active agent???? " +zabbixServerApiList.get(listServer).getServerName());
                        }

                  }

                dataObject_s.clear();

             }  

          } catch (Exception e){
                logger.error("ERROR " + e.getMessage());
                WorkingStats.kernelPanicPlus();
          } finally {
                ActiveAgentTimer_lock=false;
          }

       };   
   
}