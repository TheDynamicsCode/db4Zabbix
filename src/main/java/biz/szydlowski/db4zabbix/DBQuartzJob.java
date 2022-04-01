 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix;



import static biz.szydlowski.db4zabbix.WorkingObjects.DBApiList;
import biz.szydlowski.utils.WorkingStats;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.UnableToInterruptJobException;

/**
 *
 * @author szydlowskidom
 */
public class DBQuartzJob implements InterruptableJob {
    
   
    static final Logger logger = LogManager.getLogger(DBQuartzJob.class);
   
    private volatile Thread  thisThread;    
    private JobKey   jobKey   = null; 
    private volatile boolean isJobInterrupted = false;

    String value = "init";
    int queue = 0;
 
  
    public  DBQuartzJob(){ 
    }

    
    @Override
    public void execute(JobExecutionContext jeContext) throws JobExecutionException {

        thisThread = Thread.currentThread();
       // logger.info("Thread name of the current job: " + thisThread.getName());

        jobKey = jeContext.getJobDetail().getKey();
        logger.debug("Job " + jobKey + " executing at " + new Date());
        boolean stillExecuting=false;
        
        try {

                JobDataMap jdMap = jeContext.getJobDetail().getJobDataMap();

                DBCore _DBCore = new DBCore();
                queue= Integer.parseInt(jdMap.get("queue").toString());
                if (queue>DBApiList.size()){
                    logger.fatal("sybaseApiList error");
                }
                if (!DBApiList.get(queue).isActiveMode()){
                     WorkingStats.setUnlockForQueue(queue);
                } else if (DBApiList.get(queue).isNowExecuting()){
                    logger.error("Job " + jobKey + " is still executing !!!!");
                     WorkingStats.idlePlus();
                     WorkingStats.setLockForQueue(queue);
                     stillExecuting = true;
                } else {
                  
                    WorkingStats.setUnlockForQueue(queue);
                    DBApiList.get(queue).setIsNowExecuting();
                    DBApiList.get(queue).clearReturnAndStateValue();
                  
                    sqlType sql_type = DBApiList.get(queue).getSqlType();
                    String  sql_query = DBApiList.get(queue).getSqlQuery();
                 
                  
                    if ( null ==sql_type ) {
                        
                        value  = _DBCore.doAndReturnData(queue);
                        
                        WorkingStats.testsCountPlus();
                        
                        if (_DBCore.isError()){
                            logger.error("DB core returned error." + DBApiList.get(queue).getAlias());
                            DBApiList.get(queue).addErrorReturnValue(_DBCore.getErrorDescription());
                            DBApiList.get(queue).setReadyToSendToTrue(); //done with errors
                            WorkingStats.errorCountPlus();
                        } else {
                            DBApiList.get(queue).addNormalReturnValue(value);
                            DBApiList.get(queue).setReadyToSendToTrue();
                            WorkingStats.okCountPlus();
                        }
                    }  else switch (sql_type) {
                                                    
                        case PURE_WITH_MULTIPLE:
                            WorkingStats.testsCountPlus();
                            
                           if (DBApiList.get(queue).getDatabaseProperty("difference", "false").equals("true")){ 
                                if (DBApiList.get(queue).isPrepareQuery()){
                                     if (DBApiList.get(queue).isGeneratedPrepareQuery()){
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();
                                     } else {
                                        DBApiList.get(queue).
                                               setGeneratedPrepareQuery(_DBCore.doPrepareQuery(sql_query));
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();  
                                         
                                    }
                                    
                                }
                              
                                preparePureWithMultiple(_DBCore, sql_query);
                               
                            } else {
                               
                                if (DBApiList.get(queue).isPrepareQuery()){
                                     if (DBApiList.get(queue).isGeneratedPrepareQuery()){
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();
                                     } else {
                                        DBApiList.get(queue).
                                               setGeneratedPrepareQuery(_DBCore.doPrepareQuery(sql_query));
                                        //System.out.println(" <<<< " + sql_query);
                                        //System.out.println(" >>>> " + _DBCore.doPrepareQuery(sql_query));
                                          
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();  
                                         
                                    }
                                    
                                } 
                               
                               
                               List<List<String>> maps =  _DBCore.getDiscoveryMap(queue, sql_query , true);                            
                                
                                if (_DBCore.isError()){
                                    logger.error("DB core returned error." + DBApiList.get(queue).getAlias());
                                    DBApiList.get(queue).setReadyToSendToFalse(); //done with errors
                                    WorkingStats.errorCountPlus();
                               }  else {
                                 if (maps!=null){
                                     if (DBApiList.get(queue).isComplementary()){
                                         setComplementaryMap(maps, _DBCore);
                                    }
                                    DBApiList.get(queue).setDiscoveryMapAndMetaData(maps);
                                  } //end maaps!=null
                               } // end else DBERROR
                               
                            }
                                
                             DBApiList.get(queue).setReadyToSendToTrue();
                             WorkingStats.okCountPlus();
                            //}  
                            break;
                       
                        case PURE_WITH_REVERSE_MULTIPLE:
                            WorkingStats.testsCountPlus();
                            
                            if (DBApiList.get(queue).getDatabaseProperty("difference", "false").equals("true")){  
                               
                                if (DBApiList.get(queue).isPrepareQuery()){
                                     if (DBApiList.get(queue).isGeneratedPrepareQuery()){
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();
                                     } else {
                                        DBApiList.get(queue).
                                               setGeneratedPrepareQuery(_DBCore.doPrepareQuery(sql_query));
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();  
                                         
                                    }
                                    
                                }
                                
                                preparePureWithReverseMultiple(_DBCore, sql_query);                              
                              
                             
                            } else {
                                
                                if (DBApiList.get(queue).isPrepareQuery()){
                                     if (DBApiList.get(queue).isGeneratedPrepareQuery()){
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();
                                     } else {
                                        DBApiList.get(queue).
                                               setGeneratedPrepareQuery(_DBCore.doPrepareQuery(sql_query));
                                         sql_query = DBApiList.get(queue).getGeneratePrepareQuery();  
                                         
                                    }
                                    
                                }
                                
                                
                                List<List<String>> maps =  _DBCore.getDiscoveryMap(queue, sql_query, false); 
                                //System.out.println(maps);
                                 
                                if (maps!=null)  DBApiList.get(queue).setDiscoveryMapAndMetaData(maps);
                                ///System.out.println(maps);
                                
                                if (_DBCore.isError()){
                                    logger.error("DB core returned error." + DBApiList.get(queue).getAlias());
                                    DBApiList.get(queue).setReadyToSendToFalse(); //done with errors
                                    WorkingStats.errorCountPlus();
                                } else {
                                 if (maps!=null){
                                     if (DBApiList.get(queue).isComplementary()){
                                         setComplementaryMap(maps, _DBCore);
                                    }
                                    DBApiList.get(queue).setDiscoveryMap(maps);
                                    //System.out.println(maps);
                                  } //end maaps!=null
                               } // 
                               
                            }
                              
                                
                            DBApiList.get(queue).setReadyToSendToTrue();
                            WorkingStats.okCountPlus();
                            //}  
                            break;    
                            
                        case DISCOVERY_QUERY:
                            if (!DBApiList.get(queue).isDiscoveryMapFromFile()){
                            
                                List<List<String>> discovery = _DBCore.getDiscoveryMap(queue, sql_query, true);

                                if (_DBCore.isError()){
                                    DBApiList.get(queue).setDiscoveryMapAndMetaData(new ArrayList<>());
                                    DBApiList.get(queue).setReadyToSendToFalse();
                                    DBApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                } else {
                                    DBApiList.get(queue).setDiscoveryMapAndMetaData(discovery);
                                    DBApiList.get(queue).setReadyToSendToTrue();
                                } 
                            } else {
                                DBApiList.get(queue).setReadyToSendToTrue();
                            }
                            break;
                        case DISCOVERY_STATIC:
                            DBApiList.get(queue).setDiscoveryMapAndMetaData(DBApiList.get(queue).getSqlQuery()); 
                            DBApiList.get(queue).setReadyToSendToTrue();
                            break;
                        default:
                            value  = _DBCore.doAndReturnData(queue);
                            WorkingStats.testsCountPlus();
                            if (_DBCore.isError()){
                                logger.error("DB core returned error." + DBApiList.get(queue).getAlias());
                                DBApiList.get(queue).addErrorReturnValue(_DBCore.getErrorDescription());
                                DBApiList.get(queue).setReadyToSendToTrue(); //done with errors
                                WorkingStats.errorCountPlus();
                            } else {
                                DBApiList.get(queue).addNormalReturnValue(value);
                                DBApiList.get(queue).setReadyToSendToTrue();
                                WorkingStats.okCountPlus();
                            }   break;
                    }

                    //dla wszystkich
                    DBApiList.get(queue).setIsNowExecutingToFalse();
                    stillExecuting=false;

                    //Runtime.getRuntime().gc();
                }


        }  catch (Exception e) {
            logger.error("--- Error in job " + queue + "|"+ DBApiList.get(queue).getAlias() + "! ----");
            
            DBApiList.get(queue).setIsNowRefreshingToFalse();
            DBApiList.get(queue).setIsNowExecutingToFalse();

            Thread.currentThread().interrupt();

            JobExecutionException e2 = new JobExecutionException(e);
            // this job will refire immediately
            e2.refireImmediately();
            throw e2;  
      } finally {
              if (!stillExecuting){
                  DBApiList.get(queue).setIsNowExecutingToFalse();
                  DBApiList.get(queue).setIsNowRefreshingToFalse();
                  if (!DBApiList.get(queue).isSendToZbx()) DBApiList.get(queue).setReadyToSendToFalse();
              } else {
                      WorkingStats.idlePlus();
              }
              

             if (isJobInterrupted) {
                logger.warn("Job " + jobKey + " did not complete");
             } else {
                logger.debug("Job " + jobKey + " completed at " + new Date());
             }
        } 
    }

    
    private void preparePureWithReverseMultiple(DBCore _DBCore, String sql_query){
         
           List<List<String>> maps_start = null;                               
           List<List<String>> maps_end = null;  
           
           if (DBApiList.get(queue).hasLatestValue()){
                //System.out.println("get latestdiscoverymap " + DBApiList.get(queue).getAlias());
              
                maps_start = new ArrayList<>();
                for ( List<String> b : DBApiList.get(queue).getLatestDiscoveryMap()){
                   maps_start.add(new ArrayList<>(b));
                }
                

               if (DBApiList.get(queue).isComplementary()){
                     if (maps_start!=null) setComplementaryMap(maps_start, _DBCore);
               }

                                                                           
               maps_end = _DBCore.getDiscoveryMap(queue, sql_query, false);  
               long curr_timestamp=System.currentTimeMillis();
               long latest_timestamp=DBApiList.get(queue).getLatestValueTimestamp();

               if (_DBCore.isError()){
                    logger.error("DB core returned error. (maps_end) " + DBApiList.get(queue).getAlias()); 
                    DBApiList.get(queue).setReadyToSendToFalse(); //done with errors
                    WorkingStats.errorCountPlus();
               } else {
                    logger.debug("set latestdiscoverymap " + DBApiList.get(queue).getAlias() + "|" + curr_timestamp);
                    DBApiList.get(queue).setLatestDiscoveryMap(maps_end, curr_timestamp);

                    if (DBApiList.get(queue).isComplementary()){
                         if (maps_end!=null) setComplementaryMap(maps_end, _DBCore);
                    }
               }  
               
                               
               double diff_sec = (curr_timestamp - latest_timestamp)/1000.0;
               //System.out.println( diff_sec);
               if (diff_sec<=0.0) {
                   logger.error("preparePureWithReverseMultiple::diff_sec<=0.0");
                   diff_sec=1.0;
               }
               double d = 0.0;
               
                      
               if (maps_start.size()!=maps_end.size()){
                      logger.error("preparePureWithReverseMultiple e1 maps_start!=maps_end | " + maps_start.size() + "|" + maps_end.size() );
               } else {
                   double diff=0.0;

                   //kolumna z "paramsColumn" nie będzie odejmowana
                   String [] pcolumns = DBApiList.get(queue).getDatabaseProperty("paramsColumn", "column").toUpperCase().split(",");
                   String [] div_by_sample_time = DBApiList.get(queue).getDatabaseProperty("div_by_sample_time", "true").toLowerCase().split(",");
                   String [] diff_extended = DBApiList.get(queue).getDatabaseProperty("difference_extended", "true").toLowerCase().split(",");
                  
                   int cols = checkParmsColumnInt(maps_start.get(0), pcolumns);

                  // System.out.println("cols " + cols);

                   if (div_by_sample_time.length>1){
                       if (maps_start.get(0).size()-cols!=div_by_sample_time.length){
                           logger.error("maps_start.size()-pcolumns.length!=div_by_sample_time.length ->>" + maps_start.get(0).size()+" "+pcolumns.length +" "+div_by_sample_time.length);
                           logger.error(DBApiList.get(queue));
                          // logger.error(maps_start);
                           //logger.error(pcolumns);
                       }
                   }
                   
                   if (diff_extended.length>1){
                       if (maps_start.get(0).size()-cols!=diff_extended.length){
                           logger.error("ERROR IN::"+DBApiList.get(queue).getAlias());
                           logger.error("maps_start.size()-pcolumns.length!=diff_extended.length ->>" + maps_start.get(0).size()+" "+pcolumns.length +" "+diff_extended.length);
                      
                       }
                   }
                   
                   if (div_by_sample_time.length==1){
                       if (!DBApiList.get(queue).getDatabaseProperty("div_by_sample_time", "true").equals("true")){
                           diff_sec=1;
                       }
                   }


                   try {
                       for (int i=1; i<maps_start.size(); i++){ //skip metadata
                                int idx=0;
                                int indx_ext=0;
                                for (int j=0; j<maps_start.get(i).size(); j++){ 
                                    if (!checkParmsColumn(maps_start.get(0).get(j), pcolumns)){
                                        if (maps_start.get(i).get(j).contains("null")) maps_start.get(i).set(j, "0");
                                        if (maps_end.get(i).get(j).contains("null")) maps_end.get(i).set(j, "0");
                                        
                                       // System.out.println(maps_start.get(0).get(j) + "|" +div_by_sample_time[idx]);

                                        if (DBApiList.get(queue).isInteger() || DBApiList.get(queue).isLong()){
                                            diff = Long.parseLong(maps_end.get(i).get(j))-Long.parseLong(maps_start.get(i).get(j));
                                        }
                                        else if (DBApiList.get(queue).isFloat() ) {
                                            diff = Double.parseDouble(maps_end.get(i).get(j))-1.0*Double.parseDouble(maps_start.get(i).get(j));
                                        }
                                        
                                        //System.out.println(diff + " | " + maps_end.get(i).get(j)+ " | " + maps_start.get(i).get(j));

                                        if (div_by_sample_time[idx].equals("true")) d = diff / diff_sec;
                                        else  d = diff;
                                        
                                        if (d<0){
                                            logger.error("ERROR IN diff_calc::preparePureWithReverseMultiple " + d);
                                        }
                                        
                                        logger.debug("Diff calc div_by_sample_time " + div_by_sample_time[idx] + " " + maps_start.get(0).get(j) + " diff_sec " + diff_sec);

                                       
                                        if (diff_extended.length>0){
                                            if ( diff_extended[indx_ext].equals("false")) {
                                                   maps_end.get(i).set(j, maps_start.get(i).get(j));
                                            } else {
                                                  maps_end.get(i).set(j, getValidFormat(div_by_sample_time[idx],d));
                                            }
                                        } else {
                                             maps_end.get(i).set(j, getValidFormat(div_by_sample_time[idx],d));
                                        }
                                       
                                        if (div_by_sample_time.length>idx+1) idx++; //bezpiecznik
                                        if (diff_extended.length>indx_ext+1) indx_ext++;
                                       
                                    } else {
                                        maps_end.get(i).set(j, maps_start.get(i).get(j));
                                    }                                                 
                                }
                            }

                   } catch (Exception e){
                        logger.error("ERROR xC266" + e);   
                   }

                   if (maps_end!=null) DBApiList.get(queue).setDiscoveryMapAndMetaData(maps_end);
                   maps_start.clear();
                   maps_start = null;

               }
           } else { 
               logger.debug("First run diff for " + DBApiList.get(queue).getAlias());
               long timestamp=System.currentTimeMillis();
               maps_start  = _DBCore.getDiscoveryMap(queue, sql_query, false); 
               if (_DBCore.isError()){
                    logger.error("DB core returned error. (maps_start) " + DBApiList.get(queue).getAlias());
                    DBApiList.get(queue).setReadyToSendToFalse(); //done with errors
                    WorkingStats.errorCountPlus();
               } else {
                    DBApiList.get(queue).setLatestDiscoveryMap(maps_start, timestamp); //latest value true auto
                    DBApiList.get(queue).setReadyToSendToFalse(); //done  
                    WorkingStats.okCountPlus();
               }

           }
     }
     
     
    private String getValidFormat(String div_by_sample_time, double diff){
            String ret="-1";
            if (div_by_sample_time.equals("true")){
                  ret = Double.toString(diff);
            } else {
                if (DBApiList.get(queue).isInteger() || DBApiList.get(queue).isLong()){
                    int int_diff = (int) diff;
                    ret=Integer.toString(int_diff);
                }
                else if (DBApiList.get(queue).isFloat() ) {
                    ret = Double.toString(diff);
                }
            }
            
            return ret;
    } 
    
    private void preparePureWithMultiple(DBCore _DBCore, String sql_query){
           List<List<String>> maps_start = null;                               
           List<List<String>> maps_end = null;  
           
           if (DBApiList.get(queue).hasLatestValue()){
               logger.debug("get latestdiscoverymap " + DBApiList.get(queue).getAlias());
              
                maps_start = new ArrayList<>();
                for ( List<String> b : DBApiList.get(queue).getLatestDiscoveryMap()){
                   maps_start.add(new ArrayList<>(b));
                }

               if (DBApiList.get(queue).isComplementary()){
                     if (maps_start!=null) setComplementaryMap(maps_start, _DBCore);
               }

                                                                           
               maps_end = _DBCore.getDiscoveryMap(queue, sql_query, true);  
               long curr_timestamp=System.currentTimeMillis();
               long latest_timestamp=DBApiList.get(queue).getLatestValueTimestamp();

               if (_DBCore.isError()){
                    logger.error("DB core returned error. (maps_end) " + DBApiList.get(queue).getAlias()); 
                    DBApiList.get(queue).setReadyToSendToFalse(); //done with errors
                    WorkingStats.errorCountPlus();
               } else {
                    logger.debug("set latestdiscoverymap " + DBApiList.get(queue).getAlias() + "|" + curr_timestamp);
                    DBApiList.get(queue).setLatestDiscoveryMap(maps_end, curr_timestamp);

                    if (DBApiList.get(queue).isComplementary()){
                         if (maps_end!=null) setComplementaryMap(maps_end, _DBCore);
                    }
               }  
                        
               double diff_sec = (curr_timestamp - latest_timestamp)/1000.0;
               if (diff_sec<=0.0) {
                   logger.error("preparePureWithMultiple::diff_sec<=0.0");
                   diff_sec=1.0;
               }
               double d = 0.0;

              if (maps_start.size()!=maps_end.size()){
                     logger.error("preparePureWithMultiple e1 maps_start!=maps_end | " + maps_start.size() + "|" + maps_end.size() );
               } else {
                       DBApiList.get(queue).setLatestDiscoveryMap(maps_end, queue);
                       double diff=0.0;
                       if (!DBApiList.get(queue).getDatabaseProperty("div_by_sample_time", "true").equals("true")){
                           diff_sec=1;
                       }
                    
                       try {
                           if (DBApiList.get(queue).isInteger() || DBApiList.get(queue).isLong()){

                                for (int i=1; i<maps_start.size(); i++){ //skip metadata
                                   for (int j=0; j<maps_start.get(i).size(); j++){ 
                                        if ( maps_start.get(0).get(j).toLowerCase().startsWith("value") ){
                                            diff = Long.parseLong(maps_end.get(i).get(j))-Long.parseLong(maps_start.get(i).get(j));
                                            d = diff / diff_sec; 
                                            maps_end.get(i).set(j, getValidFormat(DBApiList.get(queue).getDatabaseProperty("div_by_sample_time", "true"),d));
                                        } 

                                    }
                                }
                           } else if (DBApiList.get(queue).isFloat()){
                                 for (int i=1; i<maps_start.size(); i++){ //skip metadata
                                    for (int j=0; j<maps_start.get(i).size(); j++){ 
                                        if ( maps_start.get(0).get(j).toLowerCase().startsWith("value") ){
                                            diff = Double.parseDouble(maps_end.get(i).get(j))-1.0*Double.parseDouble(maps_start.get(i).get(j));
                                            d = diff / diff_sec;
                                            maps_end.get(i).set(j, getValidFormat(DBApiList.get(queue).getDatabaseProperty("div_by_sample_time", "true"),d));
                                        } 
                                    }
                                }
                           }  
                       } catch (Exception e){
                            logger.error("ERROR xC266" + e);   
                       }

                       if (maps_end!=null){
                           DBApiList.get(queue).setDiscoveryMapAndMetaData(maps_end);
                       }


                       maps_start.clear();
                       maps_start = null;

                   }

           } else { 
               logger.debug("First run diff for " + DBApiList.get(queue).getAlias());
               long timestamp=System.currentTimeMillis();
               maps_start  = _DBCore.getDiscoveryMap(queue, sql_query, true); 
               if (_DBCore.isError()){
                    logger.error("DB core returned error. (maps_start) " + DBApiList.get(queue).getAlias());
                    DBApiList.get(queue).setReadyToSendToFalse(); //done with errors
                    WorkingStats.errorCountPlus();
               } else {
                    DBApiList.get(queue).setLatestDiscoveryMap(maps_start, timestamp); //latest value true auto
                    DBApiList.get(queue).setReadyToSendToFalse(); //done  
                    WorkingStats.okCountPlus();
               }

           }
    }
    
    private void setComplementaryMap(List<List<String>> maps, DBCore _DBCore){
        if (DBApiList.get(queue).mustComplementaryNow()) {

           logger.info("** DISCOVERY Complementary for queue *** " + queue);                                                

           logger.info("Complementary " + DBApiList.get(queue).getComplementary_query());

            List<List<String>> complement  = _DBCore.getDiscoveryMap(queue, DBApiList.get(queue).getComplementary_query(),true);
                     
            DBApiList.get(queue).setComplementaryMap(complement);

        }   else  if (DBApiList.get(queue).getComplementaryMap().isEmpty()){
                if (DBApiList.get(queue).isComplementaryMapFromFile()){
                    logger.error("Empty complementary read from file");
                } else {
                    logger.info("Complementary is empty ");
                    List<List<String>> complement  = _DBCore.getDiscoveryMap(queue, DBApiList.get(queue).getComplementary_query(), true);
                    DBApiList.get(queue).setComplementaryMap(complement);
                }
        } 

        if (DBApiList.get(queue).getComplementaryMap()==null){
            logger.error("DBApiList.get(queue).getComplementMap() is null");
        } else if (!maps.isEmpty()) {

            int valcol=-1;

            for (int j=0; j<maps.get(0).size(); j++){ //get metadata
                  if ( maps.get(0).get(j).equalsIgnoreCase("VALUE")){
                     valcol=j;
                     break;
                  }
            }

           if (valcol<0){
               //STATS
               logger.debug("COMPLEMENTARY: valcol not exist for "  + DBApiList.get(queue).getSqlQuery());
               String [] pcolumns = DBApiList.get(queue).getDatabaseProperty("paramsColumn", "column").toUpperCase().split(",");
               
               if (DBApiList.get(queue).getComplementaryMap().get(0).size()!=maps.get(0).size()
                       && DBApiList.get(queue).getDiscoveryMetaData().size()!=maps.get(0).size()){
                   logger.error("Error x56 complementary.get(0).size()!=maps.get(0).size() "
                           + "&& getDiscoveryMetaData().size()!=maps.get(0).size()");
                   logger.error("Size: ComplementaryMap" + DBApiList.get(queue).getComplementaryMap().get(0).size());
                   logger.error("Size: maps" + maps.get(0).size());
                   logger.error("Size: DiscoveryMetaData" + DBApiList.get(queue).getDiscoveryMetaData().size());
               } else {
                       try {
                          for (int c=1; c < DBApiList.get(queue).getComplementaryMap().size();c++){ //0==metadata 
                            boolean bfound=false;
                            for (int j=0; j < maps.size();j++){ 
                               int found=0;
                               for  (int k=0; k< maps.get(j).size(); k++){ //kolumny
                                   if (checkParmsColumn(DBApiList.get(queue).getDiscoveryMetaData().get(k), pcolumns)){ //kolumna ktora jest stats
                                       if (maps.get(j).get(k).equals(DBApiList.get(queue).getComplementaryMap().get(c).get(k))) found++;
                                   } 
                               }
                               if (found==pcolumns.length){
                                     bfound=true;
                               } 
                            }
                            if (!bfound){
                                logger.debug("Add complementary " + DBApiList.get(queue).getComplementaryMap().get(c).toString());
                                maps.add(DBApiList.get(queue).getComplementaryMap().get(c));
                            }
                        }
                      } catch (Exception unx){
                          logger.error("Niespodziewany blad w komplementowaniu " + unx);
                      }

                }
           
           } else {
                if (DBApiList.get(queue).getComplementaryMap().get(0).size()!=maps.get(0).size()){
                   logger.error("Error x55 complementary.get(0).size()!=maps.get(0).size()");
                } else {
                        for (int c=1; c < DBApiList.get(queue).getComplementaryMap().size();c++){ //0==metadata 
                            boolean bfound=false;
                            for (int j=1; j < maps.size();j++){ //0==metadata 
                               int found=0;
                               for  (int k=0; k< maps.get(j).size(); k++){ //kolumny
                                   if (k!=valcol){
                                       //logger.info("compare " + maps.get(j).get(k));
                                       //logger.info(sybaseApiList.get(queue).getComplementaryMap().get(c).get(k));
                                       if (maps.get(j).get(k).equals(DBApiList.get(queue).getComplementaryMap().get(c).get(k))) found++;
                                   }
                               }
                                if (found==DBApiList.get(queue).getComplementaryMap().get(c).size()-1){
                                     bfound=true;
                                } 
                            }
                            if (!bfound){
                                logger.debug("Add complementary " + DBApiList.get(queue).getComplementaryMap().get(c).toString());
                                maps.add(DBApiList.get(queue).getComplementaryMap().get(c));
                            }
                        }

                }
               
           }
                
         } else {
              logger.warn("COMPLEMENTARY maps.isEmpty() for "  + DBApiList.get(queue).getZabbixKey());
              for (int c=1; c < DBApiList.get(queue).getComplementaryMap().size();c++){ //0==metadata 
                    boolean bfound=false;
                    for (int j=1; j < maps.size();j++){ //0==metadata 
                       int found=0;
                       for  (int k=0; k< maps.get(j).size(); k++){ //kolumny

                               //logger.info("compare " + maps.get(j).get(k));
                               //logger.info(sybaseApiList.get(queue).getComplementaryMap().get(c).get(k));
                               if (maps.get(j).get(k).equals(DBApiList.get(queue).getComplementaryMap().get(c).get(k))) found++;

                       }
                       if (found==DBApiList.get(queue).getComplementaryMap().get(c).size()-1){
                             bfound=true;
                       } 
                    }
                    if (!bfound){
                        logger.debug("Add complementary " + DBApiList.get(queue).getComplementaryMap().get(c).toString());
                        maps.add(DBApiList.get(queue).getComplementaryMap().get(c));
                    }
                }

            
            
         }
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
    
    private int checkParmsColumnInt(List<String> list , String [] params){
        int ile=0; 
        
        for (String s1 : list){
             for (String s2:params){
                if (s2.equalsIgnoreCase(s1)) {
                    ile++;
                }
            }
        }
        
      
        return ile;
    }
        
        
     @Override
     // this method is called by the scheduler
     public void interrupt() throws UnableToInterruptJobException {
        logger.info("Job " + jobKey + "  -- INTERRUPTING --");
        isJobInterrupted = true;
        if (thisThread != null) {
          // this call causes the ClosedByInterruptException to happen
          thisThread.interrupt(); 
        }
      }
 
}