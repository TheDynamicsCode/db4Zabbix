/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix;

import static biz.szydlowski.db4zabbix.DB4ZabbixDaemon.scheduler;
import static biz.szydlowski.db4zabbix.WorkingObjects.DBApiList;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 *
 * @author szydlowskidom
 */
public class DBQuartz {
    
    static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.DBQuartz");
  
    private boolean isDo = false;
 
    
    public DBQuartz(){
    }
     
    public void doJob( ){
      
        if (isDo){
            logger.fatal("DBQuartz is running");
            return;
        } else {
            isDo = true;
        }
           
        try {
                int queue = 0;
                int queue_active = 0;
                
                List<JobDetail> jobs = new ArrayList<>();
                List<Trigger> triggers = new ArrayList<>();
                logger.debug("Jobs " + DBApiList.size());                    
               
                for (queue=0; queue<DBApiList.size(); queue++){
                    
                    sqlType sql_type = DBApiList.get(queue).getSqlType();
                    
                     switch (sql_type) {
                         case DISCOVERY_QUERY: 
                         case DISCOVERY_STATIC:
                            logger.debug("Job " + DBApiList.get(queue).getAlias() + " is DISCOVERY_QUERY...");
                            logger.debug("Add job " +  DBApiList.get(queue).getAlias() + "." + queue);
                             DBApiList.get(queue).setReadyToSendToFalse();
                            jobs.add(JobBuilder.newJob(DBQuartzJob.class).withIdentity(DBApiList.get(queue).getAlias() + "." + queue, "Zabbix.Discovery.DB").build());
                            jobs.get(queue_active).getJobDataMap().put("queue", queue);
                            
                            queue_active++;
                            triggers.add(TriggerBuilder.newTrigger()
                                    .withIdentity("trigger." +  DBApiList.get(queue).getAlias()  + queue, "DB")
                                    .withSchedule(CronScheduleBuilder.cronSchedule( DBApiList.get(queue).getCronExpression()))
                                    .build());
                           
                            jobs.add(JobBuilder.newJob( DBQuartzJob.class).withIdentity( DBApiList.get(queue).getAlias() + ".now." + queue, "Zabbix.Discovery.DB").build());
                            jobs.get(queue_active).getJobDataMap().put("queue", queue);
                            
                            queue_active++;
                            Trigger runOnceTrigger = TriggerBuilder.newTrigger().withIdentity("trigger.now." +  DBApiList.get(queue).getAlias()  + queue).build();
                            triggers.add(runOnceTrigger);
                            break;
                            
                         default:
                            logger.debug("Add job " + DBApiList.get(queue).getAlias() + "." + queue);

                            DBApiList.get(queue).setReadyToSendToFalse();
                            DBApiList.get(queue).setIsNowExecutingToFalse();
                          
                            jobs.add(JobBuilder.newJob(DBQuartzJob.class).withIdentity(DBApiList.get(queue).getAlias() + "." + queue, "Zabbix.DB").build());
                            jobs.get(queue_active).getJobDataMap().put("queue", queue);

                            queue_active++;

                            triggers.add(TriggerBuilder.newTrigger()
                                .withIdentity("trigger." + DBApiList.get(queue).getAlias()  + queue, "DB")
                                .withSchedule(CronScheduleBuilder.cronSchedule(DBApiList.get(queue).getCronExpression()))
                                .build());
                            
                           if (DBApiList.get(queue).isFireAtStartup()){
                                jobs.add(JobBuilder.newJob( DBQuartzJob.class).withIdentity( DBApiList.get(queue).getAlias() + ".runAtStartup." + queue, "Zabbix.DB").build());
                                jobs.get(queue_active).getJobDataMap().put("queue", queue);

                                queue_active++;
                                Trigger runAtStartupTrigger = TriggerBuilder.newTrigger().withIdentity("runAtStartup.now." +  DBApiList.get(queue).getAlias()  + queue).build();
                                triggers.add(runAtStartupTrigger);
                           }
                    }
                     
                          
          
                } 
          
                 //schedule it
                 if (!scheduler.isStarted()){
                       logger.info("Start scheduler");
                       scheduler.start();
                 }  
                
                
                for (int ii=0; ii<jobs.size(); ii++){
                    scheduler.scheduleJob(jobs.get(ii), triggers.get(ii));
                }
    
            
        }
        catch(SchedulerException e){
           logger.error(e);
        }
        
    
    }
   
    public boolean stop(){

       isDo = false;
       
       try {
            
            if (scheduler.isStarted()){
                scheduler.clear();
                scheduler.shutdown();
            }           
            return true;
       }  catch(SchedulerException e){
            logger.error(e);
            return false;
       }
    } 
    
    public String refreshTask(int _queue_task){
      
        if (isDo){
            
            if (!DBApiList.get(_queue_task).isActiveMode()){
               return "The task " + DBApiList.get(_queue_task).getAlias()  + " is in inactive mode, you cannot refresh it.";
            }
       
            if  ( ! DBApiList.get(_queue_task).isNowRefreshing() ){
                
                if  ( DBApiList.get(_queue_task).isNowExecuting() ){
                   return "The task " + DBApiList.get(_queue_task).getAlias()  + " is executing now, you cannot refresh it.";
                } 
                
                
                // if you don't call startAt() then the current time (immediately) is assumed.
                Trigger runOnceTrigger = TriggerBuilder.newTrigger().withIdentity("SYBASE.trigger.now."+ _queue_task, "DB").build();
                JobDetail job = JobBuilder.newJob(DBQuartzJob.class)
                        .withIdentity("refresh."+DBApiList.get(_queue_task).getAlias()+"."+_queue_task, "DB").build();
               
                job.getJobDataMap().put("queue", _queue_task);
                DBApiList.get(_queue_task).setIsNowRefreshing();
               
                try {
                    scheduler.scheduleJob(job, runOnceTrigger);
                    return "The task " + DBApiList.get(_queue_task).getAlias() + " was scheduled.";
                } catch (SchedulerException e){
                    logger.error(e);
                    return e.getMessage();
                }             

            } else {
                return  "The task " + DBApiList.get(_queue_task).getAlias()  + " is refreshing now, you cannot refresh it again.";  
               
            }
        } else {
            return "The task is not monitoring.";
        }
   
    } 
}
