/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.timers;

import static biz.szydlowski.db4zabbix.WorkingObjects.JdbcDriverApiList;
import biz.szydlowski.db4zabbix.api.DataSource;
import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.TimerTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author DKBU
 */
public class DataSourceTask extends TimerTask  {
    
        static final Logger logger = LogManager.getLogger(DataSourceTask.class);

      

        @Override
        public void run() {
            for (JdbcDriverApi _JdbcApi : JdbcDriverApiList){   
                if (_JdbcApi.getInterfaceType().equals("pool")){
                    if (_JdbcApi.getExtendedOption().contains("pg_database")){
                        logger.info("refresh pool id=" + _JdbcApi.getPoolIndex() + " " + _JdbcApi.getInterfaceName() + " - " + _JdbcApi.getHost() + " : " +  _JdbcApi.getStringPort());
                        try {
                           
                           if (DataSource.getInstances(_JdbcApi.getPoolIndex()).isEmpty()){
                               logger.info("!!!!!!REFRESH PG_DATABASE!!!!!!!");
                               DataSource.retry(_JdbcApi);
                           }


                        } catch (SQLException | IOException | PropertyVetoException e) {
                            logger.error(e);
                        }
                    }
                }
            }
            
        }
   
}