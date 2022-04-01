/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.api;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author szydlowskidom
 */
public class DataSource {
  
    private static  List<List<DataSource>>  datasources = new ArrayList<>();
    
        
    private ComboPooledDataSource cpds;
    static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.api.DataSource");
       
    private DataSource(JdbcDriverApi jdbc) {
        try { 
            cpds = new ComboPooledDataSource();
        
            cpds.setDriverClass(jdbc.getClassName()); //loads the jdbc driver
            cpds.setJdbcUrl(jdbc.getUrl());
            cpds.setUser(jdbc.getUser());
            cpds.setPassword(jdbc.getPassword());
            cpds.setLoginTimeout(jdbc.getLoginTimeout());
           // cpds.setIdleConnectionTestPeriod(3600);
            cpds.setDataSourceName(jdbc.getInterfaceName());

            // the settings below are optional -- c3p0 can work with defaults
            cpds.setMinPoolSize(jdbc.getMinPoolSize());
            cpds.setAcquireIncrement(jdbc.getAcquireIncrement());
            cpds.setMaxPoolSize(jdbc.getMaxPoolSize());
            cpds.setMaxStatements(jdbc.getMaxStatements());
            cpds.setMaxIdleTime(300);
            cpds.setInitialPoolSize(jdbc.getMinPoolSize());
            cpds.setAcquireRetryAttempts(2);
            
           // logger.debug("USER " + user);
           
            Properties prop = new Properties(); 
            prop.setProperty("USER", jdbc.getUser());
            prop.setProperty("PASSWORD", jdbc.getPassword());
            prop.setProperty("APPLICATIONNAME", "thread.pool.db");
           
            cpds.setProperties(prop);
            cpds.setUser( jdbc.getUser() );
            cpds.setPassword( jdbc.getPassword() );
          
                        
        } catch (Exception e){
            
        }

    }

    public static void addInstance(JdbcDriverApi jdbc) throws IOException, SQLException, PropertyVetoException {
        if (jdbc.getExtendedOption().equalsIgnoreCase("default")){
            List<DataSource> ds = new ArrayList<>(); 
            ds.add(new DataSource(jdbc));
            datasources.add(ds);
        } else if (jdbc.getExtendedOption().contains("pg_database")) {
            logger.info("ExtendedOption = pg_database");
            List<String> pg_database = getPgDatabase( jdbc);
            List<DataSource> ds = new ArrayList<>(); 
            if (!pg_database.isEmpty()) jdbc.cleanDatabase();
          
            for (String db : pg_database){  
                jdbc.generateUrl(db);
                
                logger.debug("ExtendedOption = pg_database, url=" + jdbc.getUrl());
                
                ds.add(new DataSource(jdbc));
                jdbc.addDatabase(db);
               
            }
                      
            datasources.add(ds);
            
        }
       
       
    } 
    
 
   
    public static void retry (JdbcDriverApi jdbc) throws IOException, SQLException, PropertyVetoException {
                 
         if (jdbc.getExtendedOption().equalsIgnoreCase("default")){
            List<DataSource> ds = new ArrayList<>(); 
            ds.add(new DataSource(jdbc));
            datasources.set(jdbc.getPoolIndex(), ds);
         } else if (jdbc.getExtendedOption().contains("pg_database")) {
            logger.info("ExtendedOption = pg_database");
            List<String> pg_database = getPgDatabase( jdbc);
            List<DataSource> ds = new ArrayList<>(); 
            
            if (!pg_database.isEmpty()) jdbc.cleanDatabase();
            
            for (String db : pg_database){  
                jdbc.generateUrl(db);
                
                logger.debug("ExtendedOption = pg_database, url=" + jdbc.getUrl());
                
                ds.add(new DataSource(jdbc));
                jdbc.addDatabase(db);
               
            }
            
            datasources.set(jdbc.getPoolIndex(), ds);
            
        }
       
       
    } 
    
    public static int getDataSourceSize(){
       return datasources.size();
    }
      
    public static DataSource getInstanceInPool(int i, int dbInPool)  {
        if (i< datasources.size()) {
           if (dbInPool < datasources.get(i).size()){
               return datasources.get(i).get(dbInPool);
           } else return null;
           
        } else return null;
      
    }
    
     public static List<DataSource> getInstances(int i)  {
        if (i< datasources.size()) {
           return datasources.get(i);
        } else return null;
      
    }

    public Connection getConnection() {
        try {
            return this.cpds.getConnection();
        } catch (Exception e){
            return null;
        }
    }   
    
    public void closePool() {
        try {
           this.cpds.close(); 
        } catch (Exception e){
        }
    }
    
      public static List<String> getPgDatabase(JdbcDriverApi jdbc){
         List<String> pg_database = new ArrayList<>();
         try {
            Class.forName(jdbc.getClassName());
            Connection connection= DriverManager.getConnection(jdbc.getUrl(),jdbc.getUser(),jdbc.getPassword());
            Statement statement=connection.createStatement();
            ResultSet result=statement.executeQuery(jdbc.getExtendedOption());
            while(result.next()){
                pg_database.add(result.getString(1));
            }
            connection.close();
        } catch(Exception ex){
            logger.error(ex);
        }
         return pg_database;
    }

}