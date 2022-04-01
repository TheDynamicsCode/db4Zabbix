/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix;


import static biz.szydlowski.db4zabbix.WorkingObjects.DBApiList;
import static biz.szydlowski.db4zabbix.WorkingObjects.JdbcDriverApiList;
import biz.szydlowski.db4zabbix.api.DataSource;
import biz.szydlowski.db4zabbix.api.JdbcDriverApi;
import biz.szydlowski.utils.WorkingStats;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/*
select db_name(dbid) from master..sysusages group by dbid
*/
/**
 *
 * @author szydlowskidom
 */
public class DBCore {
    
    static final Logger logger = LogManager.getLogger("biz.szydlowski.db4zabbix.DBCore"); 
    String newline = System.getProperty("line.separator");
    public static final String DB_VALIDATION_QUERY = "SELECT 1";
    
    private int __queue=0;
    private boolean isError=false; 
    private String errorDescr="";
          
    Connection conn = null;
    List<Connection> connections = new ArrayList<>();

    public String doAndReturnData(int _queue){
        
       
        this.__queue=_queue;
        
        if (_queue>DBApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return "error";
        }
        
        String s_return="default";
        isError=false;      
  
                
       int check=0; 
    
        if (null!=DBApiList.get(__queue).getSqlType())switch (DBApiList.get(__queue).getSqlType()) {
              case MAINTENANCE:
                switch (DBApiList.get(__queue).getSqlQuery()) {
                   
                    case "connError":
                        s_return = ""+WorkingStats.getConnError();
                        break;
                    
                    case "activeThreads":
                        s_return = ""+WorkingStats.getActiveThreads();
                        break; 
                    
                    case "javaUsedMemory":
                        s_return = ""+WorkingStats.getJavaUsedMemory();
                        break;
                        
                    case "okCount":
                        s_return = ""+WorkingStats.getOkCount();
                        break;
                        
                    case "errorCount":
                        s_return = ""+WorkingStats.getErrorCount();
                        break;
                        
                    case "testsCount":
                        s_return = ""+WorkingStats.getTestsCount();
                        break;
                        
                    case "uptime":
                        s_return = ""+WorkingStats.getUptime();
                        break; 
                    
                    case "idletime":
                        s_return = ""+WorkingStats.getIdleTime();
                        break;
                    
                    case "lockCount":
                        s_return = ""+WorkingStats.getLockCount();
                        break; 
                    
                    case "lockAttempt":
                        s_return = ""+WorkingStats.getLockAttempt();
                        break;
                        
                    case "zabbixProcessed":
                        s_return = ""+WorkingStats.getZabbix_processed();
                        break;
                        
                    case "zabbixFailed":
                        s_return = ""+WorkingStats.getZabbix_failed();
                        break;
                        
                    case "zabbixTotal":
                        s_return = ""+WorkingStats.getZabbix_total();
                        break;  
                    
                    case "autorestart":
                        s_return = ""+WorkingStats.getAutorestartCount();
                        break; 
                        
                    case "version":
                        s_return = ""+Version.getAgentVersion();
                        break;
                        
                    default:
                        logger.fatal("MAINTENANCE " + DBApiList.get(__queue).getSqlQuery());
                        s_return  = "-210";
                        isError=true;
                        errorDescr = "MAINTENANCE UNKNOWN FUNCTION";
                }   break;
        // koniec wbudowanego
            case EMBEDDED:
                switch (DBApiList.get(__queue).getSqlQuery()) {
                    
                    case "connection":
                        check = checkDBConn() ;
                        if (check==200) s_return = "CONN";
                        if (check==-200) s_return = "NO_CONN";
                        break;
                        
                    case "connection_numeric":
                        check = checkDBConn();
                        if (check==200) s_return = "1";
                        if (check==-200) s_return = "0";
                        break;
                        
                   case "connection_extended":
                        s_return = checkDBConnections();
                        attemptCloseConnections();
                        break; 
                        
                        
                    case "connection_time":
                        long startTime = System.currentTimeMillis();
                        checkDBConn();
                        long endTime = System.currentTimeMillis() - startTime;
                        s_return  = "" + endTime;
                        break;
                        
                        
                        
                    default:
                        isError=true;
                        errorDescr = "Unknown Embedded function";
                        s_return  = "-215";
                        
                }
                
                break;   
            case PREDEFINED:
            case PURE:
                              
                conn = getConnectionRetry(0);
                 
                if (conn==null){
                    isError = true;
                    errorDescr = "Could not create connection to database server";
                    return "0";
                }  if (DBApiList.get(__queue).isTiming()){                    
                    long startTime = System.currentTimeMillis();
                    doQuery(DBApiList.get(__queue).getSqlQuery());
                    long endTime = System.currentTimeMillis() - startTime;
                    if (logger.isDebugEnabled()) logger.debug("timing time " + endTime);
                    s_return  = "" + endTime;
                } else  if (DBApiList.get(__queue).isFullQuery()){   
                    //moze byc data lub nodata
                     s_return = doQuery(DBApiList.get(__queue).getSqlQuery());
                }  else if (DBApiList.get(__queue).isInteger() || DBApiList.get(__queue).isFloat() || DBApiList.get(__queue).isLong()){
                    if (DBApiList.get(__queue).getDatabaseProperty("difference", "false").equals("true")){
                       
                       int sample_interval=1;
                      
                       try {
                           sample_interval = Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("sample_interval", "5"));
                       } catch (Exception e){}
                       
                       if (sample_interval<=0) sample_interval=5;
                       
                      
                       String s_return1 = doNumericQuery(DBApiList.get(__queue).getSqlQuery());
                       long curr_uptime = System.currentTimeMillis();
                        
                       try {
                            logger.debug("Wait sample sec " + sample_interval);
                            Thread.sleep(1000*sample_interval);
                        } catch (InterruptedException ex) {
                            logger.error("e1" + ex);
                        }
        
                       String s_return2 = doNumericQuery(DBApiList.get(__queue).getSqlQuery());
                       double diff_sec = (System.currentTimeMillis() - curr_uptime)/1000.0;
                      
                       if (diff_sec<=0.0) diff_sec=1.0;
                         
                       double diff=0.0;
                       try {
                           if (DBApiList.get(__queue).isInteger()){
                                diff = 1.0*Long.parseLong(s_return2)-1.0*Long.parseLong(s_return1);
                           } else if (DBApiList.get(__queue).isFloat()){
                                diff = Double.parseDouble(s_return2)-Double.parseDouble(s_return1);
                           }  else if (DBApiList.get(__queue).isLong()){
                                diff = 1.0*Long.parseLong(s_return2)-1.0*Long.parseLong(s_return1);
                           } 
                       } catch (Exception e){
                            logger.error("ERROR xC22" + e);   
                       }
                       
                       if (DBApiList.get(__queue).getDatabaseProperty("div_by_sample_time", "true").equals("true")){
                           double d = diff / diff_sec;
                           s_return = ""+d;
                       } else {
                           s_return = ""+diff;
                       } 
                                  
                       
                    } else {
                       s_return = doNumericQuery(DBApiList.get(__queue).getSqlQuery());
                    }                   
                    
                }  else {
                   // System.out.println("doStringQuery()");
                    s_return = doStringQuery(DBApiList.get(__queue).getSqlQuery());
                }   
                attemptClose(conn) ;
                
                break;
            case COMPARE:
                s_return = doQueryCompare();
                break;
            default:
                break;
        }
       
        return  s_return;
         
    }
    
    public String getErrorDescription(){
        return errorDescr;
    }
   
   private String checkDBConnections(){
       
         connections = getConnectionsRetry(0); 
         StringBuilder sb = new StringBuilder();
         int ile_ok=0;
         int ile_total=1;
         int jd=0;

         if (connections.isEmpty()){
             return "ERROR - FAILED CONNECTIONS TO DATABASES";
             // tu nie ma byc isError
         } else {
             
         
                   
               for (int i=0; i<JdbcDriverApiList.size(); i++){
                  if (DBApiList.get(__queue).getJdbc_driver(0).getInterfaceName().equals(JdbcDriverApiList.get(i).getInterfaceName())){
                        jd=i;
                        break;                         
                  }
               }

 
     
               int j=0;
               ile_total=connections.size();
               for (Connection conns : connections){
                   
                   if (conns==null){
                        sb.append("CONN ERROR ");
                        if (JdbcDriverApiList.get(jd).getDatabase().size()>j){
                            sb.append(JdbcDriverApiList.get(jd).getDatabase().get(j));
                        }
                        sb.append("\n");
                   } else {

                       try {

                                Statement stmt = conns.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                                ResultSet rs = stmt.executeQuery(DB_VALIDATION_QUERY);


                                while (rs.next())  {
                                    rs.getString(1); 
                                }

                                attemptClose(rs);
                                attemptClose(stmt);
                                attemptClose(conns);
                                ile_ok++;

                        } 
                        catch (SQLException e) {
                               logger.error("SQL Exception/Error:");
                               logger.error("error message=" + e.getMessage());
                               logger.error("SQL State= " + e.getSQLState());
                               logger.error("Vendor Error Code= " + e.getErrorCode());
                               logger.error("SQL Exception/Error: ", e);
                               sb.append("CONN ERROR ").append(" ");
                               if (JdbcDriverApiList.get(jd).getDatabase().size()>j){
                                    sb.append(JdbcDriverApiList.get(jd).getDatabase().get(j));
                               }
                               sb.append(" ").append(e.getMessage()).append("\n");                                
                        }
                  
                  }
                 
                  j++; 
               }
               
               if (ile_ok==ile_total) sb.append("OK");
               return sb.toString();
         }
                  
         
    }
    
    private int checkDBConn(){
      
         conn = getConnectionRetry(0); 

         if (conn==null){
             return -200;
             // tu nie ma byc isError
         } else {
            
                   try {

                            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                            ResultSet rs = stmt.executeQuery(DB_VALIDATION_QUERY);


                            while (rs.next())  {
                                rs.getString(1); 
                            }

                            attemptClose(rs);
                            attemptClose(stmt);

                    } 
                    catch (SQLException e) {
                           logger.error("SQL Exception/Error:");
                           logger.error("error message=" + e.getMessage());
                           logger.error("SQL State= " + e.getSQLState());
                           logger.error("Vendor Error Code= " + e.getErrorCode());
                           logger.error("SQL Exception/Error: ", e);
                           return -200;
                    } 
             attemptClose(conn);
             return 200;
         }
                  
         
    }
    
    
    private int getPgDatabaseID(int indx){
       int ret=0;
       int i=0;
     
       if (!DBApiList.get(__queue).getPg_database().equals("default")){
           for (JdbcDriverApi _JdbcApi : JdbcDriverApiList){
              if (DBApiList.get(__queue).getJdbc_driver(indx).getInterfaceName().equals(_JdbcApi.getInterfaceName())){
                  
                    for (i=0; i<_JdbcApi.getDatabase().size(); i++){
                         //System.out.println(_JdbcApi.getDatabase().get(i));
                         if (_JdbcApi.getDatabase().get(i).equalsIgnoreCase(DBApiList.get(__queue).getPg_database())){
                            ret=i;
                         }               
                    }
              }
           }
       }
       
       return ret;
    }
    
   private boolean isPgDatabase(){
      
       if (DBApiList.get(__queue).getPg_database().equals("default")){
          return false;
       } else return true;
       
    }
     

       
     protected boolean checkForTSQLPrint(SQLException ex)  {
         boolean returnVal = false;
         if ((ex.getErrorCode() == 0) && (ex.getSQLState() == null))
         {
           returnVal = true;

         }
         return returnVal;
     }
        
    protected String printWarnings(SQLException ex){
         StringBuilder retString=new StringBuilder();

         while (ex != null)
         {
           if (checkForTSQLPrint(ex))
           {
               retString.append(ex.getMessage());
               retString.append(newline);
               ex = ex.getNextException();
           } else {
               retString.append(ex.getMessage());
               retString.append(newline);
               ex = ex.getNextException();
           }
         }

         return retString.toString();
             
    }
    
       
   private String doNumericQuery(String _query){
        
        String ret="-299";       
      
        
        /*if (connections.isEmpty()){
            logger.error("Could not create connection to database server doNumericQuery");
            isError=true;
            errorDescr = "Could not create connection to database server doNumericQuery";
            return "-200";
        }
        
        int id=0;
        id = getPgDatabaseID(0);*/
        
        if (conn==null){
            logger.error("Could not create connection to database server doNumericQuery");
            isError=true;
            errorDescr = "Could not create connection to database server doNumericQuery";
            return "-200";
        }
        
      
        try {
                if (DBApiList.get(__queue).getIsolationLevel()==-1) {
                    //conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                } else {
                    conn.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                }
                        
                if (!DBApiList.get(__queue).getDatabaseProperty("use_database", "no_use").equalsIgnoreCase("no_use")){
                    conn.setCatalog(DBApiList.get(__queue).getDatabaseProperty("use_database", "no_use"));
                }
           
               
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                 
                int updates = 0;
                SQLWarning sqlW = null;  
                             
                int more_stmt_count = 1;
                
                isResult = stmt.execute(_query);
                if (logger.isDebugEnabled()) logger.debug("Execute query " + DBApiList.get(__queue).getSqlQuery());
                
                do {
                  
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                          if (DBApiList.get(__queue).getMoreStmt()==more_stmt_count) ret = "-299";
                          stmt.clearWarnings();
                    } 
                 
                   if (isResult) {
                     
                          rs = stmt.getResultSet();
                          rsmd = rs.getMetaData();
                          int column = DBApiList.get(__queue).getColumn();


                          int numCol = rsmd.getColumnCount();
                          if (column > numCol){
                                logger.warn("**** col > colcount ****");
                                column = 1;
                         }

                         int currrent_row=0;         
                         while (rs.next()) {
                                if (DBApiList.get(__queue).getMoreStmt()==more_stmt_count) {
                                    
                                    String numberRefined = rs.getString(column);
                                    if (rs.wasNull()) {
                                        numberRefined = "0";
                                    } else {
                                    }
                                    if (DBApiList.get(__queue).isInteger()){
                                        ret = numberRefined.replaceAll("[^0-9]", "");
                                    } else if (DBApiList.get(__queue).isLong()){
                                        numberRefined=numberRefined.replaceAll("[^0-9]", "");
                                        ret = numberRefined;
                                    } else if (DBApiList.get(__queue).isFloat()){
                                         numberRefined=numberRefined.replaceAll("[^0-9,.]", "");
                                         numberRefined=numberRefined.replace(",", ".");
                                         ret = numberRefined;
                                    }
                                }

                                if (DBApiList.get(__queue).getRow() == currrent_row){
                                   break;
                                }
                                currrent_row++;
                         }
                      
                          
                       
                     int rowsSelected = stmt.getUpdateCount();
                     if (rowsSelected >= 0){
                          // logger.debug(rowsSelected + "  row(s) affected");
                     }
                      more_stmt_count++;
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         //logger.warn(sqlW);
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {
                         //logger.debug(updates + " row(s) affected");

                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while ((hasMoreResults) || (updates != -1));
             
                attemptClose(rs);
                attemptClose(stmt);
                attemptClose(conn);
        } 
        catch (SQLException e) {
               ret="-205";
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError=true;
               errorDescr = e.getMessage();
       }
        
        return ret;

     }  
   
    
    
    private String doStringQuery(String _query){
      
          
        /*if (connections.isEmpty()){
            logger.error("Could not create connection to database server doStringQuery");
            isError=true;
            errorDescr = "Could not create connection to database server doStringQuery";
            return "CONN_ERROR";
        }
        
        int id=0;
        id = getPgDatabaseID(0);*/
        
        if (conn==null){
                logger.error("Could not create connection to database server doNumericQuery");
                isError=true;
                errorDescr = "Could not create connection to database server doNumericQuery";
                return "CONN_ERROR";
        }
        
        String ret="default";
     
        try {
                if ( DBApiList.get(__queue).getIsolationLevel()==-1) {
                   // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
                } else {
                     conn.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                }  
                
                if (!DBApiList.get(__queue).getDatabaseProperty("use_database", "no_use").equalsIgnoreCase("no_use")){
                    conn.setCatalog(DBApiList.get(__queue).getDatabaseProperty("use_database", "no_use"));
                }
           
               
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                 
                int updates = 0;
                SQLWarning sqlW = null;  
           
                int more_stmt_count = 1;
                
                isResult = stmt.execute(_query);
                         
                do {
                  
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 

                          if (DBApiList.get(__queue).getMoreStmt()==more_stmt_count) ret = printWarnings(sqlW);
                          stmt.clearWarnings();
                    } 
                 
                   if (isResult) {
                          //logger.debug("after: if (isResult)");
                     
                          rs = stmt.getResultSet();
                          rsmd = rs.getMetaData();
                          int column = DBApiList.get(__queue).getColumn();


                          int numCol = rsmd.getColumnCount();
                          if (column > numCol){
                                logger.warn("**** col > colcount ****");
                                column = 1;
                         }

                         int currrent_row=0;         
                         while (rs.next()) {
                                if (DBApiList.get(__queue).getMoreStmt()==more_stmt_count) {
                                    ret = rs.getString(column);
                                   // System.out.println(ret);
                                    if (rs.wasNull()) {
                                        ret = "0";
                                    } else {
                                    }
                                }

                                if (DBApiList.get(__queue).getRow() == currrent_row){
                                   break;
                                }
                                currrent_row++;
                         }
                      
                          
                       
                     int rowsSelected = stmt.getUpdateCount();
                     if (rowsSelected >= 0){
                           //logger.debug(rowsSelected + "  row(s) affected");
                     }
                      more_stmt_count++;
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {

                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while ((hasMoreResults) || (updates != -1));
             
               attemptClose(rs);
               attemptClose(stmt);
               attemptClose(conn);
        } 
        catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               ret = "ERROR_SQL";
               isError=true;
               errorDescr = e.getMessage();
       }
       
        return ret;

   }
    
    public String doPrepareQuery(String __query){
          
       
         boolean _isError = false;
         String ret="default";
             
         StringBuilder out = new StringBuilder();
         conn = getConnectionRetry(0);

            
          if (conn==null){
              logger.error("Could not create connection to database server doPrepareQuery");
              isError=true;
              return "default";
          }
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
            
          try {  
             
                 if (DBApiList.get(__queue).getIsolationLevel()==-1) {
                   // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
                } else {
                     conn.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                }
           
              Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
              isResult = stmt.execute(__query);
          
                              
             do {
                    
                 
                if (isResult) {
                 
                     rs = stmt.getResultSet();
                     rsmd = rs.getMetaData();
                    
                     int numCol = rsmd.getColumnCount();
                                       
                     while (rs.next()) {
                        
                           for (int i=1; i<=numCol; i++) {
                                    String add = rs.getString(i);
                                    if (rs.wasNull()) {
                                        add = "(NULL)";
                                    } else {
                                    }
                                   
                                     out.append(add);
                                     out.append("  ");
                              
                                 
                           }
                        
                            out.append(newline);
                            
                    }
                                   
                        
                } 
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while (hasMoreResults);
            

                attemptClose(rs);
                attemptClose(stmt);
                attemptClose(conn);

            } 
              catch (SQLException e) { 
                     
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
                    
                    isError=true;
                    errorDescr = e.getMessage();
                    
                    _isError=true;
              }
             
            ret=out.toString();
          
            return ret;
             
      }
    
      private String doQuery(String _query){
            
       
            boolean _isError = false;
            boolean _isWarn = false;
            String ret="default";
             
            StringBuilder out = new StringBuilder();
     

            
            if (conn==null){
                logger.error("Could not create connection to database server doNumericQuery");
                isError=true;
                errorDescr = "Could not create connection to database server doNumericQuery";
                return "CONN_ERROR";
            }
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
            
             try {  
             
                 if (DBApiList.get(__queue).getIsolationLevel()==-1) {
                   // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
                } else {
                     conn.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                }
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
               
              
               isResult = stmt.execute(_query);
          
               int updates = 0;
               SQLWarning sqlW = null;  
               sqlW = stmt.getWarnings();
                 
               do {
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                        if (!DBApiList.get(__queue).isNoData()) out.append(printWarnings(sqlW));
                        _isWarn = true;
                        stmt.clearWarnings();
                    } 
                 
                  if (isResult) {
                 
                      rs = stmt.getResultSet();
                      rsmd = rs.getMetaData();
                    
                      int numCol = rsmd.getColumnCount();
                      if (!DBApiList.get(__queue).isNoData()){ 
                          if (DBApiList.get(__queue).isHtml()) out.append("<table><tr>");
                          for (int i=1; i<=numCol; i++) {
                               if (DBApiList.get(__queue).isHtml()){
                                    out.append("<th>");
                                    out.append(rsmd.getColumnName(i));
                                    out.append("</th>");
                               } else {
                                 out.append(rsmd.getColumnName(i));
                                 out.append(" | ");
                               }
                          }  
                          if (DBApiList.get(__queue).isHtml()) {
                              out.append("</tr>");
                          } else {
                              out.append(newline);
                          }
                               
                      }
                                       
                     while (rs.next()) {
                           if (!DBApiList.get(__queue).isNoData()) {
                               if (DBApiList.get(__queue).isHtml()) out.append("<tr>");
                           }
                           for (int i=1; i<=numCol; i++) {
                                if (!DBApiList.get(__queue).isNoData()){
                                    String add = rs.getString(i);
                                    if (rs.wasNull()) {
                                        add = "(NULL)";
                                    } else {
                                    }
                                    if (DBApiList.get(__queue).isHtml()){
                                        out.append("<td>");
                                        out.append(add);
                                        out.append("</td>");
                                    } else {
                                         out.append(add);
                                        out.append(" | ");
                                    }
                                    
                                }
                                 
                           }
                           if (!DBApiList.get(__queue).isNoData()) {
                                if (DBApiList.get(__queue).isHtml()) {
                                    out.append("</tr>");
                                } else {
                                    out.append(newline);
                                }
                           }
                      }
                              
                    if (!DBApiList.get(__queue).isNoData()){ 
                          if (DBApiList.get(__queue).isHtml()) out.append("</table>"); 
                    }
                    
                    int rowsSelected = stmt.getUpdateCount();
                    if (rowsSelected >= 0){
                        if (!DBApiList.get(__queue).isNoData()) {
                            if (DBApiList.get(__queue).isHtml()) out.append("<br/>"); 
                            if (DBApiList.get(__queue).isShowCount()) out.append(rowsSelected).append("  row(s) affected");
                            if (DBApiList.get(__queue).isHtml()) out.append("<br/>");
                            else out.append(newline);
                        }
                    }
                     
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         logger.warn(sqlW);
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {
                         if (!DBApiList.get(__queue).isNoData()) {
                            if (DBApiList.get(__queue).isHtml()) out.append("<br/>"); 
                          
                            if (DBApiList.get(__queue).isShowCount())  out.append(updates + " row(s) updated");
                          
                            if (DBApiList.get(__queue).isHtml()) out.append("<br/>");
                            else out.append(newline);
                         };

                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while ((hasMoreResults) || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);
                attemptClose(conn);

            } 
              catch (SQLException e) {             
                    if (!DBApiList.get(__queue).isNoData()) {
                          if (DBApiList.get(__queue).isHtml()){
                             out.append( e.getMessage());
                             out.append("<br/>");
                          }  else{
                             out.append( e.getMessage());
                             out.append(newline);
                          }
                    }
                     
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
                    
                    isError=true;
                    errorDescr = e.getMessage();
                    
                    _isError=true;
              }
             
             if (DBApiList.get(__queue).isNoData()) {
                 if (_isWarn) ret ="WARN";
                 if (_isError) ret ="ERROR";
                 if (!_isError && !_isWarn) ret ="EXECUTED";
             } else ret=out.toString();
          
             return ret;
             
      }
    
    private String doQueryCompare(){
           
          String url=DBApiList.get(__queue).getDatabaseProperty("jdbc_url_master","master");
          url=url.replace("{port}", DBApiList.get(__queue).getDatabaseProperty("jdbc_port_master","25"));
          url=url.replace("{host}", DBApiList.get(__queue).getDatabaseProperty("jdbc_host_master","master"));
          url=url.replace("{database}", DBApiList.get(__queue).getDatabaseProperty("jdbc_database_master","master"));
          url=url.replace("{user}", DBApiList.get(__queue).getDatabaseProperty("jdbc_user_master","master"));
          url=url.replace("{password}",  DBApiList.get(__queue).getDatabaseProperty("jdbc_password_master","master"));
    
                
        Connection conn1 = getConnectionRetry(DBApiList.get(__queue).getDatabaseProperty("jdbc_host_master", "master"),
                Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("jdbc_port_master","25")),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_database_master","master"),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_user_master","master"),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_password_master","master"),
                Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("jdbc_timeout_master","25")),
                Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("jdbc_retry_master","5")),
                url,
                DBApiList.get(__queue).getDatabaseProperty("jdbc_url_master","master")); 
      
        url=DBApiList.get(__queue).getDatabaseProperty("jdbc_url_slave","slave");
        url=url.replace("{port}", DBApiList.get(__queue).getDatabaseProperty("jdbc_port_slave","25"));
        url=url.replace("{host}", DBApiList.get(__queue).getDatabaseProperty("jdbc_host_slave","slave"));
        url=url.replace("{database}", DBApiList.get(__queue).getDatabaseProperty("jdbc_database_slave","slave"));
        url=url.replace("{user}", DBApiList.get(__queue).getDatabaseProperty("jdbc_user_slave","slave"));
        url=url.replace("{password}",  DBApiList.get(__queue).getDatabaseProperty("jdbc_password_slave","slave"));
          
        Connection conn2 = getConnectionRetry(DBApiList.get(__queue).getDatabaseProperty("jdbc_host_slave","slave"),
                Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("jdbc_port_slave","25")),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_database_slave","master"),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_user_slave","slave"),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_password_slave","slave"),
                Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("jdbc_timeout_slave","25")),
                Integer.parseInt(DBApiList.get(__queue).getDatabaseProperty("jdbc_retry_slave","5")),
                DBApiList.get(__queue).getDatabaseProperty("jdbc_class_slave","slave"),
                url);
        
        if (conn1 == null || conn2 == null){
            logger.error("Connection 1 or 2 is null");
            isError = true; 
            errorDescr = "Connection 1 or 2 is null";
            return "ERROR_CONN";
        }
        
        String ret="default";
        String ret1="default";
        String ret2="default";
     
        try {
                if (DBApiList.get(__queue).getDatabaseProperty("isolation_level", "default").equalsIgnoreCase("default")) {
                
                } else {
                   conn1.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                   conn2.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                }
               
                Statement stmt1 = conn1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                Statement stmt2 = conn2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs1 = null;
                ResultSet rs2 = null;
                ResultSetMetaData rsmd1 = null;
                ResultSetMetaData rsmd2 = null;
                boolean isResult1 = false;
                boolean isResult2 = false;
               
                String query = DBApiList.get(__queue).getSqlQuery();
                
                isResult1 = stmt1.execute(DBApiList.get(__queue).getDatabaseProperty("query-master", query));
                isResult2 = stmt2.execute(DBApiList.get(__queue).getDatabaseProperty("query-slave", query));
                               
                int ile_ok=0, ile_bad=0;
                             
                if (isResult1 && isResult2) {
                     
                          rs1 = stmt1.getResultSet();
                          rs2 = stmt2.getResultSet();
                          rsmd1 = rs1.getMetaData();
                          rsmd2 = rs2.getMetaData();

                          int numCol1 = rsmd1.getColumnCount();
                          int numCol2 = rsmd2.getColumnCount();
                          
                          if (numCol1!=numCol2) {
                              isError = true;
                              return "numCol1<>numCol2";
                          }
                        
                          
                          while (rs1.next()) {
                              if (rs2.next()){
                                     for (int i=1; i<= numCol1; i++){
                                        ret1 = rs1.getString(i);
                                        ret2 = rs2.getString(i);
                                        if (ret1==null && ret2==null) ile_ok++;
                                        else if (ret1==null && ret2!=null) ile_bad++;
                                        else if (ret1!=null && ret2==null) ile_bad++;
                                        else if (ret1.equalsIgnoreCase(ret2)) ile_ok++;
                                        else ile_bad++;
                                     }
                                
                              } else {
                                 return "numRow1<>numRow2"; 
                              }
                           
                          }
                          
                          ret = "" + 100.0*ile_ok/(1.0*(ile_ok+ile_bad));
                     
                  } else   {
                      isError=true;
                      return "No results";
                  }
             
                attemptClose(rs1);
                attemptClose(rs2);
                attemptClose(stmt1);
                attemptClose(stmt2); 
                attemptClose(conn1);
                attemptClose(conn2);
              
        
        } catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               ret = "ERROR_SQL"; 
               isError = true; 
               errorDescr = e.getMessage();
       }
    
        return ret;

    }
   
    
   public List<List<String>> getDiscoveryMap(int _queue, String __query, boolean metaDataUpperCase){
      
        this.__queue=_queue;
        
        if (_queue>DBApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false; 
        
        List<List<String>> values = new ArrayList<>();
        boolean add_metadata=true;
        boolean firstDatabase=true;
        
        for (int indx=0; indx<DBApiList.get(__queue).getJdbc_driverSize();indx++){
            
                connections = getConnectionsRetry(indx);  
                if (connections.isEmpty()){
                    isError = true;
                    continue;
                } 
                
                int id=0;                
                boolean isPgDatabase = isPgDatabase();
                if (isPgDatabase) id = getPgDatabaseID(indx); 
            
                for (int loop=0; loop<connections.size(); loop++){
                    
                                        
                    if (connections.get(loop)== null){
                        isError = true;
                        continue;
                    } 
                    
                    if (isPgDatabase) {
                        if (loop==id) firstDatabase=true;
                        else firstDatabase=false;
                    }
                        
                  
                    if ( firstDatabase){
                        if (DBApiList.get(__queue).isOnlyFirstDatabase()) firstDatabase=false;

                        ResultSet rs = null;
                        boolean isResult = false;
                        boolean hasMoreResults = false;

                        try { 


                           Statement stmt = connections.get(loop).createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                           isResult = stmt.execute(__query);


                           int updates = 0;


                           do {
                                 if (isResult) {

                                   rs = stmt.getResultSet();         

                                   if (add_metadata){
                                           if (indx==0){
                                               List<String> metaData = new ArrayList<>();

                                               ResultSetMetaData meta = rs.getMetaData();
                                               for (int i = 1; i <= meta.getColumnCount(); i++) {
                                                   if (metaDataUpperCase) metaData.add(meta.getColumnLabel(i).toUpperCase());
                                                   else  metaData.add(meta.getColumnLabel(i));
                                               }
                                               values.add(metaData);
                                               add_metadata=false;
                                           }

                                   }

                                   while (rs.next()) {
                                           ResultSetMetaData meta = rs.getMetaData();
                                           List<String> Data = new ArrayList<>();
                                           for (int i = 1; i <= meta.getColumnCount(); i++) {
                                                  String dt = rs.getString(i);
                                                  if (rs.wasNull()){
                                                       Data.add("null");
                                                  } else {
                                                       Data.add(dt.replace("\\", "\\\\"));
                                                  }
                                           }
                                           values.add(Data);
                                      }


                                  } else   {

                                       updates = stmt.getUpdateCount();

                                   }

                                    hasMoreResults = stmt.getMoreResults();
                                    isResult = hasMoreResults;
                                }  while ((hasMoreResults) || (updates != -1));


                                attemptClose(rs);
                                attemptClose(stmt);  


                         }   catch (SQLException e) {
                               isError=true;
                               logger.error("SQL Exception/Error:");
                               logger.error("error message=" + e.getMessage());
                               logger.error("SQL State= " + e.getSQLState());
                               logger.error("Vendor Error Code= " + e.getErrorCode());
                               logger.error("SQL Exception/Error: ", e);
                               logger.error("__query " + __query);
                               isError = true;
                               errorDescr = "SQL Exception/Error: " + e.getMessage();
                        }

                    attemptClose(connections.get(loop));
                } else {
                   attemptClose(connections.get(loop));     
                }
            }
        } //end for connections
        
        return values;
   } 
   
    
   private Connection getConnectionRetry(String host, int port, String database, String user, String password, int timeout, int retry, String className, String url) {
        int licz_conn=0;
        Connection connection =null;
        while (licz_conn<=retry){
           connection = getConnection(host, port, database, user, password, timeout, className, url);
           if (connection==null){
                logger.error("Conn in DBCore is null RETRY " + licz_conn);
                licz_conn++;
            } else {
                   licz_conn=retry+1;
                break;
            }
        }
        
        return connection;
  }
   
   private Connection getConnection(String host, int port, String database, String user, String password, int timeout, String className, String url) {
           Properties prop = new Properties();

           prop.setProperty("user", user);
           prop.setProperty("password", password);
           prop.setProperty("applicationname", "Zabbix");
          

           try {
             
               DriverManager.setLoginTimeout(timeout);
               Class.forName(className);
               Connection connection =  DriverManager.getConnection(url, prop);
         
               if(connection!=null){
                   logger.info("Connected to " + host+":"+port);
               }
           return connection;
          } catch (ClassNotFoundException e) {
                 logger.error("ClassNotFoundException:");
                 logger.error("error message=" + e.getMessage());
                 return null;

          } catch (SQLException e) {
              while(e != null) {
                            logger.error("SQL Exception/Error:");
                            logger.error("error message=" + e.getMessage());
                            logger.error("SQL State= " + e.getSQLState());
                            logger.error("Vendor Error Code= " + e.getErrorCode());

                            // it is possible to chain the errors and find the most
                            // detailed errors about the exception
                            e = e.getNextException( );
               }
               return null;
          } catch (Exception e2) {
            // handle non-SQL exception …
              logger.error("SQL Exception/Error:");
              logger.error("error message=" + e2.getMessage());

               return null;
          }

     }  
   
    private List<Connection>  getConnectionsRetry(int indx) {
        int licz_conn=0;
        while (licz_conn<=DBApiList.get(__queue).getJdbc_driver(indx).getConnectionRetry()){
           connections = getConnections(indx);
          
           if (connections==null){
                logger.error("Conn in DBCore is null RETRY " + licz_conn);
                licz_conn++;
            } else {
               // licz_conn=DBApiList.get(__queue).getJdbc_driver(indx).getConnectionRetry()+1;
                break;
            }
        }
        
        return connections;
  }
    
   private Connection  getConnectionRetry(int indx) {
        int licz_conn=0;
        Connection connection=null;
        while (licz_conn<=DBApiList.get(__queue).getJdbc_driver(indx).getConnectionRetry()){
           connection = getConnection(indx);
          
           if (connections==null){
                logger.error("Conn in DBCore is null RETRY " + licz_conn);
                licz_conn++;
            } else {
               // licz_conn=DBApiList.get(__queue).getJdbc_driver(indx).getConnectionRetry()+1;
                break;
            }
        }
        
        return connection;
  }
   
   
   private List<Connection> getConnections(int indx) {
       
           connections.clear();
          
           Properties prop = new Properties();

           prop.setProperty("user", DBApiList.get(__queue).getJdbc_driver(indx).getUser());
           prop.setProperty("password", DBApiList.get(__queue).getJdbc_driver(indx).getPassword());
           prop.setProperty("applicationname", "pool.Zabbix");
          
          if (DBApiList.get(__queue).getJdbc_driver(indx).getInterfaceType().equalsIgnoreCase("single")){
         
                  try {

                       DriverManager.setLoginTimeout(DBApiList.get(__queue).getJdbc_driver(indx).getLoginTimeout());
                       Class.forName(DBApiList.get(__queue).getJdbc_driver(indx).getClassName());
                       Connection connection =  DriverManager.getConnection(DBApiList.get(__queue).getJdbc_driver(indx).getUrl(), prop);

                       if(connection!=null){
                           logger.info("Connected to " + DBApiList.get(__queue).getJdbc_driver(indx).getHost()+":"+DBApiList.get(__queue).getJdbc_driver(indx).getPort());
                       }
                       connections.add(connection);
                       return connections;
                  } catch (ClassNotFoundException e) {
                         logger.error("ClassNotFoundException:");
                         logger.error("error message=" + e.getMessage());
                         return null;

                  } catch (SQLException e) {
                      while(e != null) {
                                    logger.error("SQL Exception/Error:");
                                    logger.error("error message=" + e.getMessage());
                                    logger.error("SQL State= " + e.getSQLState());
                                    logger.error("Vendor Error Code= " + e.getErrorCode());

                                    // it is possible to chain the errors and find the most
                                    // detailed errors about the exception
                                    e = e.getNextException( );
                       }
                       return null;
                  } catch (Exception e2) {
                    // handle non-SQL exception …
                      logger.error("SQL Exception/Error:");
                      logger.error("error message=" + e2.getMessage());

                       return null;
                  }
          } else {
               logger.debug("POOLING Connected to " + DBApiList.get(__queue).getJdbc_driver(indx).getHost()+":"+DBApiList.get(__queue).getJdbc_driver(indx).getPort());
              
               for (DataSource ds : DataSource.getInstances(DBApiList.get(__queue).getJdbc_driver(indx).getPoolIndex()) ){
                    if (ds!=null){
                        Connection connection = ds.getConnection();
                        try { 
                           if (DBApiList.get(__queue).getIsolationLevel()==-1) {

                           } else {
                                connection.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                           } 
                       } catch (Exception ex) {
                               logger.error(ex);
                       }
                       connections.add(connection);
                    }
                  
               }
            
               return connections;
             
            
          }

     }
   
   private Connection getConnection(int indx) {
       
           Properties prop = new Properties();

           prop.setProperty("user", DBApiList.get(__queue).getJdbc_driver(indx).getUser());
           prop.setProperty("password", DBApiList.get(__queue).getJdbc_driver(indx).getPassword());
           prop.setProperty("applicationname", "pool.Zabbix");
          
        
           if (DBApiList.get(__queue).getJdbc_driver(indx).getInterfaceType().equalsIgnoreCase("single")){
         
                  try {

                       DriverManager.setLoginTimeout(DBApiList.get(__queue).getJdbc_driver(indx).getLoginTimeout());
                       Class.forName(DBApiList.get(__queue).getJdbc_driver(indx).getClassName());
                       Connection connection =  DriverManager.getConnection(DBApiList.get(__queue).getJdbc_driver(indx).getUrl(), prop);

                       if(connection!=null){
                           logger.info("Connected to " + DBApiList.get(__queue).getJdbc_driver(indx).getHost()+":"+DBApiList.get(__queue).getJdbc_driver(indx).getPort());
                       }
                      
                       return connection;
                  } catch (ClassNotFoundException e) {
                         logger.error("ClassNotFoundException:");
                         logger.error("error message=" + e.getMessage());
                         return null;

                  } catch (SQLException e) {
                      while(e != null) {
                                    logger.error("SQL Exception/Error:");
                                    logger.error("error message=" + e.getMessage());
                                    logger.error("SQL State= " + e.getSQLState());
                                    logger.error("Vendor Error Code= " + e.getErrorCode());

                                    // it is possible to chain the errors and find the most
                                    // detailed errors about the exception
                                    e = e.getNextException( );
                       }
                       return null;
                  } catch (Exception e2) {
                    // handle non-SQL exception …
                      logger.error("SQL Exception/Error:");
                      logger.error("error message=" + e2.getMessage());

                       return null;
                  }
          } else {
                logger.debug("POOLING Connected to " + DBApiList.get(__queue).getJdbc_driver(indx).getHost()+":"+DBApiList.get(__queue).getJdbc_driver(indx).getPort());
                Connection connection = null;
                int id=0;
                id = getPgDatabaseID(0);
                DataSource ds = DataSource.getInstanceInPool(DBApiList.get(__queue).getJdbc_driver(indx).getPoolIndex(), id);
                
                if (ds!=null){
                    connection = ds.getConnection();
                    try { 
                       if (DBApiList.get(__queue).getIsolationLevel()==-1) {

                       } else {
                            connection.setTransactionIsolation(DBApiList.get(__queue).getIsolationLevel());
                       } 
                   } catch (Exception ex) {
                           logger.error(ex);
                   }
                }
            
               return connection;
             
            
          }

     }
    
    static void attemptClose(ResultSet o) {
	try
	    { 
                if (o != null){
                    o.close();
                }
            }
        
	catch (Exception e)
	    {
               logger.error(e);
            }
    }

    static void attemptClose(Statement o) {
	try
	    { if (o != null) o.close();}
	catch (Exception e)
	    {
                logger.error(e);
            }
    }

    static void attemptClose(Connection o)  {
	try
	    { if (o != null) o.close();}
	catch (Exception e)
	    { 
               logger.error(e);
            }
    }
    
    public void attemptCloseConnections()  {
	for (int c=0; c<connections.size(); c++) attemptClose(connections.get(c));
    }
  
    
    public boolean isError(){
         return isError;
    }
 
    
}