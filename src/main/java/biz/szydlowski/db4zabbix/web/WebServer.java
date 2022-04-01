/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.db4zabbix.web;

import static biz.szydlowski.db4zabbix.WorkingObjects.allowedConn;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Dominik
 */
public class WebServer  extends Thread {
   
    protected int serverPort   = 8080;
    protected ServerSocket serverSocket = null;
    protected boolean isStopped    = false;
    protected Thread runningThread = null;
    long id = 1;
    private int maxConnectionCount = 20;
    
    static final Logger logger = LogManager.getLogger(WebServer.class);
    ExecutorService pool;
      
    public WebServer (int port){
        this.serverPort = port;
    }
    
    public void setMaxConnectionCount(int set){
        maxConnectionCount = set;
    }

    @Override
    public void run(){
        synchronized(this){
            this.runningThread = Thread.currentThread();   
            this.runningThread.setName("WebServer");
        }
        try {
            openServerSocket();
        } catch (Exception e){
            isStopped = true;
            logger.error(e);
            
        }   
        
        pool = Executors.newFixedThreadPool(maxConnectionCount);  
        
        while(!isStopped()){ 
            boolean api=false; 
            Socket clientSocket = null;
            
            try {
                clientSocket = this.serverSocket.accept();
                clientSocket.setKeepAlive(true);
            
                String address = clientSocket.getInetAddress().getHostAddress();
                 
                Iterator<String> iteratorweb = allowedConn.iterator();
              
                while(iteratorweb.hasNext()){
                    String obj = iteratorweb.next();
                    if (obj.equals("all")){
                         api=true;
                         break;
                    } else {
                        if (obj.startsWith("*") || obj.endsWith("*")){
                            //gwiazdka
                            Pattern r = Pattern.compile(obj);
                            if ( r.matcher(address).find()){
                              api=true;
                              break;
                            } 
                            
                        } else {
                             if (obj.equalsIgnoreCase(address)){
                                 api=true;
                                 break;
                             }
                        }
                       
                    }
                }  
              //A client has connected to this server. Send welcome message
            } catch (IOException e) {
                if(isStopped()) {
                    logger.info("Server Stopped.") ;
                    return;
                }
                throw new RuntimeException("Error accepting client connection", e);
            }   
           
            pool.submit(new ServerWorkerRunnable(clientSocket, api));
        
          
            //}
           
        }
        logger.info("Server Stopped.") ;
    }


    private synchronized boolean isStopped() {
        return this.isStopped;
    }
   
    public void stopSever() {
        this.isStopped = true;
        try {
            logger.info("Currently active threads: " + Thread.activeCount());
            interruptAll();
            this.serverSocket.close();
        } catch (IOException e) {
            logger.error("Error closing server", e);
        }
    }

    
   private void interruptAll(){ 
        pool.shutdown();
   }

    private void openServerSocket() {
        try {
            this.serverSocket = new ServerSocket(this.serverPort);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port " + this.serverPort, e);
        }
    }

}