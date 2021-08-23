package org.onedatashare.transferservice.odstransferservice.service.jsch;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SftpSessionPool implements ObjectPool<Session> {


    BlockingQueue<Session> sessionBlockingQueue;
    private final AccountEndpointCredential credential;
    JSch jsch;
    Logger logger = LoggerFactory.getLogger(SftpSessionPool.class);

    public SftpSessionPool(AccountEndpointCredential credential){
        this.credential = credential;
        this.sessionBlockingQueue = new LinkedBlockingQueue<>();
        jsch = new JSch();
    }
    
    @Override
    public void addObject() {
        this.sessionBlockingQueue.add(Objects.requireNonNull(SftpUtility.createJschSession(jsch, this.credential)));
    }

    @Override
    public void addObjects(int count) {
        logger.info("creating {} number of connections", count);
        for(int i = 0; i < count; i++){
            this.addObject();
        }
    }

    @Override
    public Session borrowObject() throws InterruptedException {
        logger.info("trying to borrow an object");
        return this.sessionBlockingQueue.take();
    }

    @Override
    public void clear() {
        this.sessionBlockingQueue.removeIf(channelSftp -> !channelSftp.isConnected());
    }

    @Override
    public void close() {
        logger.info("Closing an sftp session pool");
        for(Session session : this.sessionBlockingQueue){
            session.disconnect();
            this.sessionBlockingQueue.remove(session);
        }
    }

    @Override
    public int getNumActive() {
        int count = 0;
        for(Session session : this.sessionBlockingQueue){
            if(session.isConnected()){
                count ++;
            }
        }
        return count;
    }

    @Override
    public int getNumIdle() {
        int count = 0;
        for(Session session : this.sessionBlockingQueue){
            if(!session.isConnected()){
                count ++;
            }
        }
        return count;
    }

    @Override
    public void invalidateObject(Session obj) {
        for(Session session : this.sessionBlockingQueue){
            if(session.equals(obj)){
                session.disconnect();
                this.sessionBlockingQueue.remove(session);
            }
        }
    }

    @Override
    public void returnObject(Session obj) {
        logger.info("trying to return an object");
        this.sessionBlockingQueue.add(obj);
    }
}
