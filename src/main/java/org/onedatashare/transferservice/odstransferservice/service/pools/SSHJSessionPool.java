package org.onedatashare.transferservice.odstransferservice.service.pools;

import net.schmizz.sshj.SSHClient;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SshConnectionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class SSHJSessionPool implements ObjectPool<SSHClient> {

    private final AccountEndpointCredential credential;
    private final int chunkSize;
    private LinkedBlockingQueue<SSHClient> clientLinkedBlockingQueue;
    private SshConnectionCreator connectionCreator;
    Logger logger = LoggerFactory.getLogger(SSHJSessionPool.class);

    public SSHJSessionPool(AccountEndpointCredential credential, int chunkSize){
        this.credential = credential;
        this.chunkSize = chunkSize;
        this.clientLinkedBlockingQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void addObject() throws IOException {
        SSHClient connectedClient = connectionCreator.createConnectedAndAuthenticatedSSHClient(this.credential, this.chunkSize);
        if(connectedClient != null){
            if(connectedClient.isAuthenticated() && connectedClient.isConnected()){
                this.clientLinkedBlockingQueue.add(connectedClient);
            }else{
                logger.error("created a SSHClient that is not connected or not authenticated");
            }
        }
    }

    @Override
    public void addObjects(int count)  {
        for(int i = 0; i < count; i++){
            try {
                addObject();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public SSHClient borrowObject() throws InterruptedException {
        return this.clientLinkedBlockingQueue.take();
    }

    @Override
    public void clear() {
        for(SSHClient session : this.clientLinkedBlockingQueue){
            if(!session.isAuthenticated() || !session.isConnected()){
                try {
                    session.disconnect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                this.clientLinkedBlockingQueue.remove(session);
            }
        }
    }

    @Override
    public void close() {
        for(SSHClient session : this.clientLinkedBlockingQueue){
            try {
                session.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.clientLinkedBlockingQueue.remove(session);
        }
    }

    @Override
    public int getNumActive() {
        int count = 0;
        for(SSHClient client : this.clientLinkedBlockingQueue){
            if(client.isAuthenticated() && client.isAuthenticated()){
                count++;
            }
        }
        return count;
    }

    @Override
    public int getNumIdle() {
        int count = 0;
        for(SSHClient client : this.clientLinkedBlockingQueue){
            if(client.isConnected() && !client.isAuthenticated()){
                count++;
            }
        }
        return count;
    }

    @Override
    public void invalidateObject(SSHClient obj) {

    }

    @Override
    public void returnObject(SSHClient obj) {
        this.clientLinkedBlockingQueue.add(obj);
    }

    public void setConnectionCreator(SshConnectionCreator connectionCreator){
        this.connectionCreator = connectionCreator;
    }
}
