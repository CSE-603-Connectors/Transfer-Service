package org.onedatashare.transferservice.odstransferservice.pools;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class FtpConnectionPool implements ObjectPool<FTPClient> {

    private final AccountEndpointCredential credential;
    private final int bufferSize;
    private LinkedBlockingQueue<FTPClient> connectionPool;
    Logger logger = LoggerFactory.getLogger(FtpConnectionPool.class);

    public FtpConnectionPool(AccountEndpointCredential credential, int bufferSize){
        this.credential = credential;
        this.bufferSize = 0;
        this.connectionPool = new LinkedBlockingQueue<>();
    }

    @Override
    public void addObject() throws IOException {
        FTPClient client = new FTPClient();
        String[] hostAndPort = AccountEndpointCredential.uriFormat(credential, EndpointType.ftp);
        if(hostAndPort.length >1){
            client.connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        }else{
            client.connect(hostAndPort[0]);
        }
        int replyCode = client.getReplyCode();
        if (!FTPReply.isPositiveCompletion(replyCode)) {
            client.disconnect();
            throw new IOException("Exception in connecting to FTP Server");
        }
        boolean res = client.login(credential.getUsername(), credential.getSecret());
        if(!res){
            throw new IOException("Failed to Log into the FTP server bc the credentials did not work");
        }
        client.setBufferSize(this.bufferSize);
        client.setFileTransferMode(FTPClient.BLOCK_TRANSFER_MODE);
        client.setFileType(FTPClient.BINARY_FILE_TYPE);
        client.setAutodetectUTF8(true);
        client.enterLocalPassiveMode();
        client.setControlKeepAliveTimeout(300);
        this.connectionPool.add(client);
    }

    @Override
    public void addObjects(int count) throws IOException {
        for(int i = 0; i < count; i++){
            this.addObject();
            logger.info("creating conneciton {}", i+1);
        }
    }

    @Override
    public FTPClient borrowObject() throws InterruptedException {
        if(this.getNumActive() > 0){
            return this.connectionPool.take();
        }else{
            try {
                this.addObject();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return this.connectionPool.take();
        }
    }

    @Override
    public void clear() {
        this.connectionPool.removeIf(ftpClient -> !ftpClient.isConnected() || !ftpClient.isAvailable());
    }

    @Override
    public void close() {
        for(FTPClient ftpClient : this.connectionPool){
            this.connectionPool.remove(ftpClient);
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int getNumActive() {
        int count = 0;
        for(FTPClient client : this.connectionPool){
            if(client.isAvailable() && client.isConnected()){
                ++count;
            }
        }
        return count;
    }

    @Override
    public int getNumIdle() {
        int count = 0;
        for(FTPClient client : this.connectionPool){
            if(!client.isAvailable() || !client.isConnected()){
                ++count;
            }
        }
        return count;

    }

    @SneakyThrows
    @Override
    public void invalidateObject(FTPClient obj) {
        obj.disconnect();
        this.connectionPool.remove(obj);
        this.addObject();
    }

    @Override
    public void returnObject(FTPClient obj) {
        try {
            if(!obj.completePendingCommand()){
                obj.logout();
                obj.disconnect();
                this.connectionPool.remove(obj);
                this.addObject();
            }else{
                this.connectionPool.add(obj);
            }
        } catch (IOException ignored) {}
    }
}
