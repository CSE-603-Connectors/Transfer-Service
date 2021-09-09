package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.pools.FTPConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.pools.SftpSessionPool;
import org.onedatashare.transferservice.odstransferservice.service.pools.SSHJSessionPool;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SshConnectionCreator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for preparing the SFTP & FTP conneciton pool for readers and writers
 */
@Getter
@Component
public class ConnectionBag {
    private SftpSessionPool sftpReaderPool;
    private SftpSessionPool sftpWriterPool;
    private SSHJSessionPool sshjReaderPool;
    private SSHJSessionPool sshjWriterPool;
    private FTPConnectionPool ftpReaderPool;
    private FTPConnectionPool ftpWriterPool;

    EndpointType readerType;
    EndpointType writerType;
    boolean readerMade;
    boolean writerMade;
    @Value("${ods.use.jsch}")
    int useJsch;//0 for jsch, 1 for mina, 2 for sshj

    @Autowired
    SshConnectionCreator connectionCreator;

    public ConnectionBag() {
        readerMade = false;
        writerMade = false;
    }

    public void preparePools(TransferJobRequest request) {
        if (request.getSource().getType().equals(EndpointType.sftp)) {
            readerMade = true;
            readerType = EndpointType.sftp;
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.sftp)) {
            writerMade = true;
            writerType = EndpointType.sftp;
            this.createSftpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }

        if (request.getSource().getType().equals(EndpointType.ftp)) {
            readerType = EndpointType.ftp;
            readerMade = true;
            this.createFtpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.ftp)) {
            writerMade = true;
            writerType = EndpointType.ftp;
            this.createFtpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
    }

    public void closePools() {
        if(readerType != null){
            switch (readerType){
                case ftp:
                    this.ftpReaderPool.close();
                    break;
                case sftp:
                    if(useJsch == 0){
                        sftpReaderPool.close();
                    }else{
                        sshjReaderPool.close();;
                    }
                    break;
            }
        }
        if(writerType != null){
            switch (writerType){
                case ftp:
                    this.ftpWriterPool.close();
                    break;
                case sftp:
                    if(useJsch == 0){
                        sftpWriterPool.close();
                    }else{
                        sshjWriterPool.close();
                    }
                    break;
            }
        }
    }

    public void createFtpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize){
        this.ftpReaderPool = new FTPConnectionPool(credential, chunkSize);
        try {
            this.ftpReaderPool.addObjects(connectionCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createFtpWriterPool(AccountEndpointCredential credential, int connectionCount, int chunkSize){
        this.ftpWriterPool = new FTPConnectionPool(credential, chunkSize);
        try {
            this.ftpWriterPool.addObjects(connectionCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createSftpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        if(useJsch == 0){
            this.sftpReaderPool = new SftpSessionPool(credential);
            this.sftpReaderPool.addObjects(connectionCount);
        }else if(useJsch == 1){
            this.sshjReaderPool = new SSHJSessionPool(credential, chunkSize);
            this.sshjReaderPool.setConnectionCreator(this.connectionCreator);
            this.sshjReaderPool.addObjects(connectionCount);
        }
    }

    public void createSftpWriterPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        if(useJsch == 0){
            this.sftpWriterPool = new SftpSessionPool(credential);
            this.sftpWriterPool.addObjects(connectionCount);
        }else if(useJsch == 1){
            this.sshjWriterPool = new SSHJSessionPool(credential, chunkSize);
            this.sshjWriterPool.setConnectionCreator(this.connectionCreator);
            this.sshjWriterPool.addObjects(connectionCount);
        }
    }
}
