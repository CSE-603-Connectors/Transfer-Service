package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.jsch.SftpSessionPool;
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

    boolean readerMadeSFTP;
    boolean writerMadeSFTP;
    @Value("${ods.use.jsch}")
    int useJsch;//0 for jsch, 1 for mina, 2 for sshj

    @Autowired
    SshConnectionCreator connectionCreator;

    public ConnectionBag() {
        readerMadeSFTP = false;
        writerMadeSFTP = false;
    }

    public void preparePools(TransferJobRequest request) {
        if (request.getSource().getType().equals(EndpointType.sftp)) {
            readerMadeSFTP = true;
            System.out.println(request.getSource().toString());
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.sftp)) {
            writerMadeSFTP = true;
            this.createSftpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
    }

    public void closePools() {
        if(useJsch == 0){
            if(readerMadeSFTP){
                sftpReaderPool.close();
            }
            if(writerMadeSFTP){
                sftpWriterPool.close();
            }
        }else if(useJsch == 1){
            if (readerMadeSFTP) {
                sshjReaderPool.close();
            }
            if (writerMadeSFTP) {
                sshjWriterPool.close();
            }
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
