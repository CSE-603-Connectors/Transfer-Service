package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.jsch.SftpSessionPool;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for preparing the SFTP & FTP conneciton pool for readers and writers
 */
@Getter
@Component
public class ConnectionBag {
    private SftpSessionPool sftpReaderPool;
    private SftpSessionPool sftpWriterPool;
    boolean readerMadeSFTP;
    boolean writerMadeSFTP;

    public ConnectionBag(){
        readerMadeSFTP = false;
        writerMadeSFTP= false;
    }

    public void preparePools(TransferJobRequest request) {
        if (request.getSource().getType().equals(EndpointType.sftp)) {
            readerMadeSFTP = true;
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getDestination().getType().equals(EndpointType.sftp)) {
            writerMadeSFTP = true;
            this.createSftpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount());
        }
    }

    public void closePools() {
        if(readerMadeSFTP){
            sftpReaderPool.close();
        }
        if(writerMadeSFTP){
            sftpWriterPool.close();;
        }
    }

    public void createSftpReaderPool(AccountEndpointCredential credential, int connectionCount) {
        sftpReaderPool = new SftpSessionPool(credential);
        sftpReaderPool.addObjects(connectionCount);
    }

    public void createSftpWriterPool(AccountEndpointCredential credential, int connectionCount) {
        sftpWriterPool = new SftpSessionPool(credential);
        sftpWriterPool.addObjects(connectionCount);
    }

    public void prepareFtpReader(AccountEndpointCredential credential, int connectionCount) {

    }

    public void prepareFtpWriter(AccountEndpointCredential credential, int connectionCount) {

    }


}
