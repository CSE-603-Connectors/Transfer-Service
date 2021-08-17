package org.onedatashare.transferservice.odstransferservice.service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpClientFactory;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Writer;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsReader;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class ConnectionBag {

    ArrayList<AbstractItemCountingItemStreamItemReader<DataChunk>> readerList;
    ArrayList<ItemWriter<DataChunk>> writerList;
    BlockingQueue<FTPClient> ftpClientQueue;
    BlockingQueue<ChannelSftp> sftpClientQueue;
    private int concurrencyCount;

    public ConnectionBag(){
        this.readerList = new ArrayList<>();
        this.writerList = new ArrayList<>();
    }

    public void prepareFtp(int concurrencyCount){
        this.concurrencyCount = concurrencyCount;
        ftpClientQueue = new ArrayBlockingQueue<>(concurrencyCount, true);

    }

    public void prepareSftp(int concurrencyCount){
        this.concurrencyCount = concurrencyCount;
        sftpClientQueue = new ArrayBlockingQueue<>(concurrencyCount, true);
    }

    protected AbstractItemCountingItemStreamItemReader<DataChunk> getReader(TransferJobRequest.Source source, EntityInfo fileInfo, int chunkSize) {
        switch (source.getType()) {
            case vfs:
                VfsReader vfsReader = new VfsReader(source.getVfsSourceCredential(), fileInfo, chunkSize);
                readerList.add(vfsReader);
                return vfsReader;
            case sftp:
                SFTPReader sftpReader = new SFTPReader(source.getVfsSourceCredential(), fileInfo, chunkSize);
                readerList.add(sftpReader);
                return sftpReader;
            case ftp:
                FTPReader ftpReader = new FTPReader(source.getVfsSourceCredential(), fileInfo, chunkSize);
                this.readerList.add(ftpReader);
                return ftpReader;
            case s3:
                AmazonS3Reader s3Reader = new AmazonS3Reader(source.getVfsSourceCredential(), chunkSize);
                readerList.add(s3Reader);
                return s3Reader;
            case box:
                BoxReader boxReader = new BoxReader(source.getOauthSourceCredential(), fileInfo, chunkSize);
                readerList.add(boxReader);
                return boxReader;
        }
        return null;
    }

    protected ItemWriter<DataChunk> getWriter(TransferJobRequest.Destination destination, EntityInfo fileInfo) {
        switch (destination.getType()) {
            case vfs:
                VfsWriter vfsWriter = new VfsWriter(destination.getVfsDestCredential());
                writerList.add(vfsWriter);
                return vfsWriter;
            case sftp:
                SFTPWriter sftpWriter = new SFTPWriter(destination.getVfsDestCredential());
                writerList.add(sftpWriter);
                return sftpWriter;
            case ftp:
                FTPWriter ftpWriter = new FTPWriter(destination.getVfsDestCredential());
                writerList.add(ftpWriter);
                return ftpWriter;
            case s3:
                AmazonS3Writer s3Writer = new AmazonS3Writer(destination.getVfsDestCredential(), fileInfo);
                writerList.add(s3Writer);
                return s3Writer;
            case box:
                BoxWriter boxWriter = new BoxWriter(destination.getOauthDestCredential(), fileInfo);
                writerList.add(boxWriter);
                return boxWriter;
        }
        return null;
    }

    public void createReaders(int concurrencyCount){
        this.concurrencyCount = concurrencyCount;
        for(int i = 0; i < concurrencyCount; i++){

        }
    }

    public FTPClient createFtpClient(AccountEndpointCredential accountEndpointCredential, String basePath) throws FileSystemException {
        String hostAndPort = accountEndpointCredential.getUri().split("ftp://")[1];
        String[] hostAndPortArr = hostAndPort.split(":");
        FileSystemOptions fileSystemOptions = new FileSystemOptions();
        return FtpClientFactory.createConnection(hostAndPortArr[0], Integer.parseInt(hostAndPortArr[1]),accountEndpointCredential.getUsername().toCharArray(), accountEndpointCredential.getSecret().toCharArray(), basePath, fileSystemOptions);
    }

    public ChannelSftp createSftpClient(AccountEndpointCredential accountEndpointCredential, String basePath) throws JSchException, SftpException {
        JSch jsch = new JSch();
        ChannelSftp channelSftp = SftpUtility.createConnection(jsch, accountEndpointCredential);
        channelSftp.cd(basePath);
        return channelSftp;
    }
}
