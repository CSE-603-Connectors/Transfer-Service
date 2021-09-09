package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.onedatashare.transferservice.odstransferservice.model.*;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.pools.FTPConnectionPool;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);
    AccountEndpointCredential sourceCred;
    int chunkSize;
    FilePartitioner partitioner;
    EntityInfo fileInfo;
    private FTPConnectionPool connectionPool;
    private InputStream fileInputstream;
    private FTPClient client;

    public FTPReader(AccountEndpointCredential credential, EntityInfo file, int chunkSize) {
        this.chunkSize = chunkSize;
        this.sourceCred = credential;
        this.partitioner = new FilePartitioner(this.chunkSize);
        fileInfo = file;
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        this.partitioner.createParts(this.fileInfo.getSize(), fileInfo.getId());
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    protected DataChunk doRead() throws IOException {
        FilePart filePart = this.partitioner.nextPart();
        if(filePart == null) return null;
        logger.info("The current filePart is {}", filePart);
        return readInDataChunk(this.fileInputstream, filePart);
    }

    public synchronized DataChunk readInDataChunk(InputStream stream, FilePart filePart) throws IOException {
        byte[] buffer = new byte[filePart.getSize()];
        int totalBytes = 0;
        while(totalBytes < filePart.getSize()){
            int byteRead = stream.read(buffer, totalBytes, filePart.getSize()-totalBytes);
            if (byteRead == -1) return null;
            totalBytes += byteRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(buffer.length, buffer, filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), this.fileInfo.getPath(), this.fileInfo.getId());
        this.client.setRestartOffset(filePart.getEnd());
        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() throws IOException, InterruptedException {
        this.client = this.connectionPool.borrowObject();
        logger.info("Got the connection to the pool {}", this.client);
        InputStream stream  = client.retrieveFileStream(this.fileInfo.getPath());
        if(stream == null){
            logger.error("failed to open the input stream the reply is {}", this.client.getReplyString());
            if(425 == this.client.getReplyCode()){
                throw new IOException("The server failed to allocate resources for data connection by sending back code 425");
            }
        }
        logger.info("Got input stream to file {}, and the stream is {}", this.fileInfo.getPath(), this.fileInputstream);
        this.fileInputstream = stream;
    }

    @Override
    protected void doClose() {
        if(this.fileInputstream != null){
            try {
                this.fileInputstream.close();
                logger.info("Closed the input stream");
                while(!this.client.completePendingCommand());
                //this.client.completePendingCommand();
                logger.info("Completed the file transfer");
            } catch (IOException ignored) {}
        }
        this.connectionPool.returnObject(this.client);
    }

    public void setConnectionBag(FTPConnectionPool pool) {
        this.connectionPool = pool;
    }
}
