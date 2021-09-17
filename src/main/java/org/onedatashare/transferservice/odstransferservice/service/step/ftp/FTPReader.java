package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.FtpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class FTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);
    InputStream inputStream;
    String sBasePath;
    String fName;
    AccountEndpointCredential sourceCred;
    int chunckSize;
    FilePartitioner partitioner;
    EntityInfo fileInfo;
    private FtpConnectionPool connectionPool;
    private FTPClient client;

    public FTPReader(AccountEndpointCredential credential, EntityInfo file ,int chunckSize) {
        this.chunckSize = chunckSize;
        this.sourceCred = credential;
        this.partitioner = new FilePartitioner(this.chunckSize);
        fileInfo = file;
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        sBasePath += fileInfo.getPath();
        this.partitioner.createParts(this.fileInfo.getSize(), fileInfo.getId());
    }

    @AfterStep
    public void afterStep(){
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    public InputStream moveStream(InputStream stream, long moveBy) throws IOException {
        long totalMoved = 0;
        while(totalMoved < moveBy){
            long temp = stream.skip(moveBy-totalMoved);
            if(temp ==-1 || temp == 0) break;
            totalMoved+=temp;
        }
        logger.info("Moved the stream {}", totalMoved);
        return stream;
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        FilePart filePart = this.partitioner.nextPart();
        if(filePart == null) return null;
        logger.info("Current file part is {}", filePart);
        FTPClient client = this.connectionPool.borrowObject();
        InputStream inputStream = client.retrieveFileStream(this.fileInfo.getPath());
        logger.debug("Got input stream to {}", this.fileInfo.getPath());
        if(filePart.getStart()>0){ //the first chunk does not need to move anything
            moveStream(inputStream, filePart.getStart());
        }
        byte[] data = new byte[filePart.getSize()];
        int totalBytes = 0;
        while(totalBytes < filePart.getSize()){
            int byteRead = inputStream.read(data, totalBytes, filePart.getSize()-totalBytes);
            if (byteRead == -1) return null;
            totalBytes += byteRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(totalBytes, data, filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), this.fileInfo.getId());
        this.client.setRestartOffset(filePart.getStart());
        logger.info(chunk.toString());
        inputStream.close();
        this.connectionPool.returnObject(client);
        return chunk;
    }


    @Override
    protected void doOpen() throws InterruptedException, IOException {
        logger.info("Insided doOpen");
        this.client = this.connectionPool.borrowObject();
        this.inputStream = this.client.retrieveFileStream(this.fileInfo.getPath());
        //clientCreateSourceStream(sBasePath, fName);
    }

    @Override
    protected void doClose() {
        logger.info("Inside doClose");
        try {
            if (inputStream != null) inputStream.close();
        } catch (Exception ex) {
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
        this.connectionPool.returnObject(this.client);
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (FtpConnectionPool) connectionPool;
    }
}
