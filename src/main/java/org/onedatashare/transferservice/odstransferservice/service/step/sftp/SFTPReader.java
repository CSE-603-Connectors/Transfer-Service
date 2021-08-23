package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.jsch.SftpSessionPool;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import java.io.InputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(SFTPReader.class);

    String sBasePath;
    String fName;
    AccountEndpointCredential sourceCred;
    EntityInfo fileInfo;
    int chunckSize;
    FilePartitioner filePartitioner;
    private SftpSessionPool sftpConnectionPool;
    private Session sftpSession;
    private ChannelSftp channelSftp;

    public SFTPReader(AccountEndpointCredential credential, int chunckSize, EntityInfo file) {
        this.fileInfo = file;
        this.filePartitioner = new FilePartitioner(chunckSize);
        this.setName(ClassUtils.getShortName(SFTPReader.class));
        this.chunckSize = chunckSize;
        this.sourceCred = credential;
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        logger.info(sBasePath);
        fName = fileInfo.getId();
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fName);
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        FilePart currentChunk = this.filePartitioner.nextPart();
        if (currentChunk == null) return null;
        InputStream chunkOfFile = this.channelSftp.get(fileInfo.getPath(), null, currentChunk.getStart());//this should be a network call under the hood
        byte[] data = new byte[currentChunk.getSize()];
        int totalBytesRead = 0;
        while(totalBytesRead < currentChunk.getSize()){
            int bytesRead = chunkOfFile.readNBytes(data, 0, currentChunk.getSize());
            if(bytesRead == -1) return null;
            totalBytesRead += bytesRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(currentChunk.getSize(), data, currentChunk.getStart(), (int) currentChunk.getPartIdx(), this.fileInfo.getId());
        chunkOfFile.close();
        logger.info(chunk.toString());
        return chunk;
    }

    /**
     * Gets a connection from the sftp reader connection pool
     * creates a ChannelSftp which is a wy to execute comands against Sftp sessions
     * Then we cd into the base directory where we can find the files.
     */
    @Override
    protected void doOpen() throws InterruptedException, JSchException, SftpException {
        this.sftpSession = this.sftpConnectionPool.borrowObject();
        this.channelSftp = (ChannelSftp) sftpSession.openChannel("sftp");
        this.channelSftp.connect();
//        if(!sBasePath.isEmpty()){
//            channelSftp.cd(sBasePath);
//        }
        logger.info("after cd into base path" + channelSftp.pwd());
    }

    /**
     * Closes the channel sftp we open and releases the session object back to the pool
     */
    @Override
    protected void doClose() {
        this.channelSftp.disconnect();
        this.sftpConnectionPool.returnObject(this.sftpSession);
    }

    /**
     * Lets the SftpReader have access to a connection pool
     * @param connectionPool
     */
    public void setConnectionPool(SftpSessionPool connectionPool) {
        this.sftpConnectionPool = connectionPool;
    }

    @Override
    public void setResource(Resource resource) {

    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}