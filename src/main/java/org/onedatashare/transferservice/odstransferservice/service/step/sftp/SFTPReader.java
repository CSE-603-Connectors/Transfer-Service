package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;

public class SFTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(SFTPReader.class);

    String sBasePath;
    String fName;
    AccountEndpointCredential sourceCred;
    EntityInfo fileInfo;
    int chunckSize;
    FilePartitioner filePartitioner;
    private SftpSessionPool sftpConnectionPool;
    private Session sftpSession;
    HashMap<String, ChannelSftp> threadChannels;

    public SFTPReader(AccountEndpointCredential credential, int chunckSize, EntityInfo file) {
        this.fileInfo = file;
        this.filePartitioner = new FilePartitioner(chunckSize);
        this.setName(ClassUtils.getShortName(SFTPReader.class));
        this.chunckSize = chunckSize;
        this.sourceCred = credential;
        threadChannels = new HashMap<>();
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        fName = fileInfo.getId();
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fName);
    }

    public synchronized ChannelSftp getChannelSftp(String currentThreadName) throws JSchException {
        ChannelSftp channelSftp = null;
        if (!this.threadChannels.containsKey(currentThreadName)) {
            channelSftp = (ChannelSftp) sftpSession.openChannel("sftp");
            channelSftp.connect();
            this.threadChannels.put(currentThreadName, channelSftp);
            return channelSftp;
        } else {
            return this.threadChannels.get(currentThreadName);
        }
    }

    @Override
    protected DataChunk doRead() throws IOException, SftpException, JSchException {
        FilePart currentChunk = this.filePartitioner.nextPart();
        if (currentChunk == null) return null;
        logger.info("Part to read {}", currentChunk);
        ChannelSftp channelSftp = getChannelSftp(Thread.currentThread().getName());
        InputStream chunkOfFile = channelSftp.get(fileInfo.getPath(), null, currentChunk.getStart());
        byte[] data = new byte[currentChunk.getSize()];
        int totalBytesRead = 0;
        while (totalBytesRead < currentChunk.getSize()) {
            int bytesRead = chunkOfFile.read(data, 0, currentChunk.getSize());
            if (bytesRead == -1) return null;
            totalBytesRead += bytesRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(currentChunk.getSize(), data, currentChunk.getStart(), (int) currentChunk.getPartIdx(), this.fileInfo.getId());
        chunkOfFile.close();
        logger.info("Read in {}", chunk.toString());
        return chunk;
    }

    /**
     * Gets a connection from the sftp reader connection pool
     * creates a ChannelSftp which is a wy to execute comands against Sftp sessions
     * Then we cd into the base directory where we can find the files.
     */
    @Override
    protected void doOpen() throws InterruptedException {
        this.sftpSession = this.sftpConnectionPool.borrowObject();
        logger.info("Got Session from SftpConnectionPool {}", this.sftpSession);
    }

    /**
     * Closes the channel sftp we open and releases the session object back to the pool
     */
    @Override
    protected void doClose() {
        for (Channel channel : this.threadChannels.values()) {
            channel.disconnect();
        }
        this.sftpConnectionPool.returnObject(this.sftpSession);
    }

    /**
     * Lets the SftpReader have access to a connection pool
     *
     * @param connectionPool
     */
    public void setConnectionPool(SftpSessionPool connectionPool) {
        this.sftpConnectionPool = connectionPool;
    }

}