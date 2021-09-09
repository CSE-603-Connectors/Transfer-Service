package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.pools.SftpSessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class SFTPWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

    String stepName;
    private String dBasePath;
    AccountEndpointCredential destCred;
    HashMap<String, ChannelSftp> channelSftpMap;
    private SftpSessionPool sftpConnectionPool;
    private Session sftpSession;
    private ChannelSftp channelSftp;
    private AtomicLong totalSent;

    public SFTPWriter(AccountEndpointCredential destCred) {
        channelSftpMap = new HashMap<>();
        this.destCred = destCred;
    }

    public void setConnectionPool(SftpSessionPool connectionPool) {
        this.sftpConnectionPool = connectionPool;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException, JSchException {
        this.stepName = stepExecution.getStepName();
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.sftpSession = this.sftpConnectionPool.borrowObject();
        ChannelSftp channelSftp = (ChannelSftp) this.sftpSession.openChannel("sftp");
        channelSftp.connect();
        createRemoteFolder(channelSftp, this.dBasePath);
        channelSftp.disconnect();
        totalSent = new AtomicLong();
    }

    @AfterStep
    public void afterStep() {
        for (ChannelSftp value : channelSftpMap.values()) {
            if (!value.isConnected()) {
                value.disconnect();
                logger.info("Disconnected a Channel SFTP");
            }
        }
        this.sftpConnectionPool.returnObject(this.sftpSession);
    }

    public void establishChannel(String stepName) {
        try {
            ChannelSftp channelSftp = (ChannelSftp) this.sftpSession.openChannel("sftp");
            channelSftp.connect();
            if (!cdIntoDir(channelSftp, dBasePath)) {
                createRemoteFolder(channelSftp, dBasePath);
                cdIntoDir(channelSftp, dBasePath);
            }
            channelSftpMap.put(stepName, channelSftp);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    private ChannelSftp createRemoteFolder(ChannelSftp channelSftp, String remotePath) {
        logger.info("The remote path is {}", channelSftp.pwd());
        String[] folders = remotePath.split("/");
        for (String folder : folders) {
            logger.info(folder);
            if (!folder.isEmpty()) {
                boolean flag = true;
                try {
                    channelSftp.cd(folder);
                } catch (SftpException e) {
                    flag = false;
                }
                if (!flag) {
                    try {
                        channelSftp.mkdir(folder);
                        channelSftp.cd(folder);
                        flag = true;
                    } catch (SftpException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return channelSftp;
    }

    public boolean cdIntoDir(ChannelSftp channelSftp, String directory) {
        try {
            if (directory.isEmpty() || directory.equals(channelSftp.pwd())) {
                return true;
            } else {
                channelSftp.cd(directory);
                return true;
            }
        } catch (SftpException sftpException) {
            logger.warn("Could not cd into the directory we might have made {}", directory);
            return false;
        }
    }

    public OutputStream getStream(String fileName) {
        boolean appendMode = false;
        if (!channelSftpMap.containsKey(fileName)) {
            establishChannel(fileName);
        } else if (channelSftpMap.get(fileName).isClosed() || !channelSftpMap.get(fileName).isConnected()) {
            channelSftpMap.remove(fileName);
            appendMode = true;
            establishChannel(fileName);
        }
        this.channelSftp = this.channelSftpMap.get(fileName);
        try {
            if (appendMode) {
                return channelSftp.put(fileName, ChannelSftp.APPEND);
            } else {
                return channelSftp.put(fileName, ChannelSftp.OVERWRITE);
            }
        } catch (SftpException sftpException) {
            logger.warn("We failed getting the OuputStream to a file :(");
            sftpException.printStackTrace();
        }
        return null;
    }

    public ChannelSftp getChannelSftp(String currentThreadName, String fileName, int listSize) throws JSchException, SftpException, IOException {
        ChannelSftp channelSftp = null;
        if (!this.channelSftpMap.containsKey(currentThreadName)) {
            channelSftp = (ChannelSftp) this.sftpSession.openChannel("sftp");
            channelSftp.connect();
            channelSftp.setBulkRequests(listSize);
            if (!cdIntoDir(channelSftp, dBasePath)) {
                createRemoteFolder(channelSftp, dBasePath);
            }
            this.channelSftpMap.put(currentThreadName, channelSftp);
            return channelSftp;
        } else if (!this.channelSftpMap.get(currentThreadName).isConnected()) {
            channelSftp = this.channelSftpMap.get(currentThreadName);
            channelSftp.connect();
            this.channelSftpMap.put(currentThreadName, channelSftp);
            return channelSftp;
        } else {
            return this.channelSftpMap.get(currentThreadName);
        }
    }

    @SneakyThrows
    public ChannelSftp createStream(String fileName) {
        ChannelSftp channelSftp = (ChannelSftp) this.sftpSession.openChannel("sftp");
        channelSftp.connect();
        channelSftp.cd(this.dBasePath);
        return channelSftp;
    }

    public synchronized OutputStream getStream(String fileName, ChannelSftp channelSftp, long startPosition, long previousLength) throws SftpException {
        return channelSftp.put(fileName, null, ChannelSftp.RESUME, startPosition-previousLength);
    }

    @Override
    public void write(List<? extends DataChunk> items) throws IOException, SftpException {
        String fileName = items.get(0).getFileName();
        ChannelSftp channelSftp = createStream(fileName);
        for (DataChunk dataChunk : items) {
            logger.info("Writing {} with a total written of {}", dataChunk, totalSent);
            OutputStream stream = getStream(fileName, channelSftp, dataChunk.getStartPosition(), this.totalSent.longValue());
            stream.write(dataChunk.getData());
            stream.flush();
            this.totalSent.addAndGet(dataChunk.getSize());
            stream.close();
            logger.info(dataChunk.toString());
        }
        channelSftp.disconnect();
    }
}