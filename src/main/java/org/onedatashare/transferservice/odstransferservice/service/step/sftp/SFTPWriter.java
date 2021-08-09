package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
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

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

    private String dBasePath;
    AccountEndpointCredential destCred;
    HashMap<String, ChannelSftp> fileToChannel;
    JSch jsch;

    public SFTPWriter(AccountEndpointCredential destCred) {
        fileToChannel = new HashMap<>();
        this.destCred = destCred;
        jsch = new JSch();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
    }

    @AfterStep
    public void afterStep() {
        for (ChannelSftp value : fileToChannel.values()) {
            if (!value.isConnected()) {
                value.disconnect();
            }
        }
    }
    public void establishChannel(String fileName) {
        try {
            ChannelSftp channelSftp = SftpUtility.createConnection(jsch, destCred);
            assert channelSftp != null;
            if (!cdIntoDir(channelSftp, dBasePath)) {
                mkdir(channelSftp, dBasePath);
            }
            if (fileToChannel.containsKey(fileName)) {
                fileToChannel.remove(fileName);
            }
            fileToChannel.put(fileName, channelSftp);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public boolean cdIntoDir(ChannelSftp channelSftp, String directory) {
        try {
            channelSftp.cd(directory);
            return true;
        } catch (SftpException sftpException) {
            logger.warn("Could not cd into the directory we might have made moohoo");
            sftpException.printStackTrace();
        }
        return false;
    }

    public boolean mkdir(ChannelSftp channelSftp, String basePath) {
        try {
            channelSftp.mkdir(basePath);
            return true;
        } catch (SftpException sftpException) {
            logger.warn("Could not make the directory you gave us boohoo");
            sftpException.printStackTrace();
        }
        return false;
    }

    public OutputStream getStream(String fileName) {
        boolean appendMode = false;
        if (!fileToChannel.containsKey(fileName)) {
            establishChannel(fileName);
        } else if (fileToChannel.get(fileName).isClosed() || !fileToChannel.get(fileName).isConnected()) {
            fileToChannel.remove(fileName);
            appendMode = true;
            establishChannel(fileName);
        }
        ChannelSftp channelSftp = this.fileToChannel.get(fileName);
        try {
            if (appendMode) {
                return channelSftp.put(fileName, ChannelSftp.APPEND);
            }
            return channelSftp.put(fileName, ChannelSftp.OVERWRITE);
        } catch (SftpException sftpException) {
            logger.warn("We failed getting the OuputStream to a file :(");
            sftpException.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(List<? extends DataChunk> items) throws IOException {
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            OutputStream tap = getStream(chunk.getFileName());
            if (tap == null) {
                establishChannel(chunk.getFileName());
                tap = getStream(chunk.getFileName());
            }
            tap.write(chunk.getData());
            if (items.size() - 1 == i) { //last chunk we on needs to flush as thats the end of the pipe
                tap.flush();
            }
        }
    }
}