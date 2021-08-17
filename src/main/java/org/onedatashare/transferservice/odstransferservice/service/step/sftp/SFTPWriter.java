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

    String stepName;
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
        this.stepName = stepExecution.getStepName();
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
    }

    @AfterStep
    public void afterStep() {
        for(ChannelSftp value: fileToChannel.values()){
            if(!value.isConnected()){
                value.disconnect();
            }
        }
    }

    public void establishChannel(String stepName){
        try {
            ChannelSftp channelSftp = SftpUtility.createConnection(jsch, destCred);
            assert channelSftp != null;
            if(!cdIntoDir(channelSftp, dBasePath)){
                mkdir(channelSftp, dBasePath);
            }
            if(fileToChannel.containsKey(stepName)){
                fileToChannel.remove(stepName);
            }
            fileToChannel.put(stepName, channelSftp);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public boolean cdIntoDir(ChannelSftp channelSftp, String directory){
        try {
            channelSftp.cd(directory);
            return true;
        } catch (SftpException sftpException) {
            logger.warn("Could not cd into the directory we might have made moohoo");
        }
        return false;
    }

    public boolean mkdir(ChannelSftp channelSftp, String basePath){
        try {
            channelSftp.mkdir(basePath);
            return true;
        } catch (SftpException sftpException) {
            logger.warn("Could not make the directory you gave us boohoo");
        }
        return false;
    }

    public OutputStream getStream(String stepName) {
        boolean appendMode = false;
        if(!fileToChannel.containsKey(stepName)){
            establishChannel(stepName);
        }else if(fileToChannel.get(stepName).isClosed() || !fileToChannel.get(stepName).isConnected()){
            fileToChannel.remove(stepName);
            appendMode = true;
            establishChannel(stepName);
        }
        ChannelSftp channelSftp = this.fileToChannel.get(stepName);
        try {
            if(appendMode){
                return channelSftp.put(stepName, ChannelSftp.APPEND);
            }
            return channelSftp.put(stepName, ChannelSftp.OVERWRITE);
        } catch (SftpException sftpException) {
            logger.warn("We failed getting the OuputStream to a file :(");
            sftpException.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(List<? extends DataChunk> items) {
        OutputStream destination = getStream(this.stepName);
        if(destination == null){
            logger.error("OutputStream is null....Not able to write : " + items.get(0).getFileName());
            establishChannel(stepName);
        }
        if (destination != null) {
            try {
                for (DataChunk b : items) {
                    logger.info("Current chunk in SFTP Writer " + b.toString());
                    destination.write(b.getData());
                    destination.flush();
                }
            } catch (IOException e) {
                logger.error("Error during writing chunks...exiting");
                e.printStackTrace();
            }
        }
    }
}