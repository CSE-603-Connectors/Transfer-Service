package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.jsch.SftpSessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterJob;
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
    private SftpSessionPool sftpConnectionPool;
    private Session session;

    public SFTPWriter(AccountEndpointCredential destCred) {
        fileToChannel = new HashMap<>();
        this.destCred = destCred;
    }

    public void setConnectionPool(SftpSessionPool connectionPool) {
        this.sftpConnectionPool = connectionPool;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException, JSchException {
        this.stepName = stepExecution.getStepName();
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.session = this.sftpConnectionPool.borrowObject();
    }

    @AfterStep
    public void afterStep() {
        for (ChannelSftp value : fileToChannel.values()) {
            if (!value.isConnected()) {
                value.disconnect();
            }
        }
        this.sftpConnectionPool.returnObject(this.session);
    }

    public void establishChannel(String stepName) {
        try {
            ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
            channelSftp.connect();
            if (!cdIntoDir(channelSftp, dBasePath)) {
                createRemoteFolder(channelSftp, dBasePath);
                cdIntoDir(channelSftp, dBasePath);
            }
            fileToChannel.put(stepName, channelSftp);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    private void createRemoteFolder(ChannelSftp channelSftp, String remotePath){
        logger.info("The remote path is {}", channelSftp.pwd());
        String[] folders = remotePath.split("/");
        for(String folder : folders){
            logger.info(folder);
            if(!folder.isEmpty()){
                boolean flag = true;
                try{
                    channelSftp.cd(folder);
                }catch(SftpException e){
                    flag = false;
                }
                if(!flag){
                    try{
                        channelSftp.mkdir(folder);
                        channelSftp.cd(folder);
                        flag = true;
                    }catch(SftpException ignored){
                        ignored.printStackTrace();
                    }
                }
            }
        }
    }

    public boolean cdIntoDir(ChannelSftp channelSftp, String directory) {
        try {
            if(directory.isEmpty() || directory.equals(channelSftp.pwd())){
                return true;
            }else{
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
                return channelSftp.put(fileName,ChannelSftp.APPEND);
            } else {
                return channelSftp.put(fileName,ChannelSftp.OVERWRITE);
            }
        } catch (SftpException sftpException) {
            logger.warn("We failed getting the OuputStream to a file :(");
            sftpException.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(List<? extends DataChunk> items) throws IOException {
        OutputStream destination = getStream(items.get(0).getFileName());
        for (DataChunk dataChunk : items) {
            logger.info(dataChunk.toString());
            destination.write(dataChunk.getData());
        }
        destination.flush();
    }
}