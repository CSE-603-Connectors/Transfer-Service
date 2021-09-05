package org.onedatashare.transferservice.odstransferservice.service.step.sftp;


import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.service.pools.SSHJSessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class SshJWriter implements ItemWriter<DataChunk> {


    private SSHJSessionPool connectPool;
    private SSHClient client;
    private SFTPClient sftpClient;
    private String destinationPath;
    private RemoteFile remoteFile;
    HashMap<String, RemoteFile> fileNameToRemoteFile;
    Logger logger = LoggerFactory.getLogger(SshJWriter.class);

    public SshJWriter() {
        this.fileNameToRemoteFile = new HashMap<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException, IOException {
        this.client = this.connectPool.borrowObject();
        if (!this.client.isConnected() || !this.client.isAuthenticated()) {
            logger.info("There is a problem with the client not being connected or authenticated");
        }
        this.sftpClient = this.client.newSFTPClient();
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        logger.info("The remote path to dump data in is {}", this.destinationPath);
        this.sftpClient.mkdirs(destinationPath);
    }

    @AfterStep
    public void afterStep() {
        for (String key : this.fileNameToRemoteFile.keySet()) {
            try {
                this.fileNameToRemoteFile.get(key).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.fileNameToRemoteFile.remove(key);
        }
        this.connectPool.returnObject(this.client);
    }

    public RemoteFile getRemoteFile(String pathToFile) {
        if(this.remoteFile != null){
            return this.remoteFile;
        }else{
            try {
                this.remoteFile = this.sftpClient.open(pathToFile, EnumSet.of(OpenMode.WRITE));
                return this.remoteFile;
            } catch (IOException e) {
                logger.error("Failed to open the file with just write permissions will be creating, and writing {}", pathToFile);
            }
            try {
                this.remoteFile = this.sftpClient.open(pathToFile, EnumSet.of(OpenMode.CREAT, OpenMode.WRITE));
                return this.remoteFile;
            } catch (IOException e) {
                logger.error("failed to create the file with the path {}", pathToFile);
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void write(List<? extends DataChunk> items) throws IOException {
        Path path = Paths.get(this.destinationPath, items.get(0).getFileName());
        RemoteFile remoteFile = getRemoteFile(path.toString());
        for (DataChunk chunk : items) {
            logger.info("The path we are writing to is {}", path);
            remoteFile.write(chunk.getStartPosition(), chunk.getData(), 0, chunk.getData().length);
            logger.info("The current remote file is {}", remoteFile);
            logger.info("The current chunk we are writing to the remote file is {}", chunk);
        }
    }

    public void setConnectionPool(SSHJSessionPool sshjWriterPool) {
        this.connectPool = sshjWriterPool;
    }
}
