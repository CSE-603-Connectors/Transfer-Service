package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpClientFactory;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.InputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class FTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);
    InputStream inputStream;
    String sBasePath;
    String fName;
    AccountEndpointCredential sourceCred;
    int chunckSize;
    FilePartitioner partitioner;
    EntityInfo fileInfo;

    public FTPReader(AccountEndpointCredential credential, EntityInfo file, int chunckSize) {
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
        fName = fileInfo.getId();
        this.partitioner.createParts(this.fileInfo.getSize(), this.fName);
    }

    @AfterStep
    public void afterStep() {
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }


    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        FilePart filePart = this.partitioner.nextPart();
        if (filePart == null) return null;
        byte[] data = new byte[filePart.getSize()];
        int totalBytes = 0;
        while (totalBytes < filePart.getSize()) {
            int byteRead = this.inputStream.read(data, totalBytes, filePart.getSize() - totalBytes);
            if (byteRead == -1) return null;
            totalBytes += byteRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(totalBytes, data, filePart.getStart(), (int) filePart.getPartIdx(), this.fName);
        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() {
        clientCreateSourceStream(sBasePath, this.fileInfo.getPath());
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
    }

    @SneakyThrows
    public void clientCreateSourceStream(String basePath, String fName) {
        logger.info("Inside clientCreateSourceStream for : " + fName + " ");

        //***GETTING STREAM USING APACHE COMMONS VFS2
        FileSystemOptions opts = FtpUtility.generateOpts();
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, this.sourceCred.getUsername(), this.sourceCred.getSecret());
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
        String wholeThing;

        if (this.sourceCred.getUri().contains("ftp://")) {
            wholeThing = this.sourceCred.getUri() + "/" + basePath + fileInfo.getPath();
        } else {
            wholeThing = "ftp://" + this.sourceCred.getUri() + "/" + basePath + fileInfo.getPath();
        }
        this.inputStream = VFS.getManager().resolveFile(wholeThing, opts).getContent().getInputStream();
    }

}
