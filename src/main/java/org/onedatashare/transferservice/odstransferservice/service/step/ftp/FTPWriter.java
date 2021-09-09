package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.pools.FTPConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class FTPWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(FTPWriter.class);

    String stepName;
    private String dBasePath;
    AccountEndpointCredential destCred;
    FileObject foDest;
    private StaticUserAuthenticator auth;
    private RandomAccessContent randomAccessFile;
    private FTPConnectionPool connectionPool;

    public FTPWriter(AccountEndpointCredential destCred) {

        this.destCred = destCred;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Inside FTP beforeStep");
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        stepName = stepExecution.getStepName();
        this.auth = new StaticUserAuthenticator(null, this.destCred.getUsername(), this.destCred.getSecret());
        ftpDest();
    }

    @AfterStep
    public void afterStep() {
        try {
            this.randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ftpDest() {
        logger.info("Creating ftpDest for :" + this.stepName);

        try {
            FileSystemOptions opts = FtpUtility.generateOpts();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
            String wholeThing;
            if (!dBasePath.endsWith("/")) dBasePath += "/";
            if (this.destCred.getUri().contains("ftp://")) {
                wholeThing = this.destCred.getUri() + "/" + dBasePath + this.stepName;
            } else {
                wholeThing = "ftp://" + this.destCred.getUri() + "/" + dBasePath + this.stepName;
            }
            foDest = VFS.getManager().resolveFile(wholeThing, opts);
            foDest.createFile();
            this.randomAccessFile = foDest.getContent().getRandomAccessContent(RandomAccessMode.READWRITE);
        } catch (Exception ex) {
            logger.error("Error in setting ftp connection...");
            ex.printStackTrace();
        }
    }

    public synchronized void writeInDataChunk(DataChunk dataChunk) throws IOException {
        this.randomAccessFile.seek(dataChunk.getStartPosition());
        this.randomAccessFile.write(dataChunk.getData());
    }

    public void write(List<? extends DataChunk> list) throws IOException {
        logger.info("Inside Writer---writing chunk of : " + list.get(0).getFileName());
        for(DataChunk dataChunk: list) writeInDataChunk(dataChunk);
    }

    public void setConnectionBag(FTPConnectionPool pool) {
        this.connectionPool = pool;
    }
}
