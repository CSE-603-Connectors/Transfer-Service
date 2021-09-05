package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.pools.SSHJSessionPool;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.IOException;

public class SshJReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    private final int chunkSize;
    private final EntityInfo fileInfo;
    private final FilePartitioner filePartitioner;
    private SSHJSessionPool connectionPool;
    private RemoteFile remoteFile;
    private SSHClient client;
    Logger logger = LoggerFactory.getLogger(SshJReader.class);

    public SshJReader(int chunkSize, EntityInfo fileInfo) {
        this.fileInfo = fileInfo;
        this.chunkSize = chunkSize;
        filePartitioner = new FilePartitioner(chunkSize);
        this.setName(ClassUtils.getShortName(SshJReader.class));
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
//        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
//        logger.info(sBasePath);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
    }

    @Override
    protected DataChunk doRead() throws Exception {
        FilePart part = this.filePartitioner.nextPart();
        if (part == null) return null;
        logger.info("Part to read {}", part);
        byte[] buffer = new byte[part.getSize()];
        int totalBytes = 0;
        while (totalBytes < part.getSize()) {
            int amountRead = this.remoteFile.read(part.getStart(), buffer, 0, part.getSize());
            totalBytes += amountRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(totalBytes, buffer, part.getStart(), Long.valueOf(part.getPartIdx()).intValue(), this.fileInfo.getId());
        logger.info("Read in {}", chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() throws IOException, InterruptedException {
        this.client = this.connectionPool.borrowObject();
        this.remoteFile = client.newSFTPClient().open(this.fileInfo.getPath());
    }

    @Override
    protected void doClose() throws Exception {
        this.remoteFile.close();
        this.connectionPool.returnObject(this.client);
    }

    public void setConnectionPool(SSHJSessionPool sshjReaderPool) {
        this.connectionPool = sshjReaderPool;
    }
}
