package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.services.drive.Drive;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.onedatashare.transferservice.odstransferservice.config.GDriveConfig;

import java.io.*;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class GDriveReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(GDriveReader.class);
    private static final int MINIMUM_CHUNK_SIZE = 524288;
    private final OAuthEndpointCredential sourceCredential;
    private int chunkSize;
    private final FilePartitioner partitioner;
    Drive gdriveClient;
    String fileName;
    private String sourcePath;
    EntityInfo fileInfo;
    private Drive.Files.Get getSkeleton;

    public GDriveReader(OAuthEndpointCredential sourceCredential, int chunkSize, EntityInfo fileInfo) {
        this.sourceCredential = sourceCredential;
        this.fileInfo = fileInfo;
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        this.partitioner = new FilePartitioner(this.chunkSize);
        this.setName(ClassUtils.getShortName(GDriveReader.class));
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.sourcePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.SOURCE_BASE_PATH);
        logger.info("Starting the job for this file: " + this.fileName);
        partitioner.createParts(this.chunkSize, fileInfo.getId());
    }

    @Override
    protected DataChunk doRead() throws Exception {
        FilePart part = partitioner.nextPart();
        String[] rangeStr = ODSUtility.filePartToRange(part).toString().split(":");
        this.getSkeleton.set(rangeStr[0]+":", rangeStr[1]);
        ByteArrayOutputStream data = new ByteArrayOutputStream(part.getSize());
        this.getSkeleton.executeMediaAndDownloadTo(data);
        return ODSUtility.makeChunk(data.size(), data.toByteArray(), part.getStart(), Long.valueOf(part.getPartIdx()).intValue(), fileInfo.getId());
    }

    @Override
    protected void doOpen() throws Exception {
        GDriveConfig config = GDriveConfig.getInstance();
        this.gdriveClient = config.getDriveService(this.sourceCredential);
        Drive.Files.Get getRequest = this.gdriveClient.files().get(fileInfo.getId());
        this.getSkeleton = getRequest;
    }

    @Override
    protected void doClose() {
        this.gdriveClient = null;
        this.getSkeleton.clear();
    }

}
