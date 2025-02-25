package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFileUploadSession;
import com.box.sdk.BoxFileUploadSessionPart;
import com.box.sdk.BoxFolder;
import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

/**
 * This class is responsible for writing to Box using the chunked upload approach & small file upload
 * Ideally we should separate this out I think.
 */
public class BoxWriterLargeFile implements ItemWriter<DataChunk> {

    private BoxAPIConnection boxAPIConnection;
    EntityInfo fileInfo;
    private HashMap<String, BoxFileUploadSession> fileMap;
    private HashMap<String, MessageDigest> digestMap;
    private List<BoxFileUploadSessionPart> parts;
    String destinationBasePath;
    BoxFolder boxFolder;
    Logger logger = LoggerFactory.getLogger(BoxWriterLargeFile.class);
    StepExecution stepExecution;
    @Setter
    private MetricsCollector metricsCollector;
    @Getter
    @Setter
    private MetricCache metricCache;

    private LocalDateTime readStartTime;

    public BoxWriterLargeFile(OAuthEndpointCredential oAuthDestCredential, EntityInfo fileInfo) {
        this.boxAPIConnection = new BoxAPIConnection(oAuthDestCredential.getToken());
        this.fileInfo = fileInfo;
        this.fileMap = new HashMap<>();
        this.digestMap = new HashMap<>();
        this.parts = new ArrayList<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destinationBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH); //path to place the files
        this.boxFolder = new BoxFolder(this.boxAPIConnection, this.destinationBasePath);
        this.stepExecution = stepExecution;
    }

    /**
     * Executes after we finish making all the write() calls
     * For large file uploads we upload the hash we compute and commit so Box constructs the file
     */
    @AfterStep
    public void afterStep() {
        BoxFileUploadSession session = this.fileMap.get(this.fileInfo.getId());
        MessageDigest messageDigest = this.digestMap.get(this.fileInfo.getId());
        session.commit(Base64.getEncoder().encodeToString(messageDigest.digest()), this.parts, new HashMap<>(), null, null);
    }

    /**
     * Simple method to prepare for chunked uploads.
     *
     * @param fileName
     * @throws NoSuchAlgorithmException
     */
    private void prepareForUpload(String fileName) throws NoSuchAlgorithmException {
        if (!ready(fileName)) {
            BoxFileUploadSession.Info session = this.boxFolder.createUploadSession(fileName, this.fileInfo.getSize());
            this.fileMap.put(fileName, session.getResource());
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
            this.digestMap.put(fileName, messageDigest);
        }
    }

    /**
     * Checker to see if we have already prepared for the current file transfer
     *
     * @param fileName
     * @return
     */
    private boolean ready(String fileName) {
        if (!this.fileMap.containsKey(fileName) || !this.digestMap.containsKey(fileName)) {
            return false;
        }
        return true;
    }

    /**
     * Here we implement both writing methods it could be better to actually use 2 separate writers instead to make the code cleaner
     * This way we just detect if we need small or large file uploads.
     * Small: just adds chunks to a Pri Queue in the smallFileUpload obj and that maintains the order then in after step we write
     * Large: For every part we upload we compute the hash and save it as well as the BoxParts so in after step we can commit upload
     *
     * @param items
     * @throws NoSuchAlgorithmException
     */
    @Override
    public void write(List<? extends DataChunk> items) throws NoSuchAlgorithmException {
        String fileName = items.get(0).getFileName();
        prepareForUpload(fileName);
        BoxFileUploadSession session = this.fileMap.get(fileName);
        MessageDigest digest = this.digestMap.get(fileName);
        for (DataChunk dataChunk : items) {
            BoxFileUploadSessionPart part = session.uploadPart(dataChunk.getData(), dataChunk.getStartPosition(), Long.valueOf(dataChunk.getSize()).intValue(), this.fileInfo.getSize());
            this.parts.add(part);
            digest.update(dataChunk.getData());
            logger.info("Current chunk in BoxLargeFile Writer " + dataChunk.toString());
        }
        this.digestMap.put(fileName, digest);
    }

    @BeforeRead
    public void beforeRead() {
        this.readStartTime = LocalDateTime.now();
        logger.info("Before write start time {}", this.readStartTime);
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        ODSConstants.metricsForOptimizerAndInflux(items, this.readStartTime, logger, stepExecution, metricCache, metricsCollector);
    }

}
