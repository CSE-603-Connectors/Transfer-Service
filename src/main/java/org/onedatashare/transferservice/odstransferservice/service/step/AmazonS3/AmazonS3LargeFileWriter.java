package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.AWSMultiPartMetaData;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class AmazonS3LargeFileWriter implements ItemWriter<DataChunk> {

    private final String bucketName;
    Logger logger = LoggerFactory.getLogger(AmazonS3LargeFileWriter.class);
    private AWSMultiPartMetaData metaData;
    EntityInfo fileInfo;
    private final AmazonS3 client;
    private String destBasepath;
    private boolean firstPass;
    StepExecution stepExecution;
    private LocalDateTime readStartTime;
    @Getter
    @Setter
    MetricsCollector metricsCollector; //this is for influxdb and for running pmeter
    @Getter
    @Setter
    private MetricCache metricCache; //this is for the optimizer
    private String uploadId;
    private String fileName;

    public AmazonS3LargeFileWriter(AccountEndpointCredential destCredential, EntityInfo fileInfo) {
        this.fileInfo = fileInfo;
        this.firstPass = false;
        String[] temp = destCredential.getUri().split(":::");
        this.bucketName = temp[1];
        String region = temp[0];
        AWSCredentials credentials = new BasicAWSCredentials(destCredential.getUsername(), destCredential.getSecret());
        this.client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("The S3LargeFileWriter has EntityInfo: " + this.fileInfo.toString());
        this.destBasepath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.stepExecution = stepExecution;
    }

    @BeforeRead
    public void beforeRead() {
        this.readStartTime = LocalDateTime.now();
    }

    public synchronized void prepareS3Transfer(String fileName) {
        if (!this.firstPass) {
            this.metaData = new AWSMultiPartMetaData();
            String key = Paths.get(this.destBasepath, fileName).toString();
            this.metaData.prepareMetaData(client, this.bucketName, key);
            this.uploadId = this.metaData.getInitiateMultipartUploadResult().getUploadId();
            this.firstPass = true;
            this.fileName = key;
        }
    }


    @Override
    public void write(List<? extends DataChunk> items) {
        if (!this.firstPass) {
            prepareS3Transfer(items.get(0).getFileName());
        }
        for (DataChunk currentChunk : items) {
            logger.info(currentChunk.toString());
            UploadPartRequest uploadPartRequest;
            if (currentChunk.getStartPosition() + currentChunk.getSize() == this.fileInfo.getSize()) {
                uploadPartRequest = ODSUtility.makePartRequest(currentChunk, this.bucketName, this.metaData.getInitiateMultipartUploadResult().getUploadId(), this.fileName, true);
            } else {
                uploadPartRequest = ODSUtility.makePartRequest(currentChunk, this.bucketName, this.uploadId, this.fileName, false);
            }
            UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
            this.metaData.addUploadPart(uploadPartResult);
        }
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        ODSConstants.metricsForOptimizerAndInflux(items, this.readStartTime, logger, stepExecution, metricCache, metricsCollector);
    }

    @AfterStep
    public void afterStep() {
        this.metaData.completeMultipartUpload(client);
        this.metaData.reset();
    }

}