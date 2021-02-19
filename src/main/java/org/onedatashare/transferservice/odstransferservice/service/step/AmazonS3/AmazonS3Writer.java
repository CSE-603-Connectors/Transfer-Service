package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.onedatashare.transferservice.odstransferservice.model.AWSMultiPartMetaData;
import org.onedatashare.transferservice.odstransferservice.model.AWSSinglePutRequestMetaData;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.onedatashare.transferservice.odstransferservice.utility.S3Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


public class AmazonS3Writer implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(AmazonS3Writer.class);
    private final AccountEndpointCredential destCredential;
    AmazonS3URI s3URI;
    HashMap<String, AmazonS3> clientHashMap;//Must use multiple S3 clients b/c changing the region is not thread safe and will create race conditions as per aws java docs in AmazonS3.java
    private AWSMultiPartMetaData metaData;
    private AWSSinglePutRequestMetaData singlePutRequestMetaData;
    boolean multipartUpload;
    String fileName;
    EntityInfo fileInfo;
    long currentFileSize;

    public AmazonS3Writer(AccountEndpointCredential destCredential, EntityInfo fileInfo){
        this.fileName = fileInfo.getId();
        this.fileInfo = fileInfo;
        this.destCredential = destCredential;
        this.clientHashMap = new HashMap<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
//        this.currentFileSize = Long.parseLong(stepExecution.getExecutionContext().getString(this.fileName));
        logger.info("Before Step of AmazonS3Writer and the step name is {}", stepExecution.getStepName());
        this.currentFileSize = this.fileInfo.getSize();
        String destBasepath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.fileName = stepExecution.getStepName();
        this.s3URI = new AmazonS3URI(S3Utility.constructS3URI(this.destCredential, this.fileName, destBasepath));//for aws the step name will be the file key.
        createClientWithCreds();
        if(this.currentFileSize < FIVE_MB){
            this.multipartUpload = false;
            this.singlePutRequestMetaData = new AWSSinglePutRequestMetaData();
        }else{
            this.multipartUpload = true;
            this.metaData = new AWSMultiPartMetaData();
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        AmazonS3 client = this.clientHashMap.get(this.fileName);
        if(!this.multipartUpload){
            this.singlePutRequestMetaData.addAllChunks(items);
            DataChunk lastChunk = items.get(items.size()-1);
            if(lastChunk.getStartPosition() + lastChunk.getSize() == this.currentFileSize){
                PutObjectRequest putObjectRequest = new PutObjectRequest(this.s3URI.getBucket(), this.s3URI.getKey(), this.singlePutRequestMetaData.condenseListToOneStream(this.currentFileSize), makeMetaDataForSinglePutRequest(this.currentFileSize));
                client.putObject(putObjectRequest);
            }
        }else{
            if(!this.metaData.isPrepared()){
                this.metaData.prepareMetaData(client, this.s3URI.getBucket(), this.s3URI.getKey());
            }
            for(DataChunk currentChunk : items){
                logger.info("The current chunk is {}, with size {} and the start Position is {}", currentChunk.getChunkIdx(), currentChunk.getSize(), currentChunk.getStartPosition());
                if(currentChunk.getStartPosition() + currentChunk.getSize() == this.currentFileSize){
                    logger.info("At the last chunk of the transfer {}", currentChunk.getChunkIdx());
                    this.metaData.addUploadPart(client.uploadPart(ODSUtility.makePartRequest(currentChunk, this.s3URI.getBucket(), this.metaData.getInitiateMultipartUploadResult().getUploadId(), this.s3URI.getKey(), true)));
                    this.metaData.completeMultipartUpload(this.clientHashMap.get(this.fileName));
                }else{
                    UploadPartRequest uploadPartRequest = ODSUtility.makePartRequest(currentChunk, this.s3URI.getBucket(), this.metaData.getInitiateMultipartUploadResult().getUploadId(), this.s3URI.getKey(), false);
                    UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
                    this.metaData.addUploadPart(uploadPartResult);
                }
            }
        }
    }

    @AfterStep
    public void afterStep(){
        this.clientHashMap.remove(this.fileName);
        if(this.multipartUpload){
            this.metaData.reset();
        }
    }

    public void createClientWithCreds(){
        if(this.clientHashMap.containsKey(this.fileName)){
            this.clientHashMap.get(this.fileName);
        }else{
            logger.info("Creating credentials for {}", this.destCredential.getUsername());
            AWSCredentials credentials = new BasicAWSCredentials(this.destCredential.getUsername(), this.destCredential.getSecret());
            this.clientHashMap.put(this.fileName, AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(this.s3URI.getRegion())
                    .build());
        }
    }

    public ObjectMetadata makeMetaDataForSinglePutRequest(long size){
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(size);
        return objectMetadata;
    }


}

