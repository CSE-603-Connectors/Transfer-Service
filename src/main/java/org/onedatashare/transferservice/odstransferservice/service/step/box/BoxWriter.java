package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFileUploadSession;
import com.box.sdk.BoxFileUploadSessionPart;
import com.box.sdk.BoxFolder;
import org.onedatashare.transferservice.odstransferservice.model.BoxMultiPartUpload;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SinglePutReqeust;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_ID;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.FIFTY_MB;

public class BoxWriter implements ItemWriter<DataChunk> {

    private OAuthEndpointCredential credential;
    private BoxAPIConnection boxAPIConnection;
    EntityInfo fileInfo;
    private HashMap<String, BoxFileUploadSession> sessionMap;
    private HashMap<String, BoxMultiPartUpload> multiPartUploadHashMap;
    String parentFolderId;
    BoxFolder boxFolder;
    boolean chunkedUpload;
    SinglePutReqeust onePut;
    String fileName;
    BoxMultiPartUpload multiPartUpload;

    Logger logger = LoggerFactory.getLogger(BoxWriter.class);

    public BoxWriter(OAuthEndpointCredential oauthDestCredential, EntityInfo fileInfo) {
        this.credential = oauthDestCredential;
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.fileInfo = fileInfo;
        this.sessionMap = new HashMap<>();
        multiPartUpload = new BoxMultiPartUpload();
        this.multiPartUploadHashMap = new HashMap<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.parentFolderId = stepExecution.getJobParameters().getString(DEST_BASE_ID);
        logger.info(this.parentFolderId);
        this.boxFolder = new BoxFolder(this.boxAPIConnection, this.parentFolderId);//for now hard coded to 0 which is the root of the box account
        if (this.fileInfo.getSize() < FIFTY_MB) {
            chunkedUpload = false;
            onePut = new SinglePutReqeust();
        } else {
            chunkedUpload = true;
            multiPartUpload.prepare(this.fileInfo.getSize());
        }
    }

    @AfterStep
    public void afterStep() {
        if (this.chunkedUpload) {
            BoxFileUploadSession session = this.sessionMap.remove(this.fileName);
            this.multiPartUpload.finishUpload(session);
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) {
        this.fileName = items.get(0).getFileName();
        if (!chunkedUpload) {
            onePut.addAllChunks(items);
            logger.info("Doing one chunk upload");
            this.boxFolder.uploadFile(onePut.condenseListToOneStream(this.fileInfo.getSize()), fileName);
        } else {
            for (DataChunk dataChunk : items) {
                if (!this.sessionMap.containsKey(dataChunk.getFileName())) {
                    BoxFileUploadSession.Info boxInfo = this.boxFolder.createUploadSession(dataChunk.getFileName(), this.fileInfo.getSize());
                    this.sessionMap.put(dataChunk.getFileName(), boxInfo.getResource());
                    this.multiPartUpload.addPart(boxInfo.getResource(), dataChunk);
                } else {
                    this.multiPartUpload.addPart(this.sessionMap.get(dataChunk.getFileName()), dataChunk);
                }
            }
        }
    }
}
