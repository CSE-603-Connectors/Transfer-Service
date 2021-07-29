package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.*;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.GDriveUtility;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class GDriveWriter implements ItemWriter<DataChunk> {

    private static final int MINIMUM_CHUNK_SIZE = 524288;
    Logger logger = LoggerFactory.getLogger(GDriveWriter.class);
    private final AccountEndpointCredential destCredential;
    Drive gdriveClient;
    private AWSMultiPartMetaData metaData;
    private AWSSinglePutRequestMetaData singlePutRequestMetaData;
    String fileName;
    EntityInfo fileInfo;
    long currentFileSize;

    public GDriveWriter(AccountEndpointCredential destCredential, EntityInfo fileInfo){
        this.fileName = "";
        this.fileInfo = fileInfo;
        this.destCredential = destCredential;
        try {
            this.gdriveClient = GDriveUtility.constructClient(this.destCredential);
        } catch (IOException | GeneralSecurityException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(List<? extends DataChunk> list) throws Exception {
        //System.out.println("I am here");
        File fileMetadata = new File();
        fileMetadata.setName("test_test_test.pdf");
        java.io.File filePath = new java.io.File(fileInfo.getPath());
        InputStreamContent mediaContent =
                new InputStreamContent("application/pdf",
                        new BufferedInputStream(new FileInputStream(filePath)));
        mediaContent.setLength(filePath.length());
        Drive.Files.Create request = this.gdriveClient.files().create(fileMetadata, mediaContent);
        request.getMediaHttpUploader().setProgressListener(new CustomProgressListener());
        request.getMediaHttpUploader().setChunkSize(MINIMUM_CHUNK_SIZE);
        request.execute();

    }
}

class CustomProgressListener implements MediaHttpUploaderProgressListener {
    public void progressChanged(MediaHttpUploader uploader) throws IOException {
        switch (uploader.getUploadState()) {
            case INITIATION_STARTED:
                System.out.println("Initiation has started!");
                break;
            case INITIATION_COMPLETE:
                System.out.println("Initiation is complete!");
                break;
            case MEDIA_IN_PROGRESS:
                System.out.printf("Upload in progress: %f %% uploaded \n", uploader.getProgress()*100);
//                System.out.println(uploader.getProgress());
                break;
            case MEDIA_COMPLETE:
                System.out.println("Upload is complete!");
        }
    }
}