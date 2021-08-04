package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import org.onedatashare.transferservice.odstransferservice.config.GDriveConfig;
import org.onedatashare.transferservice.odstransferservice.model.*;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class GDriveWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(GDriveWriter.class);
    HashMap<String, File> fileMap;
    private final OAuthEndpointCredential destCredential;
    Drive gdriveClient;
    String fileName;
    EntityInfo fileInfo;
    String basePath;
    private String mimeType;
    GDriveConfig config;

    public GDriveWriter(OAuthEndpointCredential destCredential, EntityInfo fileInfo) {
        this.fileName = "";
        this.fileInfo = fileInfo;
        this.destCredential = destCredential;
        config = GDriveConfig.getInstance();
        fileMap = new HashMap<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        this.gdriveClient = config.getDriveService(this.destCredential);
        basePath = stepExecution.getJobExecution().getJobParameters().getString(DEST_BASE_PATH);
        this.mimeType = URLConnection.guessContentTypeFromName(fileInfo.getPath());

    }

    @Override
    public void write(List<? extends DataChunk> list) throws Exception {
        File fileMetadata = new File();
        for(DataChunk chunk : list){

        }
        fileMetadata.setName("test_test_test.pdf");
        java.io.File filePath = new java.io.File(fileInfo.getPath());
        InputStreamContent mediaContent =
                new InputStreamContent(mimeType,
                        new BufferedInputStream(new FileInputStream(filePath)));
        mediaContent.setLength(filePath.length());
        Drive.Files.Create request = this.gdriveClient.files().create(fileMetadata, mediaContent);
        request.getMediaHttpUploader().setProgressListener(new CustomProgressListener());
        request.getMediaHttpUploader().setChunkSize(FIVE_MB);
        request.execute();

    }

    public void openConnection() {

    }

    public void closeConnection() {

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