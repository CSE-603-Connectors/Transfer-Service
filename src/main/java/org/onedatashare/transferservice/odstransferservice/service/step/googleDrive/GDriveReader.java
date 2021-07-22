package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
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
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.utility.GDriveUtility;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.onedatashare.transferservice.odstransferservice.utility.S3Utility;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class GDriveReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(AmazonS3Reader.class);
    private final AccountEndpointCredential sourceCredential;
    private int chunkSize;
    private final FilePartitioner partitioner;
    Drive gdriveClient;
    String fileName;
    private String sourcePath;
    File getSkeleton;
    String getMime;

    public GDriveReader(AccountEndpointCredential sourceCredential) {
        this.sourceCredential = sourceCredential;
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        this.partitioner = new FilePartitioner(this.chunkSize);
        this.setName(ClassUtils.getShortName(GDriveReader.class));
        try {
            this.gdriveClient = GDriveUtility.constructClient(this.sourceCredential);
        } catch (IOException | GeneralSecurityException e) {
            e.printStackTrace();
        }
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = stepExecution.getStepName();//For an S3 Reader job this should be the object key
        this.sourcePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.SOURCE_BASE_PATH);
//        this.amazonS3URI = new AmazonS3URI(S3Utility.constructS3URI(this.sourceCredential.getUri(), this.fileName, this.sourcePath));
//        this.getSkeleton = new GetObjectRequest(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        try {
            this.getSkeleton = this.gdriveClient.files().get(fileName).execute();
            this.getMime = this.getSkeleton.getMimeType();
            System.out.println("Size: " + this.getSkeleton.getSize());
            System.out.println("Description: " + this.getSkeleton.getProperties());
            System.out.println("MIME type: " + this.getSkeleton.getMimeType());


        } catch (IOException e) {
            System.out.println("An error occurred: " + e);
        }
        logger.info("Starting the job for this file: " + this.fileName);
    }

    @Override
    public void setResource(Resource resource) {

    }


    @Override
    protected DataChunk doRead() throws Exception {
        return null;
    }

    @Override
    protected void doOpen() throws Exception {

    }

//    @Override
//    protected DataChunk doRead() throws Exception {
//        FilePart part = partitioner.nextPart();
//        if (part == null) return null;
//        logger.info("Current Part:-"+part.toString());
//        // InputStream stream =
//        this.gdriveClient.files().get(this.fileName).getRequestHeaders().setRange()
//        InputStream partOfFile = this.gdriveClient.files().export(this.getSkeleton.getId(), this.getMime).executeMediaAsInputStream();
////        Drive partOfFile = this.gdriveClient.files().get(this.getSkeleton.getId()) .setFields( (part.getStart(), part.getEnd()));//this is inclusive or on both start and end so take one off so there is no colision
////        Drive partOfFile = this.gdriveClient.files().get({fileId: this.getSkeleton.getId(), headers: {"Range": "bytes=500-999" }});
//        //this is inclusive or on both start and end so take one off so there is no colision
////        { fileId: fileId, alt: 'media', headers: { "Range": "bytes=500-999" } },
//        byte[] dataSet = new byte[part.getSize()];
//        partOfFile.readNBytes()
//        long totalBytes = 0;
//        //  S3ObjectInputStream stream = partOfFile.getObjectContent();
//        while (totalBytes < part.getSize()) {
//            int bytesRead = 0;
//            bytesRead += stream.read(dataSet, Long.valueOf(totalBytes).intValue(), Long.valueOf(part.getSize()).intValue());
//            if (bytesRead == -1) return null;
//            totalBytes += bytesRead;
//        }
//        stream.close();
//        return ODSUtility.makeChunk(part.getSize(), dataSet, part.getStart(), Long.valueOf(part.getPartIdx()).intValue(), this.fileName);
//    }
//
//    @Override
//    protected void doOpen() throws Exception {
//        logger.info(this.amazonS3URI.toString());
//        this.currentFileMetaData = this.s3Client.getObjectMetadata(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
//        partitioner.createParts(this.currentFileMetaData.getContentLength(), this.fileName);
//    }


    @Override
    protected void doClose() throws Exception {

    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
//    private static final String APPLICATION_NAME = "Google Drive API Java Quickstart";
//    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
//    private static final String TOKENS_DIRECTORY_PATH = "tokens";
//    private static final String destinationFolder = "C:\\Users\\Devya Singh\\Downloads\\authtestdownloads\\";
//    /**
//     * Global instance of the scopes required by this quickstart.
//     * If modifying these scopes, delete your previously saved tokens/ folder.
//     */
//    //private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE_METADATA_READONLY);
//    private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);
//    //['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'];
//    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
//    //private static final String CREDENTIALS_FILE_PATH = "/Downloads/client_secret_742926411922-npv1mg1fq5meb2hvqjk6cpf9hddeksp3.apps.googleusercontent.com";
//    /**
//     * Creates an authorized Credential object.
//     * @param HTTP_TRANSPORT The network HTTP Transport.
//     * @return An authorized Credential object.
//     * @throws IOException If the credentials.json file cannot be found.
//     */
//    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
//        // Load client secrets.
//        InputStream in = GDriveReader.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
//        if (in == null) {
//            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
//        }
//        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
//        // Build flow and trigger user authorization request.
//        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
//                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
//                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
//                .setAccessType("offline")
//                .build();
//        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
//        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
//    }
//    public static void main(String... args) throws IOException, GeneralSecurityException {
//        extracted();
//    }
//
//    private static void extracted() throws GeneralSecurityException, IOException {
//        // Build a new authorized API client service.
//        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
//        //System.out.println("HTTP TRANSPORT %s", HTTP_TRANSPORT.toString());
//        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
//                .setApplicationName(APPLICATION_NAME)
//                .build();
//        // Print the names and IDs for up to 10 files.
//
//        FileList result = service.files().list()
//                .setQ("name contains 'Arlington'")
//                .setPageSize(10)
//                .setFields("nextPageToken, files(id, name, kind)")  //The setField for Drive API is used for partial responses, it will depend on what data you want that will be part of the returned object.
//                .execute();
//        List<File> files = result.getFiles();
//        if (files == null || files.isEmpty()) {
//            System.out.println("No files found.");
//        } else {
//            System.out.println("Files:");
//            for (File file : files) {
//                System.out.printf("%s (%s) %s\n", file.getName(), file.getId(), file.getKind()); // for setFields https://developers.google.com/drive/api/v3/reference/files
//                OutputStream outputStream = new FileOutputStream(destinationFolder + file.getName());
////                        new ByteArrayOutputStream();
//                service.files().get(file.getId())
//                        .executeMediaAndDownloadTo(outputStream);
//                outputStream.flush();
//                outputStream.close();
////                service.files().export(file.getId(), "application/pdf")
////                        .executeMediaAndDownloadTo(outputStream);
//            }
//        }
//    }


}

