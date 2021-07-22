package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.client.http.FileContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import junit.framework.TestCase;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.utility.GDriveUtility;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.List;

import static org.junit.Assert.*;

public class GDriveReaderTest extends TestCase implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {
//    GDriveUtility testObj;
    GDriveReader testObj;
    private String destinationFolder = "C:\\Users\\Devya Singh\\Downloads\\";

    public AccountEndpointCredential createTestCredentials() {

        AccountEndpointCredential accountEndpointCredential = new AccountEndpointCredential();
        accountEndpointCredential.setUri("http://localhost");
        accountEndpointCredential.setUsername("742926411922-npv1mg1fq5meb2hvqjk6cpf9hddeksp3.apps.googleusercontent.com:::myproject1-317703");
        accountEndpointCredential.setSecret("LAx91ZuLhDKnFAN1LvtTdcxt");
        return accountEndpointCredential;

    }

    public void testCreateClient() throws GeneralSecurityException, IOException {
//        testObj = new GDriveUtility();
        testObj = new GDriveReader(createTestCredentials());

//        Drive testservice = GDriveUtility.constructClient(createTestCredentials());
        Assert.isTrue(testObj != null, "The client is null somehow");
        // Print the names and IDs for up to 10 files.
        FileList result = testObj.gdriveClient.files().list()
//        FileList result = testservice.files().list()
                .setQ("name contains 'Arlington'")
                .setPageSize(10)
                .setFields("nextPageToken, files(id, name, kind)")  //The setField for Drive API is used for partial responses, it will depend on what data you want that will be part of the returned object.
                .execute();
        List<File> files = result.getFiles();
        if (files == null || files.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
//            for (File file : files) {
//                System.out.printf("%s (%s) %s\n", file.getName(), file.getId(), file.getKind()); // for setFields https://developers.google.com/drive/api/v3/reference/files
////                OutputStream outputStream = new FileOutputStream(destinationFolder + file.getName());
//////                        new ByteArrayOutputStream();
////                service.files().get(file.getId())
////                        .executeMediaAndDownloadTo(outputStream);
////                outputStream.flush();
////                outputStream.close();
////                service.files().export(file.getId(), "application/pdf")
////                        .executeMediaAndDownloadTo(outputStream);
//            }

            for (File file : files) {
                System.out.printf("%s (%s) %s %s\n", file.getName(), file.getId(), file.getMimeType(), file.getFileExtension()); // for setFields https://developers.google.com/drive/api/v3/reference/files
                OutputStream outputStream = new FileOutputStream(destinationFolder + file.getName());
//                        new ByteArrayOutputStream();
                testObj.gdriveClient.files().get(file.getId())
                        .executeMediaAndDownloadTo(outputStream);
                outputStream.flush();
                outputStream.close();
                File fileMetadata = new File();
                fileMetadata.setName("NP-Completeness_NA_print.pdf");
                java.io.File filePath = new java.io.File("C:\\Users\\Devya Singh\\Downloads\\NP-Completeness_NA_print.pdf");
                FileContent mediaContent = new FileContent("application/pdf", filePath);
                File file_up = testObj.gdriveClient.files().create(fileMetadata, mediaContent)
                        .setFields("id")
                        .execute();
                System.out.println("File ID: " + file_up.getId());
//                service.files().export(file.getId(), file.getMimeType())
//                        .executeMediaAndDownloadTo(outputStream);
//                service.files().export(file.getId(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
////                        .executeMediaAndDownloadTo(outputStream);
            }
        }
    }


    @Override
    public void setResource(Resource resource) {

    }

    @Override
    public DataChunk read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {

    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}