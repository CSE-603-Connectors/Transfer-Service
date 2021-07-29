package org.onedatashare.transferservice.odstransferservice.utility;


// [START drive_quickstart]
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
import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

public class AuthTest {
    private static final String APPLICATION_NAME = "Google Drive API Java Quickstart";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";
    private static final String destinationFolder = "C:\\Users\\Devya Singh\\Downloads\\authtestdownloads\\";
    /**
     * Global instance of the scopes required by this quickstart.
     * If modifying these scopes, delete your previously saved tokens/ folder.
     */
    //private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE_METADATA_READONLY);
    private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);
    //['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'];
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
    //private static final String CREDENTIALS_FILE_PATH = "/Downloads/client_secret_742926411922-npv1mg1fq5meb2hvqjk6cpf9hddeksp3.apps.googleusercontent.com";
    /**
     * Creates an authorized Credential object.
     * @param HTTP_TRANSPORT The network HTTP Transport.
     * @return An authorized Credential object.
     * @throws IOException If the credentials.json file cannot be found.
     */
    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = AuthTest.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    public static void main(String... args) throws IOException, GeneralSecurityException {
        // Build a new authorized API client service.
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        //System.out.println("HTTP TRANSPORT %s", HTTP_TRANSPORT.toString());
        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
        // Print the names and IDs for up to 10 files.
        FileList result = service.files().list()
                .setQ("name contains 'Arlington'")
                .setPageSize(10)
                .setFields("nextPageToken, files(id, name, kind)")  //The setField for Drive API is used for partial responses, it will depend on what data you want that will be part of the returned object.
                .execute();
        List<File> files = result.getFiles();
        if (files == null || files.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (File file : files) {
                System.out.printf("%s (%s) %s\n", file.getName(), file.getId(), file.getKind()); // for setFields https://developers.google.com/drive/api/v3/reference/files
                OutputStream outputStream = new FileOutputStream(destinationFolder + file.getName());
//                        new ByteArrayOutputStream();
                service.files().get(file.getId())
                        .executeMediaAndDownloadTo(outputStream);
                outputStream.flush();
                outputStream.close();
//                service.files().export(file.getId(), "application/pdf")
//                        .executeMediaAndDownloadTo(outputStream);
            }
        }
    }
}