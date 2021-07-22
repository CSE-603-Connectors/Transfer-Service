package org.onedatashare.transferservice.odstransferservice.utility;

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
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;


public class GDriveUtility {
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    //this should probably accept an OAuthEndpointCredential obj instead.
    public static Drive constructClient(AccountEndpointCredential credential) throws IOException, GeneralSecurityException {
        String username = credential.getUsername();
        String clientSecret = credential.getSecret();
        String uri = credential.getUri();
        String[] partsId = username.split(":::");
        String clientID = partsId[0];
        String projectID = partsId[1];
        final String APPLICATION_NAME = "Google Drive API Java Quickstart";
        final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);
        final String TOKENS_DIRECTORY_PATH = "tokens";
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

        String creds = "{"  +
                "\"installed\": {  " +
                "\"client_id\": \"" + clientID + "\"," +
                "\"project_id\": \"" + projectID + "\"," +
                "\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\"," +
                "\"token_uri\": \"https://oauth2.googleapis.com/token\"," +
                "\"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\","+
                "\"client_secret\": \"" + clientSecret + "\"," +
                    "\"redirect_uris\": [ \"urn:ietf:wg:oauth:2.0:oob\",  \""+ uri +"\" ] } }";

        InputStream in = new ByteArrayInputStream(creds.getBytes());

        if (in == null) {
            throw new NullPointerException("Unable to find GDrive credentials. ");
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        Credential authCode = new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");


        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, authCode)
                .setApplicationName(APPLICATION_NAME)
                .build();

        return service;
    }



//    public static AmazonS3 constructClient(AccountEndpointCredential credential, String region){
//        AWSCredentials credentials = new BasicAWSCredentials(credential.getUsername(), credential.getSecret());
//        return AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(credentials))
//                .withRegion(region)
//                .build();
//    }
//
//    public static String constructS3URI(String uri, String fileKey, String basePath){
//        StringBuilder builder = new StringBuilder();
//        String[] temp = uri.split(":::");
//        String bucketName = temp[1];
//        String region = temp[0];
//        builder.append("https://").append(bucketName).append(".").append("s3.").append(region).append(".").append("amazonaws.com").append(basePath).append(fileKey);
//        return builder.toString();
//    }

}
