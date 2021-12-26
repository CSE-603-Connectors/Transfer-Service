package org.onedatashare.transferservice.odstransferservice.service.step.http;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.nio.file.*;

import java.util.*;
import org.slf4j.Logger;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class HttpWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(HttpWriter.class);
    AccountEndpointCredential destCredential;
    String fileName;
    String destPath;
    Path filePath;


    public HttpWriter(AccountEndpointCredential credential) {
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destPath != null;
        this.filePath = Paths.get(this.destPath);
    }

    public void initClient(URI uri, int connections) {
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.setHost(uri);
        HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setMaxConnectionsPerHost(hostConfig, connections);
        connectionManager.setParams(params);
        HttpClient client = new HttpClient(connectionManager);
        Credentials creds = new UsernamePasswordCredentials("", "");
        client.getState().setCredentials(AuthScope.ANY, creds);
        client.setHostConfiguration(hostConfig);
    }
/*
    // WebDAV URL:
    final String baseUrl = ...;
    // Source file to upload:
    File f = ...;
        try{
        HttpClient client = new HttpClient();
        Credentials creds = new UsernamePasswordCredentials("username", "password");
        client.getState().setCredentials(AuthScope.ANY, creds);

        PutMethod method = new PutMethod(baseUrl + "/" + f.getName());
        RequestEntity requestEntity = new InputStreamRequestEntity(
                new FileInputStream(f));
        method.setRequestEntity(requestEntity);
        client.executeMethod(method);
        System.out.println(method.getStatusCode() + " " + method.getStatusText());
    }
 */
    @AfterStep
    public void afterStep(){

    }

    @Override
    public void write(List<? extends DataChunk> list) throws Exception {

    }
}
