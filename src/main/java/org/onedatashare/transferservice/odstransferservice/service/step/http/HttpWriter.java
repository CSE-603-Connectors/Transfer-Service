package org.onedatashare.transferservice.odstransferservice.service.step.http;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.jackrabbit.webdav.DavMethods;
import org.apache.jackrabbit.webdav.DavMethods.*;
import org.apache.jackrabbit.webdav.property.*;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import org.apache.jackrabbit.webdav.client.methods.HttpProppatch;

import java.io.FileInputStream;
import java.nio.file.*;

import java.util.*;
import org.slf4j.Logger;

import javax.xml.crypto.Data;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class HttpWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(HttpWriter.class);
    AccountEndpointCredential destCredential;
    String fileName;
    String destPath;
    Path filePath;
    HttpClient client;



    public HttpWriter(AccountEndpointCredential credential) {
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destPath != null;
        this.filePath = Paths.get(this.destPath);
    }

    public void initClient(URI uri) {
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.setHost(uri);
        client = new HttpClient();
        Credentials creds = new UsernamePasswordCredentials(destCredential.getUsername(), destCredential.getSecret());
        client.getState().setCredentials(AuthScope.ANY, creds);
    }

    @AfterStep
    public void afterStep(){
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        PutMethod put = new PutMethod("webdav url");
        for(DataChunk item: items) {
            RequestEntity requestEntity =  new InputStreamRequestEntity(new FileInputStream(Arrays.toString(item.getData())));
            put.setRequestHeader(ODSConstants.RANGE, String.format(ODSConstants.byteRange, item.getStartPosition(), item.getStartPosition()+item.getSize()));
            put.setRequestEntity(requestEntity);
            client.executeMethod(put);
        }
        if (this.destPath == null) {
//            this.destPath
        }
    }
}
