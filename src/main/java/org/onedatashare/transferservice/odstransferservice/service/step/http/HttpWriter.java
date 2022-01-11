package org.onedatashare.transferservice.odstransferservice.service.step.http;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.pools.WebDavConnectionPool;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

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
    WebDavConnectionPool webDavConnectionPool;



    public HttpWriter(AccountEndpointCredential credential) {
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destPath != null;
        this.filePath = Paths.get(this.destPath);
    }

    @AfterStep
    public void afterStep(){
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        HttpClient client = this.webDavConnectionPool.borrowObject();
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

    public void setPool(ObjectPool connectionPool) {
        this.webDavConnectionPool = (WebDavConnectionPool) connectionPool;
    }
}
