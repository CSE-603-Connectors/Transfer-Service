package org.onedatashare.transferservice.odstransferservice.service.step.http;

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

    @AfterStep
    public void afterStep(){

    }

    @Override
    public void write(List<? extends DataChunk> list) throws Exception {

    }
}
