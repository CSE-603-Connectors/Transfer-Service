package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.amazonaws.util.IOUtils;
import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.UploadSessionCursor;
import com.dropbox.core.v2.files.UploadSessionStartUploader;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class DropBoxWriter implements ItemWriter<DataChunk> {

    private final OAuthEndpointCredential credential;
    private String destinationPath;
    private DbxClientV2 client;
    String sessionId;
    private UploadSessionCursor cursor;

    public DropBoxWriter(OAuthEndpointCredential credential){
        this.credential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws DbxException {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destinationPath != null;
        this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, this.credential.getToken());
        sessionId = this.client.files().uploadSessionStart().finish().getSessionId();
    }

    @AfterStep
    public void afterStep() throws DbxException {
        CommitInfo commitInfo = CommitInfo.newBuilder(this.destinationPath).build();
        this.client.files().uploadSessionFinish(cursor, commitInfo);
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        this.cursor = new UploadSessionCursor(sessionId, items.get(0).getSize());
        for(DataChunk chunk : items){
            this.client.files().uploadSessionAppendV2(cursor).uploadAndFinish(new ByteArrayInputStream(chunk.getData()));
        }
    }
}
