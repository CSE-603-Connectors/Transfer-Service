package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class VfsWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(VfsWriter.class);
    AccountEndpointCredential destCredential;
    HashMap<String, FileChannel> stepDrain;
    String fileName;
    String destinationPath;

    public VfsWriter(AccountEndpointCredential credential) {
        stepDrain = new HashMap<>();
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = stepExecution.getStepName();
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
    }

    @AfterStep
    public void afterStep() {
        try {
            if(this.stepDrain.containsKey(this.fileName)){
                this.stepDrain.get(this.fileName).close();
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public FileChannel getChannel(Path filePath) throws IOException {
        if (this.stepDrain.containsKey(filePath.getFileName().toString())) {
            return this.stepDrain.get(filePath.getFileName().toString());
        } else {
            logger.info("creating file : " + filePath.toString());
            prepareFile(filePath);
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE);
            stepDrain.put(filePath.getFileName().toString(), channel);
            return channel;
        }
    }

    public void prepareFile(Path fileToPrepare) {
        if(Files.notExists(fileToPrepare.getParent())){
            try{
                Files.createDirectories(fileToPrepare.getParent());
            } catch (IOException e) {
                logger.error("Already have the directory with this path \t {}", fileToPrepare.getParent().toString());
                e.printStackTrace();
            }
        }
        if(Files.notExists(fileToPrepare)){
            try {
                Files.createFile(fileToPrepare);
            } catch (IOException e) {
                logger.error("Already have the file with this path \t {}", fileToPrepare.toString());
                e.printStackTrace();
            }
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws IOException {
        Path currentFilePath = Paths.get(this.destinationPath, items.get(0).getFileName());
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            FileChannel channel = getChannel(currentFilePath);
            int bytesWritten = channel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            logger.info("Wrote the amount of bytes: " + String.valueOf(bytesWritten));
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote " + bytesWritten + " but we should have written " + chunk.getSize());
        }
    }
}
