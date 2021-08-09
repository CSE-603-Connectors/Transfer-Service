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
    String destinationPath;
    Path filePath;

    public VfsWriter(AccountEndpointCredential credential) {
        stepDrain = new HashMap<>();
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
    }

    @AfterStep
    public void afterStep() {
        for(String fileKey : this.stepDrain.keySet()){
            try{
                this.stepDrain.get(fileKey).close();
            } catch (IOException e){
                logger.error("Tried to close the FileChannel for file {}", fileKey);
                e.printStackTrace();
            }
        }
    }

    public FileChannel getChannel(String fileName, String basePath) {
        if (this.stepDrain.containsKey(fileName)) {
            return this.stepDrain.get(fileName);
        } else {
            this.filePath = Paths.get(this.destinationPath + basePath + fileName);
            prepareFile(this.filePath);
            logger.info("creating file : " + fileName);
            FileChannel channel = null;
            try {
                channel = FileChannel.open(this.filePath, StandardOpenOption.WRITE);
                stepDrain.put(fileName, channel);
            } catch (IOException exception) {
                logger.error("Not Able to open the channel");
                exception.printStackTrace();
            }
            return channel;
        }
    }

    public void prepareFile(Path pathToCreate) {
        try {
            Files.createDirectories(pathToCreate);
            Files.createFile(pathToCreate);
        }catch (FileAlreadyExistsException fileAlreadyExistsException){
            logger.error("Already have the file with this path \t" + this.filePath.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        for (DataChunk chunk : items) {
            FileChannel channel = getChannel(chunk.getFileName(), chunk.getBasePath());
            int bytesWritten = channel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            logger.info("Wrote the amount of bytes: {}", bytesWritten);
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote {} but we should have written {}", bytesWritten, chunk.getSize());
        }
    }
}
