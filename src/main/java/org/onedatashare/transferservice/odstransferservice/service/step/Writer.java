package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

@Component
public class Writer implements ItemWriter<byte[]> {
    Logger logger = LoggerFactory.getLogger(Writer.class);
    public void write(List<? extends byte[]> list) throws Exception {
        for(byte[] elem : list){
            logger.info(new String(elem));
        }
        logger.info("Inside Writer----------------");
        for (byte[] b : list) {

            StreamOutput.getOutputStream().write(b);
            StreamOutput.getOutputStream().flush();
        }
    }
}
