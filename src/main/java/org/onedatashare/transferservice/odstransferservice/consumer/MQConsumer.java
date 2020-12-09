package org.onedatashare.transferservice.odstransferservice.consumer;

import com.google.gson.Gson;
import org.onedatashare.transferservice.odstransferservice.OdsTransferService;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;


@Service
public class MQConsumer {

    Logger logger = LoggerFactory.getLogger(MQConsumer.class);

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobControl jc;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    JobLauncher asyncJobLauncher;

    @Autowired
    CrudService crudService;

    @Autowired
    JobParamService jobParamService;

    @RabbitListener(queues = OdsTransferService.DEFAULT_PARSING_QUEUE)
    public void consumeDefaultMessage(final MqMessage message) throws Exception {
        logger.info("Received new message from queue.");
        Gson g = new Gson();
        TransferJobRequest request = g.fromJson(message.getText(), TransferJobRequest.class);

        JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
        jobParamService.setStaticVar(request);
        crudService.insertBeforeTransfer(request);
        jc.setRequest(request);
        jc.setChunkSize(SIXTYFOUR_KB); //64kb.
        asyncJobLauncher.run(jc.concurrentJobDefination(), parameters);
    }
}