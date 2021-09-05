package org.onedatashare.transferservice.odstransferservice.consumer;


import com.google.gson.Gson;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQConsumer {

    Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    @Autowired
    JobControl jc;

    @Autowired
    JobLauncher syncJobLauncher;

    @Autowired
    JobParamService jobParamService;

    @Autowired
    CrudService crudService;

    @Autowired
    ConnectionBag connectionBag;


    @RabbitListener(queues = "${ods.rabbitmq.queue}")
    public void consumeDefaultMessage(final Message message) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        String jsonStr = new String(message.getBody());
        Gson g = new Gson();
        TransferJobRequest request = g.fromJson(jsonStr, TransferJobRequest.class);
        logger.info(request.toString());
        JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
        crudService.insertBeforeTransfer(request);
        jc.setRequest(request);
        syncJobLauncher.run(jc.concurrentJobDefinition(request), parameters);
    }
}