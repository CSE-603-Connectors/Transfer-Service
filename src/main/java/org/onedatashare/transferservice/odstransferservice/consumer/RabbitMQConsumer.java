//package org.onedatashare.transferservice.odstransferservice.consumer;
//
//
//import com.google.gson.Gson;
//import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
//import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
//import org.onedatashare.transferservice.odstransferservice.service.JobControl;
//import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.amqp.core.Message;
//import org.springframework.amqp.core.MessageListener;
//import org.springframework.amqp.rabbit.annotation.RabbitListener;
//import org.springframework.batch.core.JobParameters;
//import org.springframework.batch.core.JobParametersBuilder;
//import org.springframework.batch.core.launch.JobLauncher;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//@Service
//public class RabbitMQConsumer {
//
//    Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
//
//    @Autowired
//    JobControl jc;
//
//    @Autowired
//    JobLauncher asyncJobLauncher;
//
//    @Autowired
//    JobParamService jobParamService;
//
//    @Autowired
//    CrudService crudService;
//
//
//    @RabbitListener(queues = "${ods.rabbitmq.queue}")
//    public void consumeDefaultMessage(final Message message) throws Exception {
//        String jsonStr = new String(message.getBody());
//        System.out.println("Consuming Message - " + jsonStr);
//
//        Gson g = new Gson();
//        TransferJobRequest request = g.fromJson(jsonStr, TransferJobRequest.class);
//        System.out.println("request obj--> " + request.toString());
//        try {
//            JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
//            crudService.insertBeforeTransfer(request);
//            jc.setRequest(request);
//            jc.setChunkSize(request.getChunkSize());
//            asyncJobLauncher.run(jc.concurrentJobDefinition(), parameters);
//        } catch (Exception e) {
//            logger.error("Not able to start job");
//            e.printStackTrace();
//        }
//    }
//}