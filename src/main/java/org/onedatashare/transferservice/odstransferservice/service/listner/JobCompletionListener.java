package org.onedatashare.transferservice.odstransferservice.service.listner;

import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerCreateRequest;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerDeleteRequest;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.OptimizerService;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.OptimizerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledFuture;


@Component
public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    MetricsCollector metricsCollector;

    @Autowired
    OptimizerService optimizerService;

    @Autowired
    OptimizerListener optimizerListener;

    @Value("${spring.application.name}")
    String appName;

    @Value("${optimizer.interval}")
    long interval;

    @Autowired
    TaskScheduler taskScheduler;
    private ScheduledFuture<?> scheduledFuture;


    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("BEFOR JOB-------------------present time--" + System.currentTimeMillis());
        OptimizerCreateRequest createReqeust = new OptimizerCreateRequest();
        createReqeust.setNodeId(appName);
        createReqeust.setMaxChunkSize(Integer.MAX_VALUE);
        createReqeust.setMaxConcurrency(32);
        createReqeust.setMaxParallelism(20);
        createReqeust.setMaxPipelining(100);
        optimizerService.createOptimizerBlocking(createReqeust);
        this.scheduledFuture = taskScheduler.scheduleAtFixedRate(optimizerListener, interval);

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("After JOB------------------present time--" + System.currentTimeMillis());
        OptimizerDeleteRequest deleteRequest = new OptimizerDeleteRequest();
        deleteRequest.setNodeId(appName);
        this.scheduledFuture.cancel(false);
        optimizerService.deleteOptimizerBlocking(deleteRequest);
        JobMetric jobMetric = metricsCollector.populateJobMetric(jobExecution, null);
        metricsCollector.collectJobMetrics(jobMetric);
        connectionBag.closePools();
    }
}

