package org.onedatashare.transferservice.odstransferservice.service.listner;

import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);
    private ConnectionBag connectionBag;

    @Override
    public void beforeJob(JobExecution jobExecution) {
//        connectionBag.preparePools(transferJobRequest);
        logger.info("BEFOR JOB-------------------present time--" + System.currentTimeMillis());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        connectionBag.closePools();
        logger.info("After JOB------------------present time--" + System.currentTimeMillis());
    }

    public void setConnectionBag(ConnectionBag connectionBag){
        this.connectionBag = connectionBag;
    }
}