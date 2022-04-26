package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.Optimizer;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerInputRequest;
import org.onedatashare.transferservice.odstransferservice.service.OptimizerService;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.DoubleSummaryStatistics;
import java.util.List;

@Component
public class OptimizerListener implements ItemWriteListener<DataChunk>, ItemReadListener<DataChunk>, StepExecutionListener, Runnable {

    @Lazy
    @Autowired
    private OptimizerService optimizerService;

    @Lazy
    @Autowired
    private ThreadPoolTaskExecutor parallelExecutor;

    @Lazy
    @Autowired
    private ThreadPoolTaskExecutor stepExecutor;


    @Value("${spring.application.name}")
    private String appName;

    private long writerEndTime;
    private long readerStartTime;
    private DoubleSummaryStatistics throughputStat;
    private OptimizerInputRequest inputRequest;
    private boolean firstRead;
    private long chunkSize;
    private int pipelining;
    private StepExecution stepExecution;

    public OptimizerListener() {
        chunkSize = readerStartTime = writerEndTime = 0L;
        pipelining = 0;
        firstRead = false;
        throughputStat = new DoubleSummaryStatistics();
        inputRequest = new OptimizerInputRequest();
    }

    @Override
    public void beforeRead() {
        if (!firstRead) {
            readerStartTime = System.nanoTime();
        }
    }

    @Override
    public void afterWrite(List<? extends DataChunk> items) {
        writerEndTime = System.nanoTime();
        long totalTime = writerEndTime - readerStartTime;
        long bytesSent = items.stream().mapToLong(DataChunk::getSize).sum();
        double throughput = (double) bytesSent / (totalTime);
        throughputStat.accept(throughput);
        this.pipelining = items.size();
    }

    @Override
    public void afterRead(DataChunk item) {
        if (item.getChunkIdx() % this.pipelining == 0) {
            this.chunkSize = item.getSize();
            this.firstRead = true;
        }
    }

    @Override
    public void beforeWrite(List<? extends DataChunk> items) {
    }

    @Override
    public void onReadError(Exception ex) {
    }

    @Override
    public void onWriteError(Exception exception, List<? extends DataChunk> items) {
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }

    @Override
    public void run() {
        this.inputRequest.setThroughput(this.throughputStat.getAverage());
        this.inputRequest.setConcurrency(this.stepExecutor.getPoolSize());
        this.inputRequest.setParallelism(this.parallelExecutor.getPoolSize());
        this.inputRequest.setNodeId(this.appName);
        this.inputRequest.setPipelining(this.pipelining); //this is the commit interval of a step.
        this.inputRequest.setChunkSize(this.chunkSize); //We need to get this from the
        Optimizer newParams = this.optimizerService.inputToOptimizerBlocking(this.inputRequest);
        int parallelismVal = newParams.getParallelism();
        int concurrencyVal = newParams.getConcurrency();
        this.parallelExecutor.setMaxPoolSize(parallelismVal);
        this.parallelExecutor.setCorePoolSize(parallelismVal);
        this.stepExecutor.setCorePoolSize(concurrencyVal);
        this.stepExecutor.setMaxPoolSize(concurrencyVal);
        this.stepExecution.setCommitCount(newParams.getPipelining());
    }
}
