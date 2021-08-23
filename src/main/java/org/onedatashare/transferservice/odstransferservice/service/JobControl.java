package org.onedatashare.transferservice.odstransferservice.service;

import lombok.*;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.config.ApplicationThreadPoolConfig;
import org.onedatashare.transferservice.odstransferservice.config.DataSourceConfig;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Writer;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsReader;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsWriter;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Service
@NoArgsConstructor
@Getter
@Setter
public class JobControl extends DefaultBatchConfigurer {

    Logger logger = LoggerFactory.getLogger(JobControl.class);

    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ApplicationThreadPoolConfig threadPoolConfig;

    @Autowired
    DataSourceConfig datasource;

    public TransferJobRequest request;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Lazy
    @Autowired
    ConnectionBag connectionBag;

    @Autowired(required = false)
    public void setDatasource(DataSource datasource) {
        this.dataSource = datasource;
        this.transactionManager = new DataSourceTransactionManager(dataSource);
    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Lazy
    @Bean
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(this.createJobRepository());
        jobLauncher.setTaskExecutor(this.threadPoolConfig.sequentialThreadPool());
        logger.info("Job launcher for the transfer controller has a thread pool");
        return jobLauncher;
    }

    //    @Bean
    @SneakyThrows
    protected JobRepository createJobRepository() {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(transactionManager);
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        return factory.getObject();
    }

    private List<Flow> createConcurrentFlow(List<EntityInfo> infoList, String basePath, String id) {
        logger.info("CreateConcurrentFlow function");
        List<Flow> flows = new ArrayList<>();
        for (EntityInfo file : infoList) {
            String idForStep = "";
            if(!file.getId().isEmpty()){
                idForStep = file.getId();
            }else{
                idForStep = file.getPath();
            }
            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(idForStep).<DataChunk, DataChunk>chunk(this.request.getOptions().getPipeSize()).transactionManager().faultTolerant();
            if(ODSUtility.fullyOptimizableProtocols.contains(this.request.getSource().getType()) && ODSUtility.fullyOptimizableProtocols.contains(this.request.getDestination().getType()) && this.request.getOptions().getParallelThreadCount() > 1){
                threadPoolConfig.setParallelThreadPoolSize(request.getOptions().getParallelThreadCount());
                child.taskExecutor(this.threadPoolConfig.parallelThreadPool());
            }
            child.reader(getRightReader(request.getSource().getType(), file)).writer(getRightWriter(request.getDestination().getType(), file));
            flows.add(new FlowBuilder<Flow>(id + basePath).start(child.build()).build());
        }

        return flows;
    }


    protected AbstractItemCountingItemStreamItemReader<DataChunk> getRightReader(EndpointType type, EntityInfo fileInfo) {
        switch (type) {
            case vfs:
                return new VfsReader(request.getSource().getVfsSourceCredential(), fileInfo, request.getChunkSize());
            case sftp:
                SFTPReader sftpReader = new SFTPReader(request.getSource().getVfsSourceCredential(), request.getChunkSize(), fileInfo);
                sftpReader.setConnectionPool(this.connectionBag.getSftpReaderPool());
                return sftpReader;
            case ftp:
                return new FTPReader(request.getSource().getVfsSourceCredential(), fileInfo, request.getChunkSize());
            case s3:
                return new AmazonS3Reader(request.getSource().getVfsSourceCredential(), request.getChunkSize());
            case box:
                return new BoxReader(request.getSource().getOauthSourceCredential(), request.getChunkSize(), fileInfo);
        }
        return null;
    }

    protected ItemWriter<DataChunk> getRightWriter(EndpointType type, EntityInfo fileInfo) {
        switch (type) {
            case vfs:
                return new VfsWriter(request.getDestination().getVfsDestCredential());
            case sftp:
                SFTPWriter sftpWriter = new SFTPWriter(request.getDestination().getVfsDestCredential());
                sftpWriter.setConnectionPool(this.connectionBag.getSftpWriterPool());
                return sftpWriter;
            case ftp:
                return new FTPWriter(request.getDestination().getVfsDestCredential());
            case s3:
                return new AmazonS3Writer(request.getDestination().getVfsDestCredential(), fileInfo);
            case box:
                return new BoxWriter(request.getDestination().getOauthDestCredential(), fileInfo, request.getChunkSize());
        }
        return null;
    }

    @Lazy
    @Bean
    public Job concurrentJobDefinition() {
        List<Flow> flows = createConcurrentFlow(request.getSource().getInfoList(), request.getSource().getParentInfo().getPath(), request.getJobId());

        threadPoolConfig.setSTEP_POOL_SIZE(this.request.getOptions().getConcurrencyThreadCount());
        Flow f = new FlowBuilder<SimpleFlow>("splitFlow")
                .split(this.threadPoolConfig.stepTaskExecutor())
                .add(flows.toArray(new Flow[flows.size()]))
                .end();
        JobCompletionListener jobCompletionListener = new JobCompletionListener();
        jobCompletionListener.setConnectionBag(this.connectionBag);
        String jobId = new StringBuilder().append(request.getOwnerId()).append("-").append(request.getJobId()).append("-").append(System.currentTimeMillis()).toString();
        Job job = jobBuilderFactory
                .get(request.getOwnerId())
                //.get(new StringBuilder(request.getOwnerId()).append("-").append(ThreadLocalRandom.current().nextInt()).toString())
                .incrementer(new RunIdIncrementer())
                .preventRestart()
                .listener(jobCompletionListener)
                .start(f)
                .build()
                .build();
        return job;
    }
}