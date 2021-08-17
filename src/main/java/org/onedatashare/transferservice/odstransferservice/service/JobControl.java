package org.onedatashare.transferservice.odstransferservice.service;

import lombok.*;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.config.ApplicationThreadPoolConfig;
import org.onedatashare.transferservice.odstransferservice.config.DataSourceConfig;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
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

    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ApplicationThreadPoolConfig threadPoolConfig;

    @Autowired
    DataSourceConfig datasource;

    public TransferJobRequest request;
    Step parent;
    Logger logger = LoggerFactory.getLogger(JobControl.class);

    @Autowired
    private ApplicationContext context;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    VfsExpander vfsExpander;

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
            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(idForStep).<DataChunk, DataChunk>chunk(this.request.getOptions().getPipeSize());
            if(ODSUtility.fullyOptimizableProtocols.contains(this.request.getSource().getType()) && ODSUtility.fullyOptimizableProtocols.contains(this.request.getDestination().getType()) && this.request.getOptions().getParallelThreadCount() > 1){
                threadPoolConfig.setParallelThreadPoolSize(request.getOptions().getParallelThreadCount());
                child.taskExecutor(this.threadPoolConfig.parallelThreadPool());
            }
            child.reader(connectionBag.getReader(request.getSource(), file, request.getChunkSize()))
                    .writer(connectionBag.getWriter(request.getDestination(), file));
            flows.add(new FlowBuilder<Flow>(id + basePath).start(child.build()).build());
        }
        return flows;
    }

    @Lazy
    @Bean
    public Job concurrentJobDefinition() {
        List<EntityInfo> fileInfoList = request.getSource().getInfoList();
        if(request.getSource().getType().equals(EndpointType.vfs)){
            fileInfoList = vfsExpander.expandListOfFiles(request.getSource().getInfoList(), request.getSource().getParentInfo().getPath());
        }
        List<Flow> flows = createConcurrentFlow(fileInfoList, request.getSource().getParentInfo().getPath(), request.getJobId());
        Flow[] fl = new Flow[flows.size()];
        threadPoolConfig.setSTEP_POOL_SIZE(this.request.getOptions().getConcurrencyThreadCount());
        Flow f = new FlowBuilder<SimpleFlow>("splitFlow")
                .split(this.threadPoolConfig.stepTaskExecutor())
                .add(flows.toArray(fl))
                .build();
        return jobBuilderFactory
                .get(request.getOwnerId())
                .listener(new JobCompletionListener())
                .incrementer(new RunIdIncrementer())
                .start(f).build().build();
    }
}