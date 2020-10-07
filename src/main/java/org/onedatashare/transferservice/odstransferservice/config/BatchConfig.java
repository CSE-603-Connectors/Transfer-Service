package org.onedatashare.transferservice.odstransferservice.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.CustomReader;
import org.onedatashare.transferservice.odstransferservice.service.step.Processor;
import org.onedatashare.transferservice.odstransferservice.service.step.FTPWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.UrlResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.util.List;

@Configuration
public class BatchConfig {
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchConfig.class);

    @Autowired
    private ApplicationThreadPoolConfig threadPoolConfig;


    @Autowired
    DataSourceConfig datasource;

//    @Autowired
//    FlatFileItemReader flatFileItemReader;

//    @Autowired
//    CustomReader customReader;

//    @Autowired
//    Reader reader;

    @Autowired
    FTPWriter ftpWriter;

    @Autowired
    Processor processor;

    @Autowired
    TaskExecutor stepTaskExecutor;

    @Bean
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(createJobRepository());
        jobLauncher.setTaskExecutor(threadPoolConfig.jobRequestThreadPool());
        LOGGER.info("Job launcher for the transfer controller has a thread pool");
        return jobLauncher;
    }

    @Bean
    @SneakyThrows
    protected JobRepository createJobRepository() {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(datasource.getH2DataSource());
        factory.setTransactionManager(new DataSourceTransactionManager(datasource.getH2DataSource()));
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        return factory.getObject();
    }

    @StepScope
    @SneakyThrows
    @Lazy
    @Bean
    public CustomReader customReader(@Value("#{jobParameters['sourceBasePath']}") String basePath, @Value("#{jobParameters['sourceCredential']}") String accountId, @Value("#{jobParameters['INFO_LIST']}") String infoList){
        List<EntityInfo> fileList = new ObjectMapper().readValue(infoList, new TypeReference<List<EntityInfo>>(){});
        CustomReader<DataChunk> reader = new CustomReader<>();
        for(EntityInfo info: fileList){
            String fileName = info.getPath();
            reader.setResource(new UrlResource(basePath.substring(0,6)+accountId+"@" + basePath.substring(6) + fileName));
        }
        return reader;
    }

//    @Bean
//    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
//        LOGGER.info("Inside job---");
//        Step step = stepBuilderFactory.get("SampleStep")
//                .<byte[], byte[]>chunk(2)
//                .reader(customReader(null, null, null))
//                //.processor(processor)
//                .writer(ftpWriter)
//                //.taskExecutor(stepTaskExecutor)
//                .build();
//        return jobBuilderFactory.get("job").listener(listener())
//                .incrementer(new RunIdIncrementer())
//                .start(step)
//                .build();
//    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

}