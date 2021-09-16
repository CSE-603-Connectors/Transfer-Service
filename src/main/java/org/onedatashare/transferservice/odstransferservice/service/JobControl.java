package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.config.ApplicationThreadPoolConfig;
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
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
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

    public TransferJobRequest request;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Autowired
    ApplicationThreadPoolConfig threadPoolConfig;

    @Autowired
    DataSource datasource;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    JobCompletionListener jobCompletionListener;

    @Autowired
    RunIdIncrementer runIdIncrementer;

    @Autowired
    JobRepository jobRepository;


    private List<Flow> createConcurrentFlow(List<EntityInfo> infoList, String basePath, String id) {
        List<Flow> flows = new ArrayList<>();
        for (EntityInfo file : infoList) {
            String idForStep = "";
            if (!file.getId().isEmpty()) {
                idForStep = file.getId();
            } else {
                idForStep = file.getPath();
            }
            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(file.getPath()).<DataChunk, DataChunk>chunk(this.request.getOptions().getPipeSize());
            if (ODSUtility.fullyOptimizableProtocols.contains(this.request.getDestination().getType()) && this.request.getOptions().getParallelThreadCount() > 1) {
                threadPoolConfig.setParallelThreadPoolSize(request.getOptions().getParallelThreadCount());
                child.taskExecutor(this.threadPoolConfig.parallelThreadPool());
            }
            child.reader(getRightReader(request.getSource().getType(), file))
                    .writer(getRightWriter(request.getDestination().getType(), file))
                    .faultTolerant();
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
                sftpReader.setPool(connectionBag.getSftpReaderPool());
                return sftpReader;
            case ftp:
                FTPReader ftpReader = new FTPReader(request.getSource().getVfsSourceCredential(), fileInfo, request.getChunkSize());
                ftpReader.setPool(connectionBag.getFtpReaderPool());
                return ftpReader;
            case s3:
                return new AmazonS3Reader(request.getSource().getVfsSourceCredential(), request.getChunkSize(), fileInfo);
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
                sftpWriter.setPool(connectionBag.getSftpWriterPool());
                return sftpWriter;
            case ftp:
                FTPWriter ftpWriter = new FTPWriter(request.getDestination().getVfsDestCredential());
                ftpWriter.setPool(connectionBag.getFtpWriterPool());
                return ftpWriter;
            case s3:
                return new AmazonS3Writer(request.getDestination().getVfsDestCredential(), fileInfo);
            case box:
                return new BoxWriter(request.getDestination().getOauthDestCredential(), fileInfo, request.getChunkSize());
        }
        return null;
    }

    public Job concurrentJobDefinition() {
        connectionBag.preparePools(this.request);
        List<Flow> flows = createConcurrentFlow(request.getSource().getInfoList(), request.getSource().getParentInfo().getPath(), request.getJobId());
        Flow[] fl = new Flow[flows.size()];
        threadPoolConfig.setSTEP_POOL_SIZE(this.request.getOptions().getConcurrencyThreadCount());
        Flow f = new FlowBuilder<SimpleFlow>("splitFlow")
                .split(this.threadPoolConfig.stepTaskExecutor())
                .add(flows.toArray(fl))
                .build();
        return jobBuilderFactory
                .get(request.getOwnerId())
                .repository(jobRepository)
                .listener(jobCompletionListener)
                .incrementer(runIdIncrementer)
                .preventRestart()
                .start(f)
                .build()
                .build();
    }
}