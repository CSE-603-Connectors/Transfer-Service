package org.onedatashare.transferservice.odstransferservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@Configuration
public class ApplicationThreadPoolConfig{

    @Setter
    @Getter
    private int TRANSFER_POOL_SIZE=1;
    @Setter
    @Getter
    private int JOB_POOL_SIZE=1;
    @Setter
    @Getter
    private int JOB_MAX_POOL_SIZE=1;
    @Setter
    @Getter
    private int STEP_POOL_SIZE=1;
    @Setter
    @Getter
    private int STEP_MAX_POOL_SIZE=1;

    @Getter
    @Setter
    private int parallelThreadPoolSize = 1;

    public ThreadPoolTaskExecutor stepTaskExecutor(int concurrencyThreadCount){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(concurrencyThreadCount);
        executor.setThreadNamePrefix("step");
        executor.setKeepAliveSeconds(60);
        executor.setAllowCoreThreadTimeOut(true);
        executor.initialize();
        return executor;
    }

    public ThreadPoolTaskExecutor parallelThreadPool(int size){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(size);
        executor.setMaxPoolSize(size);
        executor.setThreadNamePrefix(System.currentTimeMillis()+"---parallel");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }
}
