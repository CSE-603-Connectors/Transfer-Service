package org.onedatashare.transferservice.odstransferservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@Configuration
public class ApplicationThreadPoolConfig {

    @Setter
    @Getter
    private int JOB_POOL_SIZE = 1;
    @Setter
    @Getter
    private int JOB_MAX_POOL_SIZE = 1;
    @Setter
    @Getter
    private int STEP_POOL_SIZE = 1;
    @Setter
    @Getter
    private int STEP_MAX_POOL_SIZE = 1;

    @Getter
    @Setter
    private int parallelThreadPoolSize = 1;

    @Bean
    public ThreadPoolTaskExecutor stepExecutor() {
        return concurrencyTaskExecutor();
    }

    @Bean
    public ThreadPoolTaskExecutor parallelExecutor() {
        return parallelTaskExecutor();
    }

    @Bean
    public ThreadPoolTaskExecutor sequentialExecutor() {
        return sequentialTaskExecutor();
    }


    private ThreadPoolTaskExecutor concurrencyTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(STEP_POOL_SIZE);
        executor.setThreadNamePrefix("step");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }

    private ThreadPoolTaskExecutor sequentialTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setThreadNamePrefix("sequential");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }

    private ThreadPoolTaskExecutor parallelTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(this.parallelThreadPoolSize);
        executor.setThreadNamePrefix("parallel");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }
}
