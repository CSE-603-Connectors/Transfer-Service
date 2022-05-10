package org.onedatashare.transferservice.odstransferservice.pools;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

@Service
public class ThreadPoolManager {

    @Getter
    HashMap<String, ThreadPoolTaskExecutor> executorHashmap;

    Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);

    @PostConstruct
    public void createMap() {
        this.executorHashmap = new HashMap<>();
        logger.info("creating executor hashmap");
    }

    public ThreadPoolTaskExecutor createThreadPool(int corePoolSize, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(corePoolSize);
        executor.setThreadNamePrefix(prefix);
        executor.setAllowCoreThreadTimeOut(true);
        executor.initialize();
        if (this.executorHashmap == null) {
            this.executorHashmap = new HashMap<>();
        }
        this.executorHashmap.put(prefix, executor);
        return executor;
    }

    /**
     * @param concurrency
     * @param parallel
     */
    public void applyOptimizer(int concurrency, int parallel) {
        for (String key : this.executorHashmap.keySet()) {
            ThreadPoolTaskExecutor pool = this.executorHashmap.get(key);
            try {
                if (key.contains(STEP_POOL_PREFIX)) {
                    logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getPoolSize(), concurrency);
                    if (concurrency > 0) {
                        pool.setCorePoolSize(concurrency);
                        pool.setMaxPoolSize(concurrency);
                        logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), concurrency);
                    }
                }
            }catch (Exception ignored){
                logger.info("Failed to set the step pool size for some reason");
                ignored.printStackTrace();
            }
            try{
                if (key.contains(PARALLEL_POOL_PREFIX)) {
                    logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getPoolSize(), parallel);
                    if (parallel > 0) {
                        pool.setMaxPoolSize(parallel);
                        pool.setCorePoolSize(parallel);
                        logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), parallel);
                    }
                }
            }catch(Exception ignore){
                logger.error("Failed to set the parallel pool");
                ignore.printStackTrace();
            }
        }
    }

    public void clearJobPool() {
        for (String key : this.executorHashmap.keySet()) {
            if (key.contains(STEP_POOL_PREFIX) || key.contains(PARALLEL_POOL_PREFIX)) {
                ThreadPoolTaskExecutor executor = this.executorHashmap.get(key);
                if(executor != null){
                    executor.shutdown();
                }
                this.executorHashmap.remove(key);
            }
        }
    }

    public ThreadPoolTaskExecutor sequentialThreadPool() {
        return this.createThreadPool(1, SEQUENTIAL_POOL_PREFIX);
    }

    public ThreadPoolTaskExecutor stepTaskExecutor(int threadCount) {
        return this.createThreadPool(threadCount, STEP_POOL_PREFIX);
    }

    public ThreadPoolTaskExecutor parallelThreadPool(int threadCount, String fileName) {
        return this.createThreadPool(threadCount, new StringBuilder().append(fileName).append("-").append(PARALLEL_POOL_PREFIX).toString());
    }

    public Integer concurrencyCount(){
        return this.executorHashmap.get(STEP_POOL_PREFIX).getPoolSize();
    }

    public Integer parallelismCount(String name){
        int parallelism = 0;
        for(String key: this.executorHashmap.keySet()){
            if(key.contains(name)){
                parallelism = this.executorHashmap.get(key).getPoolSize();
                logger.info("{} of active Count is {}", key, parallelism);
                break;
            }
        }
        return parallelism;
    }

}
