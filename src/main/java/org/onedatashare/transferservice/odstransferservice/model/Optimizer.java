package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

@Data
public abstract class Optimizer {
    int concurrency;
    int parallelism;
    int pipelining;
    long chunkSize;
}