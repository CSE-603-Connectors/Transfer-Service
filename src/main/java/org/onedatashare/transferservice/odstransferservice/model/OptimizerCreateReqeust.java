package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

@Data
public class OptimizerCreateReqeust extends Optimizer{
    String nodeId;
    int maxConcurrency;
    int maxParallelism;
    int maxPipelining;
    int maxChunkSize;
}
