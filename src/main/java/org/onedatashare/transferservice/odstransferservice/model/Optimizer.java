package org.onedatashare.transferservice.odstransferservice.model;


import lombok.*;

@Data
public class Optimizer {
    int concurrency;
    int parallelism;
    int pipelining;
    long chunkSize;
}