package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

@Data
public class OptimizerInputRequest extends Optimizer {
    String nodeId;
    double throughput;
    double rtt;
}