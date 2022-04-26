package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.Optimizer;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerCreateReqeust;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerDeleteRequest;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerInputRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class OptimizerService {

    @Autowired
    RestTemplate optimizerTemplate;

    public Optimizer inputToOptimizerBlocking(OptimizerInputRequest optimizerInputRequest) {
        return this.optimizerTemplate.postForObject("/optimizer/input", new HttpEntity<>(optimizerInputRequest), Optimizer.class);
    }

    public void createOptimizerBlocking(OptimizerCreateReqeust optimizerCreateReqeust) {
        this.optimizerTemplate.postForObject("/optimizer/create", new HttpEntity<>(optimizerCreateReqeust), Void.class);
    }

    public void deleteOptimizerBlocking(OptimizerDeleteRequest optimizerDeleteRequest) {
        this.optimizerTemplate.postForEntity("/optimizer/delete", new HttpEntity<>(optimizerDeleteRequest), Void.class);
    }
}
