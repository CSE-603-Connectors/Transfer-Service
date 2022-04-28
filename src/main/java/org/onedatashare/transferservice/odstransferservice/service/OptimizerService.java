package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.Optimizer;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerCreateRequest;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerDeleteRequest;
import org.onedatashare.transferservice.odstransferservice.model.OptimizerInputRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class OptimizerService {

    @Autowired
    RestTemplate optimizerTemplate;

    HttpHeaders headers;
    public OptimizerService(){
        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

    }

    Logger logger = LoggerFactory.getLogger(OptimizerService.class);

    public Optimizer inputToOptimizerBlocking(OptimizerInputRequest optimizerInputRequest) {
        HttpEntity<OptimizerInputRequest> inputRequestHttpEntity = new HttpEntity<>(optimizerInputRequest, this.headers);
        logger.info(inputRequestHttpEntity.getBody().toString());
        return this.optimizerTemplate.postForObject("/optimizer/input", inputRequestHttpEntity, Optimizer.class);
    }

    public void createOptimizerBlocking(OptimizerCreateRequest optimizerCreateRequest) {
        HttpEntity<OptimizerCreateRequest> createRequestHttpEntity = new HttpEntity<>(optimizerCreateRequest, this.headers);
        logger.info(createRequestHttpEntity.getBody().toString());
        this.optimizerTemplate.postForObject("/optimizer/create", createRequestHttpEntity, Void.class);
    }

    public void deleteOptimizerBlocking(OptimizerDeleteRequest optimizerDeleteRequest) {
        this.optimizerTemplate.postForObject("/optimizer/delete", new HttpEntity<>(optimizerDeleteRequest, this.headers), Void.class);
        logger.info("Deleted {}", optimizerDeleteRequest.toString());
    }
}
