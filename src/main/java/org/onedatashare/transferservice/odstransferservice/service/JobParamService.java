package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.StaticVar;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

@Service
public class JobParamService {
    Logger logger = LoggerFactory.getLogger(JobParamService.class);

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        logger.info("request received : "+request.toString());
        builder.addLong(TIME, System.currentTimeMillis());
        builder.addString(SOURCE_CREDENTIAL_ID, request.getSource().getCredId());
        builder.addString(DEST_CREDENTIAL_ID, request.getDestination().getCredId());
        builder.addString(SOURCE_BASE_PATH, request.getSource().getInfo().getPath());
        builder.addString(DEST_BASE_PATH, request.getDestination().getInfo().getPath());
//        builder.addString(INFO_LIST, request.getSource().getInfoList().toString());
        builder.addString(SOURCE_ACCOUNT_ID_PASS, request.getSource().getCredential().getAccountId() + ":" + "xxx");// request.getSource().getCredential().getPassword());
        builder.addString(DESTINATION_ACCOUNT_ID_PASS, request.getDestination().getCredential().getAccountId() + ":" + "xxx");// request.getDestination().getCredential().getPassword());
        builder.addString(PRIORITY, String.valueOf(request.getPriority()));
        builder.addString(OWNER_ID, request.getOwnerId());
        return builder.toJobParameters();
    }

    public void setStaticVar(TransferJobRequest request) {
        logger.info("Setting Static Variables...");
        Map<String, Long> hm = new HashMap<>();
        //TODO: Must remove these and find way to pass to reader and writer
        StaticVar.sPass = request.getSource().getCredential().getPassword();
        StaticVar.dPass = request.getDestination().getCredential().getPassword();

        for (EntityInfo ei : request.getSource().getInfoList()) {
            hm.put(ei.getPath(), ei.getSize());
        }
        StaticVar.setHm(hm);
    }
}
