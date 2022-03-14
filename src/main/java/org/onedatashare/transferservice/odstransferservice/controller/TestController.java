package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author deepika
 */
@Component
@RequestMapping("/test")
public class TestController {

    @Autowired
    NetworkMetricServiceImpl networkMetricService;

    @Autowired
    JobControl jobControl;

//    private DataSource dataSource;

//    @Autowired
//    JdbcTemplate jdbcTemplate;

//    final String INSERT_MESSAGE_SQL = "insert into network_metric (id) values(?) ";


//    @Autowired(required = false)
//    public void setDatasource(DataSource datasource) {
//        this.dataSource = datasource;
//    }

    Logger logger = LoggerFactory.getLogger(TestController.class);

    @RequestMapping(value = "/cron", method = RequestMethod.POST)
    public ResponseEntity<String> cron() throws Exception {
        logger.info("Controller Entry point");

        NetworkMetric networkMetric = new NetworkMetric();
        networkMetric.setId(3L);
        networkMetricService.save(networkMetric);
        networkMetricService.find();
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: ");
    }

    @Scheduled(cron = "0 0/1 * * * *")
    @Transactional(propagation= Propagation.REQUIRES_NEW)
    public void collectAndSave() throws Exception {
        //cron();
        logger.info("Collecting");
        NetworkMetric networkMetric = new NetworkMetric();
        networkMetric.setId(3L);
        networkMetricService.save(networkMetric);
//        save();

        networkMetricService.find();
    }


    /**
    public void save(){
        SimpleJdbcInsert simpleJdbcInsert =
                new SimpleJdbcInsert(dataSource).withTableName("network_metric");
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("id", "4");
//        NetworkMetric networkMetric = new NetworkMetric();
//        networkMetric.setId("4");
//        networkMetricService.saveOrUpdate(networkMetric);

        KeyHolder keyHolder = new GeneratedKeyHolder();

//        jdbcTemplate.update(connection -> {
//            PreparedStatement ps = connection.prepareStatement(INSERT_MESSAGE_SQL);
//            ps.setString(1, "5");
//            return ps;
//        }, keyHolder);

        logger.info("Inserted: " + keyHolder.getKey());
//        jdbcTemplate.execute(INSERT_MESSAGE_SQL, );
//        simpleJdbcInsert.execute(parameters);
    }
    **/
}
