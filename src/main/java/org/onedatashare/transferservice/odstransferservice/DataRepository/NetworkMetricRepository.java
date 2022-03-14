package org.onedatashare.transferservice.odstransferservice.DataRepository;

import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author deepika
 */
@Repository
@Transactional("transactionManager")
public interface NetworkMetricRepository extends JpaRepository<NetworkMetric, Long> {

}
