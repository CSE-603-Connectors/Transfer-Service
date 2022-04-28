package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import com.google.api.client.json.Json;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import com.google.gson.stream.JsonReader;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.tomcat.util.json.JSONParser;
import org.onedatashare.transferservice.odstransferservice.config.CommandLineOptions;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.PmeterMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.utility.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author deepika
 */
@Service
public class NetworkMetricServiceImpl implements NetworkMetricService {

    private Logger LOG = LoggerFactory.getLogger(NetworkMetricService.class);
    public static final String SINGLE_QUOTE = "'";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private long bytesSentOld;
    private long bytesReceivedOld;

    private long packetsSentOld;
    private long packetsReceivedOld;

    @Value("${pmeter.path}")
    String scriptPath;

    @Value("${pmeter.data}")
    String pmeterDataPath;

    @Value("${pmeter.file}")
    String pmeterFile;

    @Autowired
    Gson gson;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private CommandLineOptions cmdLineOptions;
    @Autowired
    private DataInflux dataInflux;

    private final String MEASURE = "measure";

    public NetworkMetricServiceImpl(){
        this.bytesSentOld = 0L;
        this.bytesReceivedOld = 0L;
        this.packetsReceivedOld = 0L;
        this.packetsSentOld = 0L;
    }

    @Override
    public NetworkMetric saveOrUpdate(NetworkMetric networkMetric) {
        try {
            LOG.info("Saving");
            StringBuilder stringBuilder = new StringBuilder("insert into network_metric (data, start_time, end_time) values(");
            stringBuilder.append("'");
            stringBuilder.append(networkMetric.getData());
            stringBuilder.append("',");
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(DataUtil.getStringDate(networkMetric.getStartTime()));
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(",");
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(DataUtil.getStringDate(networkMetric.getEndTime()));
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(")");
            LOG.info("sql insert: " + stringBuilder.toString());
            jdbcTemplate.execute(stringBuilder.toString());

        } catch (Exception ex) {
            ex.getMessage();
        }
        return null;
    }

    @Override
    public NetworkMetric readFile() {
        Date startTime = null;
        Date endTime = null;

        File inputFile = Paths.get(this.pmeterDataPath, this.pmeterFile).toFile();
        File tempFile = new File(ODSConstants.PMETER_TEMP_REPORT);
        LOG.info(inputFile.getPath());
        NetworkMetric networkMetric = new NetworkMetric();
        List<PmeterMetric> pmeterMetrics = new ArrayList<>();
        try(JsonReader jsonReader = new JsonReader(new FileReader(Paths.get(this.pmeterDataPath, this.pmeterFile).toString()))){
            pmeterMetrics = gson.fromJson(jsonReader, PmeterMetric.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(PmeterMetric metric : pmeterMetrics){
            LOG.info(metric.toString());
        }
        try(BufferedReader r = new BufferedReader(new FileReader(inputFile))) {

            JsonStreamParser p = new JsonStreamParser(r);
            List<Map<?, ?>> metricList = new ArrayList<>();
            while (p.hasNext()) {
                LOG.info(p.toString());
                JsonElement metric = p.next();
                if (metric.isJsonObject()) {
                    Map<?, ?> map = new Gson().fromJson(metric, Map.class);
                    startTime = DataUtil.getDate((String)map.get("start_time"));
                    endTime = DataUtil.getDate((String)map.get("end_time"));
                    metricList.add(map);
                }
            }
            networkMetric.setData(new Gson().toJson(metricList));
            networkMetric.setStartTime(startTime);
            networkMetric.setEndTime(endTime);
        }catch (IOException | ParseException e){
            LOG.error("Exception occurred while reading file",e);
        }
        inputFile.delete();
        tempFile.renameTo(inputFile);

        return networkMetric;
    }


    @Override
    public void executeScript() throws Exception {
        CommandLine cmdLine = CommandLine.parse(
                String.format("python3 %s " + MEASURE + " %s --user %s --length %s --measure %s %s",
                        scriptPath, cmdLineOptions.getNetworkInterface(), cmdLineOptions.getUser(),
                        cmdLineOptions.getLength(), cmdLineOptions.getMeasure(),cmdLineOptions.getOptions()));
        LOG.info("The python command to execute is {}",cmdLine.toString());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);
        executor.execute(cmdLine);
        LOG.info(outputStream.toString());
    }

    @Override
    public DataInflux mapData(NetworkMetric networkMetric) {
        if (networkMetric.getData() != null) {

            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.setDateFormat(TIMESTAMP_FORMAT);

            DataInflux[] dataArr = gsonBuilder.create().fromJson(networkMetric.getData(), DataInflux[].class);
            dataInflux = dataArr[dataArr.length - 1];
            float[] l = dataInflux.getLatencyArr();
            dataInflux.setLatencyVal(l[0]);
            getDeltaValueByMetric();
            mapCpuFrequency();
        }
        if (networkMetric.getJobData() == null) {
            networkMetric.setJobData(new JobMetric());
        }
        setJobData(networkMetric, dataInflux);

        return dataInflux;
    }

    private void mapCpuFrequency() {
        if (dataInflux.getCpuFrequency() != null && dataInflux.getCpuFrequency().length != 0) {
            dataInflux.setCurrCpuFrequency(dataInflux.getCpuFrequency()[0]);
            dataInflux.setMinCpuFrequency(dataInflux.getCpuFrequency()[1]);
            dataInflux.setMaxCpuFrequency(dataInflux.getCpuFrequency()[2]);
        }
    }

    private void getDeltaValueByMetric() {

        dataInflux.setBytesSentDelta(bytesSentOld != 0
                ? dataInflux.getBytesSent() - bytesSentOld
                : bytesSentOld);
        bytesSentOld = dataInflux.getBytesSent();

        dataInflux.setBytesReceivedDelta(bytesReceivedOld != 0
                ? dataInflux.getBytesReceived() - bytesReceivedOld
                : bytesReceivedOld);
        bytesReceivedOld = dataInflux.getBytesReceived();

        dataInflux.setPacketsSentDelta(packetsSentOld != 0
                ? dataInflux.getPacketSent() - packetsSentOld
                : packetsSentOld);
        packetsSentOld = dataInflux.getPacketSent();

        dataInflux.setPacketsReceivedDelta(packetsReceivedOld != 0
                ? dataInflux.getPacketReceived() - packetsReceivedOld
                : packetsReceivedOld);
        packetsReceivedOld = dataInflux.getPacketReceived();

    }

    private void setJobData(NetworkMetric networkMetric, DataInflux dataInflux) {
        JobMetric jobMetric = networkMetric.getJobData();
        dataInflux.setConcurrency(jobMetric.getConcurrency());
        dataInflux.setParallelism(jobMetric.getParallelism());
        dataInflux.setPipelining(jobMetric.getPipelining());
        dataInflux.setThroughput(jobMetric.getThroughput());
        dataInflux.setJobId(jobMetric.getJobId());
    }
}
