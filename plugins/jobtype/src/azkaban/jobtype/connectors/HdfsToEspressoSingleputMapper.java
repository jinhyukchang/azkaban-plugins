package azkaban.jobtype.connectors;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import azkaban.jobtype.connectors.EspressoEntry;
import azkaban.jobtype.connectors.HdfsToEspressoConstants;
import azkaban.jobtype.javautils.HadoopUtils;
import azkaban.jobtype.javautils.TokenBucket;
import azkaban.jobtype.proxies.EspressoAsyncResponse;
import azkaban.jobtype.proxies.EspressoAsyncSingleputProxy;
import azkaban.jobtype.proxies.IEspressoAsyncProxy;
import azkaban.jobtype.proxies.IEspressoAsyncProxy.MetricKey;
import azkaban.jobtype.proxies.IProxyMetrics;
import azkaban.jobtype.proxies.ProxyMetrics;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.linkedin.espresso.client.EspressoClient;
import com.linkedin.espresso.client.r2d2impl.R2D2EspressoClient;
import com.linkedin.espresso.pub.ContentType;
import com.linkedin.espresso.pub.operations.PutResponse;

public class HdfsToEspressoSingleputMapper extends MapReduceBase implements
    Mapper<AvroWrapper<GenericRecord>, NullWritable, AvroWrapper<GenericRecord>, NullWritable> {
  private static final Logger log = Logger.getLogger(HdfsToEspressoSingleputMapper.class);
  static {
    HadoopUtils.setLoggerOnNodes();
  }

  private IEspressoAsyncProxy espressoProxy;
  private ExecutorService futureValidationThreadPool;
  private Retryer<PutResponse> retryer;
  private IProxyMetrics proxyMetrics;
  private Reporter reporter;

  private String tableName;
  private String dbName;
  private ContentType contentType;
  private String keyColName;
  private Optional<ImmutableList<String>> subKeyColNames;
  private boolean isFailFast;

  @Override
  public void configure(JobConf job) {
    this.proxyMetrics = new ProxyMetrics(MetricKey.metricKeys());
    retryer = RetryerBuilder.<PutResponse> newBuilder()
        .retryIfException(new Predicate<Throwable>() {
          @Override
          public boolean apply(Throwable t) { //Retry except client side error
            return !(t instanceof IllegalArgumentException);
          }
        })
        //multiply 2000ms to the delay on each retry, where maximum delay between retry can go up to 30 sec
        .withWaitStrategy(WaitStrategies.exponentialWait(2000, 30000, TimeUnit.MILLISECONDS))
        //If overall delay is 2 minutes, stop retrying.
        .withStopStrategy(StopStrategies.stopAfterDelay(2, TimeUnit.MINUTES)).withRetryListener(new RetryListener() {
          @Override
          public <V> void onRetry(Attempt<V> attempt) {
            if (attempt.getAttemptNumber() > 1L) {
              proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.RETRY.name(), 1L);
            }
          }
        }).build();

    String endPoint = job.get(HdfsToEspressoConstants.ESPRESSO_ENDPOINT_KEY);
    R2D2EspressoClient.Config clientConfig = new R2D2EspressoClient.Config();
    clientConfig.setUriPrefix(endPoint);
    EspressoClient client = new R2D2EspressoClient(clientConfig);

    dbName = job.get(HdfsToEspressoConstants.ESPRESSO_DB_KEY);
    tableName = job.get(HdfsToEspressoConstants.ESPRESSO_TABLE_KEY);
    contentType = ContentType.valueOf(job.get(HdfsToEspressoConstants.ESPRESSO_CONTENT_TYPE_KEY));
    keyColName = job.get(HdfsToEspressoConstants.ESPRESSO_KEY);
    subKeyColNames = Optional.fromNullable(parseSubKeysString(job.get(HdfsToEspressoConstants.ESPRESSO_SUBKEY)));

    int numThread = job.getInt(HdfsToEspressoConstants.MR_NUM_THREAD_KEY, HdfsToEspressoConstants.MR_NODE_MAX_THREADS);
    log.info("Number of thread in this mapper: " + numThread);

    int overallQps = job.getInt(HdfsToEspressoConstants.ESPRESSO_PUT_QPS_KEY, -1);
    if (overallQps <= 0) {
      throw new IllegalArgumentException(HdfsToEspressoConstants.ESPRESSO_PUT_QPS_KEY + " should be greater than zero.");
    }

    int numMapper = job.getInt("mapreduce.job.maps", -1);
    if(numMapper <= 0) {
      throw new IllegalStateException("Cannot retrieve information about number of mappers.");
    }

    int nodeQps = overallQps / numMapper;
    if(nodeQps == 0) {
      nodeQps = 1;
    }

    TokenBucket tokenBuket = new TokenBucket(nodeQps);
    log.info("Target QPS from this node: " + nodeQps + ". Overall job QPS: " + overallQps);

    espressoProxy = new EspressoAsyncSingleputProxy(client, numThread, dbName, tableName, proxyMetrics, retryer, tokenBuket);
    futureValidationThreadPool = new ThreadPoolExecutor(numThread,
                                                        numThread,
                                                        60,
                                                        TimeUnit.SECONDS,
                                                        new ArrayBlockingQueue<Runnable>(HdfsToEspressoConstants.MR_NODE_MAX_THREADS),
                                                        new ThreadPoolExecutor.CallerRunsPolicy());

    new Timer(true).schedule(new HdfsToEspressoStatusReportTask(proxyMetrics, "Put"), TimeUnit.SECONDS.toMillis(10L), TimeUnit.SECONDS.toMillis(10L));
  }

  private ImmutableList<String> parseSubKeysString(String subKeysStr) {
    if(StringUtils.isEmpty(subKeysStr)) {
      return null;
    }
    String[] subKeysArr = subKeysStr.split(",");
    if(subKeysArr.length == 0) {
      return null;
    }
    return ImmutableList.<String>builder().add(subKeysArr).build();
  }

  @Override
  public void map(AvroWrapper<GenericRecord> key, NullWritable value, OutputCollector<AvroWrapper<GenericRecord>, NullWritable> collector,
      Reporter reporter) throws IOException {

    if(isFailFast) {
      throw new RuntimeException("Too many failures. Failing the task.");
    }
    if(this.reporter == null) {
      this.reporter = reporter;
      reporter.getCounter(IEspressoAsyncProxy.MetricKey.FAIL).increment(0L);
      reporter.getCounter(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS).increment(0L);
    }

    GenericRecord genericRecord = key.datum();
    String entryKey = String.valueOf(genericRecord.get(keyColName));
    if (StringUtils.isEmpty(entryKey)) {
      log.warn("Record does not have primary key. Skipping: " + genericRecord);
      proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.FAIL.name(), 1L);
      return;
    }

    String content = genericRecord.toString();
    EspressoEntry entry = null;
    if (subKeyColNames.isPresent()) {
      ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
      for(String colName : subKeyColNames.get()) {
        listBuilder.add(String.valueOf(genericRecord.get(colName)));
      }
      entry = new EspressoEntry(key, content, entryKey, contentType, listBuilder.build());
    } else {
      entry = new EspressoEntry(key, content, entryKey, contentType);
    }

    Optional<EspressoAsyncResponse<EspressoEntry,PutResponse>> asyncResponse = espressoProxy.putAsync(entry);

    futureValidationThreadPool.execute(new EspressoPutRequestFutureValidator(asyncResponse.get(), collector));
  }

  @Override
  public void close() {
    log.info("Mapper is closing. Closing all the resources.");
    try {
      espressoProxy.close();
      futureValidationThreadPool.shutdown();
      futureValidationThreadPool.awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      log.warn("Thread interrupted while waiting for thread pool termination.", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed while closing resources", e);
    } finally {
      log.info("Mapper completed");
    }
  }

  private class EspressoPutRequestFutureValidator implements Runnable {
    private final EspressoAsyncResponse<EspressoEntry,PutResponse> asyncResponse;
    private final OutputCollector<AvroWrapper<GenericRecord>, NullWritable> collector;

    public EspressoPutRequestFutureValidator(EspressoAsyncResponse<EspressoEntry,PutResponse> asyncResponse,
                                             OutputCollector<AvroWrapper<GenericRecord>, NullWritable> collector) {
      this.asyncResponse = asyncResponse;
      this.collector = collector;
    }

    @Override
    public void run() {
      try {
        asyncResponse.getResponse().get();
        proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS.name(), 1L);
        reporter.getCounter(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS).increment(1L);
      } catch (Exception e) {
        log.error("Put request failed on entry(ies) " + asyncResponse.getRequests(), e); //TODO Add record error part
        proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.FAIL.name(), 1L);
        reporter.getCounter(IEspressoAsyncProxy.MetricKey.FAIL).increment(1L);
        isFailFast |= isFailFast();
        log.info("isFailFast: " + isFailFast);
        for(EspressoEntry entry : asyncResponse.getRequests()) {
          try {
            synchronized (collector) { //Collector is not thread safe
              collector.collect(entry.getSrc(), NullWritable.get());
            }
          } catch (IOException e1) {
            log.error("Failed to collect error record. Making the task to fail.", e1);
            isFailFast = true;
          }
        }
        return;
      }
    }

    private boolean isFailFast() {
      long totalCount = proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.PUT_CALL.name());
      long failCount = proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.FAIL.name());
      log.info("total count: " + totalCount + " , fail count: " + failCount);
      return totalCount >= HdfsToEspressoConstants.FAIL_FAST_REQUEST_THRESHOLD
          && (failCount >= HdfsToEspressoConstants.FAIL_FAST_NUM_FAILURE
              || ((failCount * 100 / totalCount) > HdfsToEspressoConstants.FAIL_FAST_PERCENTAGE));
    }
  }
}
