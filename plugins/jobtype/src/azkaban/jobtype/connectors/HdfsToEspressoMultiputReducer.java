package azkaban.jobtype.connectors;

import java.io.IOException;
import java.util.Iterator;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

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

import azkaban.jobtype.javautils.HadoopUtils;
import azkaban.jobtype.javautils.TokenBucket;
import azkaban.jobtype.javautils.ValidationUtils;
import azkaban.jobtype.proxies.EspressoAsyncMultiputProxy;
import azkaban.jobtype.proxies.EspressoAsyncResponse;
import azkaban.jobtype.proxies.IEspressoAsyncProxy;
import azkaban.jobtype.proxies.IEspressoAsyncProxy.MetricKey;
import azkaban.jobtype.proxies.IProxyMetrics;
import azkaban.jobtype.proxies.ProxyMetrics;

public class HdfsToEspressoMultiputReducer extends MapReduceBase implements Reducer<Text, AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable>{
  private static final Logger log = Logger.getLogger(HdfsToEspressoMultiputReducer.class);
  static {
    HadoopUtils.setLoggerOnNodes();
  }

  private String keyColName;
  private ImmutableList<String> subKeyColNames;
  private ContentType contentType;

  private IEspressoAsyncProxy espressoProxy;
  private IProxyMetrics proxyMetrics;
  private ExecutorService futureValidationThreadPool;
  private Retryer<PutResponse> retryer;
  private Reporter reporter;

  private String tableName;
  private String dbName;
  private final AtomicBoolean isFailFast = new AtomicBoolean(false);

  @Override
  public void configure(JobConf job) {
    keyColName = job.get(HdfsToEspressoConstants.ESPRESSO_KEY);
    ValidationUtils.validateNotEmpty(keyColName, HdfsToEspressoConstants.ESPRESSO_KEY);
    subKeyColNames = parseSubKeysString(job.get(HdfsToEspressoConstants.ESPRESSO_SUBKEY));
    ValidationUtils.validateNotEmpty(subKeyColNames, HdfsToEspressoConstants.ESPRESSO_SUBKEY);
    contentType = ContentType.valueOf(job.get(HdfsToEspressoConstants.ESPRESSO_CONTENT_TYPE_KEY));

    this.proxyMetrics = new ProxyMetrics(MetricKey.metricKeys());
    retryer = RetryerBuilder.<PutResponse> newBuilder()
        .retryIfException(new Predicate<Throwable>() { //Retry except client side error
          @Override
          public boolean apply(Throwable t) {
            return !(t instanceof IllegalArgumentException);
          }
        })
        //multiply 2000 to the delay on each retry, where maximum delay between retry is 30 sec
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

    int numThread = job.getInt(HdfsToEspressoConstants.MR_NUM_THREAD_KEY, HdfsToEspressoConstants.MR_NODE_MAX_THREADS);
    log.info("Number of thread in this mapper: " + numThread);

    int overallQps = job.getInt(HdfsToEspressoConstants.ESPRESSO_PUT_QPS_KEY, -1);
    if (overallQps <= 0) {
      throw new IllegalArgumentException(HdfsToEspressoConstants.ESPRESSO_PUT_QPS_KEY + " should be greater than zero.");
    }

    int numReducer = job.getInt("mapreduce.job.reduces", -1);
    if(numReducer <= 0) {
      throw new IllegalStateException("Cannot retrieve information about number of reducers.");
    }

    int nodeQps = overallQps / numReducer;
    if(nodeQps == 0) {
      nodeQps = 1;
    }

    TokenBucket tokenBuket = new TokenBucket(nodeQps);
    log.info("Target QPS from this node: " + nodeQps + ". Overall job QPS: " + overallQps);

    espressoProxy = new EspressoAsyncMultiputProxy(client, numThread, dbName, tableName, proxyMetrics, retryer, tokenBuket);
    futureValidationThreadPool = new ThreadPoolExecutor(numThread,
                                                        numThread,
                                                        60,
                                                        TimeUnit.SECONDS,
                                                        new ArrayBlockingQueue<Runnable>(HdfsToEspressoConstants.MR_NODE_MAX_THREADS),
                                                        new ThreadPoolExecutor.CallerRunsPolicy());

    new Timer(true).schedule(new HdfsToEspressoStatusReportTask(proxyMetrics, "Put"),
                             TimeUnit.SECONDS.toMillis(10L),
                             TimeUnit.SECONDS.toMillis(10L));
  }

  private ImmutableList<String> parseSubKeysString(String subKeysStr) {
    if(org.apache.commons.lang.StringUtils.isEmpty(subKeysStr)) {
      return null;
    }
    String[] subKeysArr = subKeysStr.split(",");
    if(subKeysArr.length == 0) {
      return null;
    }
    return ImmutableList.<String>builder().add(subKeysArr).build();
  }

  @Override
  public void reduce(Text key, Iterator<AvroWrapper<GenericRecord>> vals,
                     OutputCollector<AvroWrapper<GenericRecord>, NullWritable> output, Reporter reporter) throws IOException {

    if(isFailFast.get()) {
      throw new RuntimeException("Too many failures. Failing the task.");
    }

    if(this.reporter == null) {
      this.reporter = reporter;
      reporter.getCounter(IEspressoAsyncProxy.MetricKey.FAIL).increment(0L); //Initialize fail counter
      reporter.getCounter(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS).increment(0L);
    }

    while(vals.hasNext()) {
      if(isFailFast.get()) {
        throw new RuntimeException("Too many failures. Failing the task.");
      }
      AvroWrapper<GenericRecord> avroWrapper = vals.next();
      GenericRecord genericRecord = avroWrapper.datum();
      String entryKey = String.valueOf(genericRecord.get(keyColName));

      ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
      for(String colName : subKeyColNames) {
        listBuilder.add(String.valueOf(genericRecord.get(colName)));
      }
      EspressoEntry entry = new EspressoEntry(avroWrapper, genericRecord.toString(), entryKey, contentType, listBuilder.build());

      Optional<EspressoAsyncResponse<EspressoEntry,PutResponse>> asyncResponse = espressoProxy.putAsync(entry);
      if(asyncResponse.isPresent()) {
        futureValidationThreadPool.execute(new EspressoPutRequestFutureValidator(asyncResponse.get(), output));
      }
    }
  }

  @Override
  public void close() {
    try {
      log.info("Reducer completed processing. Closing resources.");
      espressoProxy.close();
      futureValidationThreadPool.shutdown();
      futureValidationThreadPool.awaitTermination(3L, TimeUnit.MINUTES);
      log.info("Reducer finished closing resources.");
    } catch (Exception e) {
      throw new RuntimeException("Recuder failed closing resources", e);
    }
  }

  private class EspressoPutRequestFutureValidator implements Runnable {
    private final EspressoAsyncResponse<EspressoEntry,PutResponse> asyncResponse;
    private final OutputCollector<AvroWrapper<GenericRecord>, NullWritable> output;

    public EspressoPutRequestFutureValidator(EspressoAsyncResponse<EspressoEntry,PutResponse> asyncResponse,
                                             OutputCollector<AvroWrapper<GenericRecord>, NullWritable> output) {
      this.asyncResponse = asyncResponse;
      this.output = output;
    }

    @Override
    public void run() {
      try {
        asyncResponse.getResponse().get();
        proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS.name(), 1L);
        reporter.getCounter(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS).increment(1L);
      } catch (Exception e) {
        log.error("Put request failed. This will be recorded in error.path. Entries: " + asyncResponse.getRequests(), e);
        proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.FAIL.name(), 1L);
        reporter.getCounter(IEspressoAsyncProxy.MetricKey.FAIL).increment(1L);
        if (isFailFast()) {
          isFailFast.set(true);
          log.info("Requesting to fail as there are too many failures.");
        }

        for(EspressoEntry entry : asyncResponse.getRequests()) {
          try {
            synchronized (output) { //Collector is not thread safe
              output.collect(entry.getSrc(), NullWritable.get());
            }
          } catch (IOException e1) {
            log.error("Failed to collect error record. Making the task to fail.", e1);
            isFailFast.set(true);
          }
        }
        return;
      }
    }

    private boolean isFailFast() {
      long totalCount = proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.PUT_CALL.name());
      long failure = proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.FAIL.name());
      return totalCount >= HdfsToEspressoConstants.FAIL_FAST_REQUEST_THRESHOLD
          && (failure >= HdfsToEspressoConstants.FAIL_FAST_NUM_FAILURE
              || ((failure * 100 / totalCount) > HdfsToEspressoConstants.FAIL_FAST_PERCENTAGE));
    }
  }
}
