package azkaban.jobtype.connectors;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.base.Stopwatch;

import azkaban.jobtype.proxies.IEspressoAsyncProxy;
import azkaban.jobtype.proxies.IProxyMetrics;
import azkaban.jobtype.proxies.IProxyMetrics.StatsType;

public class HdfsToEspressoStatusReportTask extends TimerTask {
  private static Logger log = Logger.getLogger(HdfsToEspressoStatusReportTask.class);
  private static final String STATUS_REPORT_FORMAT =
      "%s %d records(retried %d, failed %d), elapsed %d sec, recent QPS %d, avg QPS %d, avg latency %d ms";

  private long prevCount;
  private long prevTimeStampSec;

  private final IProxyMetrics proxyMetrics;
  private final String operationName;
  private final Stopwatch stopwatch;

  public HdfsToEspressoStatusReportTask(IProxyMetrics proxyMetrics, String operationName) {
    this.proxyMetrics = proxyMetrics;
    this.operationName = operationName;
    this.stopwatch = Stopwatch.createStarted();
  }

  @Override
  public void run() {
    log.info(getStatus());
  }

  private String getStatus() {
    long successCount = proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.PUT_CALL_SUCCESS.name());
    long curCount = proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.PUT_CALL.name());

    long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    long elapsedSec = TimeUnit.MILLISECONDS.toSeconds(elapsedMs);
    elapsedSec = elapsedSec > 0L ? elapsedSec : 1L;

    long recentCount = curCount - prevCount;
    long delta = elapsedSec - prevTimeStampSec;
    prevCount = curCount;
    prevTimeStampSec = elapsedSec;

    return String.format(STATUS_REPORT_FORMAT,
                         operationName,
                         successCount,
                         proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.RETRY.name()),
                         proxyMetrics.getCount(IEspressoAsyncProxy.MetricKey.FAIL.name()),
                         elapsedSec,
                         recentCount / delta,
                         curCount / elapsedSec,
                         proxyMetrics.getLatency(IEspressoAsyncProxy.MetricKey.PUT_CALL.name(), StatsType.AVG, TimeUnit.MILLISECONDS));
  }
}
