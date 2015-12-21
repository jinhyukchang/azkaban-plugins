package azkaban.jobtype.proxies;

import java.util.concurrent.TimeUnit;

public interface IProxyMetrics {

  public static enum StatsType {
    AVG;
  }

  public void addCount(String key, long count);
  public void elapsed(String key, long elapsed, TimeUnit unit);
  public long getCount(String key);
  public long getLatency(String key, StatsType statsType, TimeUnit unit);
}