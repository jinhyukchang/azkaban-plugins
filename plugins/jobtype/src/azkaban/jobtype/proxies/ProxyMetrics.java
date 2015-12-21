package azkaban.jobtype.proxies;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ProxyMetrics implements IProxyMetrics {

  private final Map<String, AtomicLong> countMap;
  private final Map<String, AtomicLong> latencyRecordCountMap;
  private final Map<String, AtomicLong> totalLatencyMap;

  public ProxyMetrics(Set<String> expectedKeys) {
    countMap = new ConcurrentHashMap<String, AtomicLong>();
    latencyRecordCountMap = new ConcurrentHashMap<String, AtomicLong>();
    totalLatencyMap = new ConcurrentHashMap<String, AtomicLong>();
    initialize(expectedKeys);
  }

  private void initialize(Set<String> expectedKeys) {
    for(String key : expectedKeys) {
      countMap.put(key, new AtomicLong());
      latencyRecordCountMap.put(key, new AtomicLong());
      totalLatencyMap.put(key, new AtomicLong());
    }
  }

  @Override
  public void addCount(String key, long n) {
    AtomicLong count = countMap.get(key);
    if(count != null) {
      count.addAndGet(n);
      return;
    }

    synchronized (countMap) {
      count = countMap.get(key);
      if(count == null) {
        count = new AtomicLong(n);
        countMap.put(key, count);
      } else {
        count.addAndGet(n);
      }
    }
  }

  @Override
  public void elapsed(String key, long elapsed, TimeUnit unit) {
    AtomicLong latencyRecordCount = latencyRecordCountMap.get(key);
    AtomicLong totalLatency = totalLatencyMap.get(key);

    if(latencyRecordCount == null || totalLatency == null) {
      synchronized (totalLatencyMap) {
        if(latencyRecordCount == null) {
          latencyRecordCount = new AtomicLong();
          latencyRecordCountMap.put(key, latencyRecordCount);
        }
        if(totalLatency == null) {
          totalLatency = new AtomicLong();
          totalLatencyMap.put(key, totalLatency);
        }
      }
    }

    synchronized (totalLatency) {
      latencyRecordCount.incrementAndGet();
      totalLatency.addAndGet(unit.convert(elapsed, TimeUnit.MILLISECONDS));
    }
  }

  @Override
  public long getCount(String key) {
    AtomicLong count = countMap.get(key);
    return count == null ? 0L : count.get();
  }

  @Override
  public long getLatency(String key, StatsType statsType, TimeUnit unit) {
    switch (statsType) {
      case AVG:
      {
        AtomicLong latencyRecordCount = latencyRecordCountMap.get(key);
        AtomicLong totalLatency = totalLatencyMap.get(key);

        if (latencyRecordCount == null || latencyRecordCount.get() == 0L
            || totalLatency == null || totalLatency.get() == 0L) {
          return 0L;
        }
        long avgMs = 0L;
        synchronized(totalLatency) {
        avgMs = totalLatency.get() / latencyRecordCount.get();
      }
        return TimeUnit.MILLISECONDS.convert(avgMs, unit);
      }
      default:
        throw new IllegalArgumentException(statsType + " is not supported.");
    }
  }
}
