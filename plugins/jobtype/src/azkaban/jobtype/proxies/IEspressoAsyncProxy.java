package azkaban.jobtype.proxies;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import azkaban.jobtype.connectors.EspressoEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.linkedin.espresso.pub.operations.PutResponse;

public interface IEspressoAsyncProxy extends Closeable {
  public static enum MetricKey {
    PUT_CALL,
    PUT_CALL_SUCCESS,
    RETRY,
    FAIL;

    private static final Set<String> METRIC_KEY_SET;
    static {
      Set<String> tmp = new HashSet<String>();
      for(MetricKey mk : MetricKey.values()) {
        tmp.add(mk.name());
      }
      METRIC_KEY_SET = ImmutableSet.copyOf(tmp);
    }
    public static Set<String> metricKeys() {
      return METRIC_KEY_SET;
    }
  }

  public Optional<EspressoAsyncResponse<EspressoEntry, PutResponse>> putAsync(EspressoEntry entry);
  public IProxyMetrics getProxyMetrics();
}
