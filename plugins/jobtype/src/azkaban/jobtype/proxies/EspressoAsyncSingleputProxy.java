package azkaban.jobtype.proxies;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import azkaban.jobtype.connectors.EspressoEntry;
import azkaban.jobtype.connectors.HdfsToEspressoConstants;
import azkaban.jobtype.javautils.TokenBucket;

import com.github.rholder.retry.Retryer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.linkedin.espresso.client.EspressoClient;
import com.linkedin.espresso.pub.ResponseStatus;
import com.linkedin.espresso.pub.operations.PutRequest;
import com.linkedin.espresso.pub.operations.PutRequest.Builder;
import com.linkedin.espresso.pub.operations.PutResponse;

public class EspressoAsyncSingleputProxy implements IEspressoAsyncProxy {
  private static Logger log = Logger.getLogger(EspressoAsyncSingleputProxy.class);

  private final EspressoClient client;
  private final ExecutorService threadPool;
  private final Retryer<PutResponse> retryer;
  private final TokenBucket tokenBucket;
  private final String databaseName;
  private final String tableName;
  private final IProxyMetrics proxyMetrics;
  private volatile boolean isRunning;

  public EspressoAsyncSingleputProxy(EspressoClient client,
                                    int threadPoolSize,
                                    String databaseName,
                                    String tableName,
                                    IProxyMetrics proxyMetrics,
                                    Retryer<PutResponse> retryer,
                                    TokenBucket tokenBucket) {
    this.client = client;
    int queueCapacity = Math.min(threadPoolSize, HdfsToEspressoConstants.MR_NODE_MAX_THREADS);
    threadPool = new ThreadPoolExecutor(threadPoolSize,
                                        threadPoolSize,
                                        1L,
                                        TimeUnit.MINUTES,
                                        new ArrayBlockingQueue<Runnable>(queueCapacity),
                                        new ThreadPoolExecutor.CallerRunsPolicy());
    this.retryer = retryer;
    this.tokenBucket = tokenBucket;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.proxyMetrics = proxyMetrics;
    this.isRunning = true;
  }

  @Override
  public void close() {
    try {
      isRunning = false;
      threadPool.shutdown();
      boolean isGracefullyTerminated = threadPool.awaitTermination(3L, TimeUnit.MINUTES);
      if (!isGracefullyTerminated) {
        log.warn("Thread pool has been terminated and some tasks have been disrupted.");
      }
      client.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<EspressoAsyncResponse<EspressoEntry, PutResponse>> putAsync(EspressoEntry entry) {
    if(!isRunning) {
      throw new RuntimeException(this.getClass().getSimpleName() + " instance is already closed.");
    }

    Future<PutResponse> f = threadPool.submit(newRequester(entry));
    return Optional.of(new EspressoAsyncResponse<EspressoEntry, PutResponse>(ImmutableList.of(entry), f));
  }

  @Override
  public IProxyMetrics getProxyMetrics() {
    return proxyMetrics;
  }

  private Callable<PutResponse> newRequester(final EspressoEntry entry) {
    return retryer.wrap(new Callable<PutResponse>() {

      @Override
      public PutResponse call() throws Exception {
        Builder builder = PutRequest.builder();
        builder.setDatabase(databaseName)
               .setTable(tableName)
               .setContent(entry.getContentType(), entry.getContent());

        if(entry.getSubKeys().isPresent()) {
          String[] subKeys = new String[entry.getSubKeys().get().size() + 1];
          subKeys[0] = entry.getKey();
          Iterator<String> it = entry.getSubKeys().get().iterator();
          for(int i=1; i < subKeys.length; i++) {
            subKeys[i] = it.next();
          }
          builder.setKey(subKeys);
        } else {
          builder.setKey(entry.getKey());
        }

        tokenBucket.acquire();
        proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.PUT_CALL.name(), 1L);
        long start = System.currentTimeMillis();
        PutResponse response = client.execute(builder.build());
        proxyMetrics.elapsed(IEspressoAsyncProxy.MetricKey.PUT_CALL.name(), System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        if (ResponseStatus.BAD_REQUEST.equals(response.getResponseStatus())) {
          throw new IllegalArgumentException("Response status failed: " + response.getResponseStatus() + " : " + response.getEspressoErrorMessage());
        }
        if (!ResponseStatus.OK.equals(response.getResponseStatus()) && !ResponseStatus.CREATED.equals(response.getResponseStatus())) {
          throw new RuntimeException("Response status failed: " + response.getResponseStatus());
        }
        return response;
      }
    });
  }
}
