package azkaban.jobtype.proxies;

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
import com.linkedin.espresso.pub.operations.PutResponse;
import com.linkedin.espresso.pub.operations.TxMultiPutRequest.Builder;
import com.linkedin.espresso.pub.operations.TxMultiPutRequest;
import com.linkedin.espresso.pub.operations.TxMultiPutRequest.Part;

public class EspressoAsyncMultiputProxy implements IEspressoAsyncProxy {
  private static final Logger log = Logger.getLogger(EspressoAsyncMultiputProxy.class);

  private final EspressoClient client;
  private final ExecutorService threadPool;
  private final Retryer<PutResponse> retryer;
  private final TokenBucket tokenBucket;
  private final String databaseName;
  private final String tableName;
  private final IProxyMetrics proxyMetrics;
  private volatile boolean isRunning;

  private Builder currentBuilder;
  private String currentKey;
  private ImmutableList.Builder<EspressoEntry> currentEntriesBuilder;
  private int currentSize;

  private final static int BATCH_MAX_COUNT = 10;
  private final static int BATCH_CONTENT_MAX_LENGTH = 1024 * 1024; //Shamelessly got it from Lynda's code.

  public EspressoAsyncMultiputProxy(EspressoClient client,
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
      if(currentBuilder != null) {
        Future<PutResponse> f = threadPool.submit(newRequester());
        try {
          f.get(); //We are not returning this future, thus need to validate the result.
        } catch (Exception e) {
          log.error("Last put request has been failed. ", e);
        }
      }
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
    if(currentBuilder == null) {
      resetBatch(entry.getKey());
    }

    Optional<EspressoAsyncResponse<EspressoEntry, PutResponse>> result = Optional.absent();
    if(isBatchCallNeeded(entry.getKey())) {
      Future<PutResponse> f = threadPool.submit(newRequester());
      result = Optional.of(new EspressoAsyncResponse<EspressoEntry, PutResponse>(currentEntriesBuilder.build(), f));
      resetBatch(entry.getKey());
    }

    Part.Builder partBuilder = TxMultiPutRequest.Part.builder()
        .setTable(tableName)
        .setContent(entry.getContentType(), entry.getContent());
    if(entry.getSubKeys().isPresent()) {
      String[] subKeys = new String[entry.getSubKeys().get().size()];
      subKeys = entry.getSubKeys().get().toArray(subKeys);
      partBuilder.setSubkey(subKeys);
    }
    currentBuilder.addPart(partBuilder.build());
    currentSize += entry.getContent().length();
    currentEntriesBuilder.add(entry);

    return result;
  }

  private void resetBatch(String key) {
    currentKey = key;
    currentSize = 0;
    currentBuilder = TxMultiPutRequest.builder().setDatabase(databaseName).setKey(key);
    currentEntriesBuilder = ImmutableList.builder();
  }

  /**
   * Pass the test when either of these two or both.
   * 1. Key is switched
   * 2. Either content size or batch size exceeded
   *
   * @return
   */
  private boolean isBatchCallNeeded(String nextKey) {
    int batchCount = currentBuilder.getParts() == null ? 0 : currentBuilder.getParts().size();
    return !currentKey.equals(nextKey)
          || (currentSize >= BATCH_CONTENT_MAX_LENGTH)
          || (batchCount >= BATCH_MAX_COUNT);
  }

  @Override
  public IProxyMetrics getProxyMetrics() {
    return proxyMetrics;
  }

  private Callable<PutResponse> newRequester() {
    return retryer.wrap(new Callable<PutResponse>() {
      final int numParts = currentBuilder.getParts().size();
      final TxMultiPutRequest req = currentBuilder.build();

      @Override
      public PutResponse call() throws Exception {
        tokenBucket.acquire(numParts);
        proxyMetrics.addCount(IEspressoAsyncProxy.MetricKey.PUT_CALL.name(), numParts);
        long start = System.currentTimeMillis();
        PutResponse response = client.execute(req);
        if (ResponseStatus.BAD_REQUEST.equals(response.getResponseStatus())) {
          throw new IllegalArgumentException("Response status failed: " + response.getResponseStatus() + " : " + response.getEspressoErrorMessage());
        }

        proxyMetrics.elapsed(IEspressoAsyncProxy.MetricKey.PUT_CALL.name(), System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        if (!ResponseStatus.OK.equals(response.getResponseStatus()) && !ResponseStatus.CREATED.equals(response.getResponseStatus())) {
          throw new RuntimeException("Response status failed: " + response.getResponseStatus() + " : " + response.getEspressoErrorMessage());
        }
        return response;
      }
    });
  }
}
