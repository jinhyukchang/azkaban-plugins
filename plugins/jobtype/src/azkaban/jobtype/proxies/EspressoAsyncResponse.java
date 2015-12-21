package azkaban.jobtype.proxies;

import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;

public class EspressoAsyncResponse<K, V> {
  private final ImmutableList<K> requests;
  private final Future<V> response;

  public EspressoAsyncResponse(ImmutableList<K> requests, Future<V> response) {
    this.requests = requests;
    this.response = response;
  }

  public ImmutableList<K> getRequests() {
    return requests;
  }

  public Future<V> getResponse() {
    return response;
  }

  @Override
  public String toString() {
    return "EspressoAsyncResponse [requests=" + requests + ", response=" + response + "]";
  }
}
