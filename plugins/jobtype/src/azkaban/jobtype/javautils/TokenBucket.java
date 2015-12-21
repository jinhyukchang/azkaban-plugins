package azkaban.jobtype.javautils;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * Implementation of token bucket for self throttling purpose.
 * https://en.wikipedia.org/wiki/Token_bucket
 *
 */
public class TokenBucket {
  private static Logger log = Logger.getLogger(TokenBucket.class);
  private final int capacity;
  private int tokens;
  private final Lock lock;
  private final Condition condition;
  private final Timer refiller;

  public TokenBucket(final int capacity) {
    this.capacity = capacity;
    this.tokens = capacity;
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();

    refiller = new Timer(true);
    refiller.schedule(new TimerTask() {
      @Override public void run() { //Refill token to its capacity every sec.
        lock.lock();
        try {
          tokens = capacity;
          condition.signalAll();
        } finally {
          lock.unlock();
        }
      }
    }, TimeUnit.SECONDS.toMillis(1L),
       TimeUnit.SECONDS.toMillis(1L));
    log.info("Token bucket has initialized. " + this);
  }

  /**
   * Acquire token. If there's no more token, it blocks the thread and waits to be refilled. The bucket will be refilled every second.
   */
  public void acquire(int noTokens) {
    if (noTokens > capacity) {
      throw new IllegalArgumentException("Number of requested tokens are greater than total capacity. It will never require the token.");
    }
    lock.lock();
    try {
      while (tokens <= 0 || tokens < noTokens) {
        try {
          condition.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread got interrupted.", e);
        }
      }
      tokens -= noTokens;
    } finally {
      lock.unlock();
    }
  }

  public void acquire() {
    acquire(1);
  }

  public static void main(String[] args) throws InterruptedException {
    TokenBucket tb = new TokenBucket(500);
    while(true) {
      int i = 250;
      tb.acquire(i);
      log.info("Acquired: " + i + " , " + tb);
    }
  }

  @Override
  public String toString() {
    return "TokenBucket [capacity=" + capacity + ", tokens=" + tokens + "]";
  }
}
