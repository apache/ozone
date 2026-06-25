/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io_.retry;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.SaslException;

import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ipc_.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A collection of useful implementations of {@link RetryPolicy}.
 * </p>
 */
public class RetryPolicies {
  
  public static final Logger LOG = LoggerFactory.getLogger(RetryPolicies.class);
  
  /**
   * <p>
   * Try once, and fail by re-throwing the exception.
   * This corresponds to having no retry mechanism in place.
   * </p>
   */
  public static final RetryPolicy TRY_ONCE_THEN_FAIL = new TryOnceThenFail();
  
  /**
   * <p>
   * Keep trying forever.
   * </p>
   */
  public static final RetryPolicy RETRY_FOREVER = new RetryForever();

  /**
   * <p>
   * Keep trying forever with a fixed time between attempts.
   * </p>
   *
   * @param sleepTime sleepTime.
   * @param timeUnit timeUnit.
   * @return RetryPolicy.
   */
  public static final RetryPolicy retryForeverWithFixedSleep(long sleepTime,
      TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithFixedSleep(Integer.MAX_VALUE,
        sleepTime, timeUnit);
  }

  /**
   * <p>
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   *
   * @param maxRetries maxRetries.
   * @param sleepTime sleepTime.
   * @param timeUnit timeUnit.
   * @return RetryPolicy.
   */
  public static final RetryPolicy retryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime, timeUnit);
  }

  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by a random
   * number in the range of [0, 2 to the number of retries)
   * </p>
   *
   *
   * @param timeUnit timeUnit.
   * @param maxRetries maxRetries.
   * @param sleepTime sleepTime.
   * @return RetryPolicy.
   */
  public static final RetryPolicy exponentialBackoffRetry(
      int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
  }

  public static final RetryPolicy failoverOnNetworkException(int maxFailovers) {
    return failoverOnNetworkException(TRY_ONCE_THEN_FAIL, maxFailovers);
  }
  
  public static final RetryPolicy failoverOnNetworkException(
      RetryPolicy fallbackPolicy, int maxFailovers) {
    return failoverOnNetworkException(fallbackPolicy, maxFailovers, 0, 0);
  }
  
  public static final RetryPolicy failoverOnNetworkException(
      RetryPolicy fallbackPolicy, int maxFailovers, long delayMillis,
      long maxDelayBase) {
    return new FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers,
        delayMillis, maxDelayBase);
  }

  static class TryOnceThenFail implements RetryPolicy {
    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return new RetryAction(RetryAction.RetryDecision.FAIL, 0, "try once " +
          "and fail.");
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else {
        return obj != null && obj.getClass() == this.getClass();
      }
    }

    @Override
    public int hashCode() {
      return this.getClass().hashCode();
    }
  }

  static class RetryForever implements RetryPolicy {
    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.RETRY;
    }
  }
  
  /**
   * Retry up to maxRetries.
   * The actual sleep time of the n-th retry is f(n, sleepTime),
   * where f is a function provided by the subclass implementation.
   *
   * The object of the subclasses should be immutable;
   * otherwise, the subclass must override hashCode(), equals(..) and toString().
   */
  static abstract class RetryLimited implements RetryPolicy {
    final int maxRetries;
    final long sleepTime;
    final TimeUnit timeUnit;
    
    private String myString;

    RetryLimited(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      if (maxRetries < 0) {
        throw new IllegalArgumentException("maxRetries = " + maxRetries+" < 0");
      }
      if (sleepTime < 0) {
        throw new IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
      }

      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      if (retries >= maxRetries) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0 , getReason());
      }
      return new RetryAction(RetryAction.RetryDecision.RETRY,
          timeUnit.toMillis(calculateSleepTime(retries)), getReason());
    }

    protected String getReason() {
      return constructReasonString(maxRetries);
    }

    public static String constructReasonString(int retries) {
      return "retries get failed due to exceeded maximum allowed retries " +
          "number: " + retries;
    }

    protected abstract long calculateSleepTime(int retries);

    @Override
    public int hashCode() {
      return toString().hashCode();
    }
    
    @Override
    public boolean equals(final Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public String toString() {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(maxRetries=" + maxRetries
            + ", sleepTime=" + sleepTime + " " + timeUnit + ")";
      }
      return myString;
    }
  }
  
  static class RetryUpToMaximumCountWithFixedSleep extends RetryLimited {
    public RetryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return sleepTime;
    }
  }

  static class ExponentialBackoffRetry extends RetryLimited {
    
    public ExponentialBackoffRetry(
        int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);

      if (maxRetries < 0) {
        throw new IllegalArgumentException("maxRetries = " + maxRetries + " < 0");
      } else if (maxRetries >= Long.SIZE - 1) {
        //calculateSleepTime may overflow. 
        throw new IllegalArgumentException("maxRetries = " + maxRetries
            + " >= " + (Long.SIZE - 1));
      }
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return calculateExponentialTime(sleepTime, retries + 1);
    }

  }
  
  /**
   * Fail over and retry in the case of:
   *   Immediate socket exceptions (e.g. no route to host, econnrefused)
   *   Socket exceptions after initial connection when operation is idempotent
   * 
   * The first failover is immediate, while all subsequent failovers wait an
   * exponentially-increasing random amount of time.
   * 
   * Fail immediately in the case of:
   *   Socket exceptions after initial connection when operation is not idempotent
   * 
   * Fall back on underlying retry policy otherwise.
   */
  static class FailoverOnNetworkExceptionRetry implements RetryPolicy {
    
    private RetryPolicy fallbackPolicy;
    private int maxFailovers;
    private int maxRetries;
    private long delayMillis;
    private long maxDelayBase;
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers) {
      this(fallbackPolicy, maxFailovers, 0, 0, 0);
    }
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers, long delayMillis, long maxDelayBase) {
      this(fallbackPolicy, maxFailovers, 0, delayMillis, maxDelayBase);
    }
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers, int maxRetries, long delayMillis, long maxDelayBase) {
      this.fallbackPolicy = fallbackPolicy;
      this.maxFailovers = maxFailovers;
      this.maxRetries = maxRetries;
      this.delayMillis = delayMillis;
      this.maxDelayBase = maxDelayBase;
    }

    /**
     * @return 0 if this is our first failover/retry (i.e., retry immediately),
     *         sleep exponentially otherwise
     */
    private long getFailoverOrRetrySleepTime(int times) {
      return times == 0 ? 0 : 
        calculateExponentialTime(delayMillis, times, maxDelayBase);
    }
    
    @Override
    public RetryAction shouldRetry(Exception e, int retries,
        int failovers, boolean isIdempotentOrAtMostOnce) throws Exception {
      if (failovers >= maxFailovers) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "failovers (" + failovers + ") exceeded maximum allowed ("
            + maxFailovers + ")");
      }
      if (retries - failovers > maxRetries) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0, "retries ("
            + retries + ") exceeded maximum allowed (" + maxRetries + ")");
      }

      if (isSaslFailure(e)) {
          return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
                  "SASL failure");
      }

      if (e instanceof ConnectException ||
          e instanceof EOFException ||
          e instanceof NoRouteToHostException ||
          e instanceof UnknownHostException ||
          e instanceof ConnectTimeoutException) {
        return new RetryAction(RetryAction.RetryDecision.FAILOVER_AND_RETRY,
            getFailoverOrRetrySleepTime(failovers));
      } else if (e instanceof RetriableException
          || getWrappedRetriableException(e) != null) {
        // RetriableException or RetriableException wrapped 
        return new RetryAction(RetryAction.RetryDecision.RETRY,
              getFailoverOrRetrySleepTime(retries));
      } else if (e instanceof InvalidToken) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "Invalid or Cancelled Token");
      } else if (e instanceof AccessControlException ||
              hasWrappedAccessControlException(e)) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "Access denied");
      } else if (e instanceof SocketException
          || (e instanceof IOException && !(e instanceof RemoteException))) {
        if (isIdempotentOrAtMostOnce) {
          return new RetryAction(RetryAction.RetryDecision.FAILOVER_AND_RETRY,
              getFailoverOrRetrySleepTime(retries));
        } else {
          return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
              "the invoked method is not idempotent, and unable to determine "
                  + "whether it was invoked");
        }
      } else {
          return fallbackPolicy.shouldRetry(e, retries, failovers,
              isIdempotentOrAtMostOnce);
      }
    }
  }

  /**
   * Return a value which is <code>time</code> increasing exponentially as a
   * function of <code>retries</code>, +/- 0%-50% of that value, chosen
   * randomly.
   * 
   * @param time the base amount of time to work with
   * @param retries the number of retries that have so occurred so far
   * @param cap value at which to cap the base sleep time
   * @return an amount of time to sleep
   */
  private static long calculateExponentialTime(long time, int retries,
      long cap) {
    long baseTime = Math.min(time * (1L << retries), cap);
    return (long) (baseTime * (ThreadLocalRandom.current().nextDouble() + 0.5));
  }

  private static long calculateExponentialTime(long time, int retries) {
    return calculateExponentialTime(time, retries, Long.MAX_VALUE);
  }

  private static boolean isSaslFailure(Exception e) {
      Throwable current = e;
      do {
          if (current instanceof SaslException) {
            return true;
          }
          current = current.getCause();
      } while (current != null);

      return false;
  }
  
  static RetriableException getWrappedRetriableException(Exception e) {
    if (!(e instanceof RemoteException)) {
      return null;
    }
    Exception unwrapped = ((RemoteException)e).unwrapRemoteException(
        RetriableException.class);
    return unwrapped instanceof RetriableException ? 
        (RetriableException) unwrapped : null;
  }

  private static boolean hasWrappedAccessControlException(Exception e) {
    Throwable throwable = e;
    while (!(throwable instanceof AccessControlException) &&
        throwable.getCause() != null) {
      throwable = throwable.getCause();
    }
    return throwable instanceof AccessControlException;
  }
}
