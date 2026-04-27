/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.invocation;

import java.util.Collections;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.Time;

class RetryInfo {
  private final long retryTime;
  private final long delay;
  private final RetryPolicy.RetryAction action;
  private final long expectedFailoverCount;
  private final Exception failException;

  RetryInfo(long delay, RetryPolicy.RetryAction action, long expectedFailoverCount,
            Exception failException) {
    this.delay = delay;
    this.retryTime = Time.monotonicNow() + delay;
    this.action = action;
    this.expectedFailoverCount = expectedFailoverCount;
    this.failException = failException;
  }

  boolean isFailover() {
    return action != null
        && action.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;
  }

  boolean isFail() {
    return action != null
        && action.action == RetryPolicy.RetryAction.RetryDecision.FAIL;
  }

  Exception getFailException() {
    return failException;
  }

  static RetryInfo newRetryInfo(RetryPolicy policy, Exception e,
                                Counters counters, boolean idempotentOrAtMostOnce,
                                long expectedFailoverCount) throws Exception {
    RetryPolicy.RetryAction max = null;
    long maxRetryDelay = 0;
    Exception ex = null;

    final Iterable<Exception> exceptions = e instanceof MultiException ?
        ((MultiException) e).getExceptions().values()
        : Collections.singletonList(e);
    for (Exception exception : exceptions) {
      final RetryPolicy.RetryAction a = policy.shouldRetry(exception,
          counters.getRetries(), counters.getFailovers(), idempotentOrAtMostOnce);
      if (a.action != RetryPolicy.RetryAction.RetryDecision.FAIL) {
        // must be a retry or failover
        if (a.delayMillis > maxRetryDelay) {
          maxRetryDelay = a.delayMillis;
        }
      }

      if (max == null || max.action.compareTo(a.action) < 0) {
        max = a;
        if (a.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
          ex = exception;
        }
      }
    }

    return new RetryInfo(maxRetryDelay, max, expectedFailoverCount, ex);
  }

  public long getRetryTime() {
    return retryTime;
  }

  public RetryPolicy.RetryAction getAction() {
    return action;
  }

  public long getDelay() {
    return delay;
  }

  public long getExpectedFailoverCount() {
    return expectedFailoverCount;
  }

  @Override
  public String toString() {
    return "RetryInfo{" +
        "retryTime=" + retryTime +
        ", delay=" + delay +
        ", action=" + action +
        ", expectedFailoverCount=" + expectedFailoverCount +
        ", failException=" + failException +
        '}';
  }
}
