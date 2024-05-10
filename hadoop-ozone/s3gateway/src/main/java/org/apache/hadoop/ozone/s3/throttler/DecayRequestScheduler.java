/*
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
package org.apache.hadoop.ozone.s3.throttler;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.http.HttpServer2.HTTP_MAX_THREADS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_FACTOR_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_FACTOR_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_PERIOD_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_PERIOD_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_REJECTION_RATIO_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_REJECTION_RATIO_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_THREADS_REJECTION_RATIO_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_THREADS_REJECTION_RATIO_DEFAULT;

/**
 * An implementation of request scheduler that decays the count of requests
 * of each user over the time.
 */
public class DecayRequestScheduler implements RequestScheduler {
  public static final int HTTP_MAX_THREADS_KEY_DEFAULT = 200;

  private static final Logger LOG =
      LoggerFactory.getLogger(DecayRequestScheduler.class);

  private final ConcurrentHashMap<Object, AtomicLong> currentRequests =
      new ConcurrentHashMap<>();

  // Total running requests count withing a decay window.
  private final AtomicLong totalRequestsCount = new AtomicLong();

  // max requests of last decay window (ratio * total requests)
  private final AtomicLong maxRequests = new AtomicLong();
  private final AtomicLong handlersCount = new AtomicLong();
  private final IdentityProvider identityProvider;
  private final double decayFactor;
  private final long decayPeriodMillis;
  private final double rejectionRatio;
  private final long maxHandlersCount;

  public DecayRequestScheduler(IdentityProvider identityProvider,
                               OzoneConfiguration config) {
    this.identityProvider = identityProvider;
    this.decayFactor = parseDecayFactor(config);
    this.decayPeriodMillis = parseDecayPeriodMillis(config);
    this.rejectionRatio = parseRejectionRatio(config);
    this.maxHandlersCount = parseMaxHandlersCount(config);

    Timer timer = new Timer(true);
    DecayTask task = new DecayTask(this, timer);
    timer.scheduleAtFixedRate(task, decayPeriodMillis, decayPeriodMillis);
  }

  private static double parseRejectionRatio(OzoneConfiguration conf) {
    double ratio = conf.getDouble(OZONE_S3G_DECAYSCHEDULER_REJECTION_RATIO_KEY,
        OZONE_S3G_DECAYSCHEDULER_REJECTION_RATIO_DEFAULT);
    if (ratio < 0 || ratio > 1) {
      throw new IllegalArgumentException("Rejection ratio " +
          "must be between 0 and 1");
    }

    return ratio;
  }

  private static double parseDecayFactor(OzoneConfiguration conf) {
    double factor = conf.getDouble(OZONE_S3G_DECAYSCHEDULER_FACTOR_KEY,
        OZONE_S3G_DECAYSCHEDULER_FACTOR_DEFAULT);
    if (factor < 0 || factor > 1) {
      throw new IllegalArgumentException("Decay Factor " +
          "must be between 0 and 1");
    }

    return factor;
  }

  private static long parseMaxHandlersCount(OzoneConfiguration conf) {
    double ratio = conf.getDouble(
        OZONE_S3G_DECAYSCHEDULER_THREADS_REJECTION_RATIO_KEY,
        OZONE_S3G_DECAYSCHEDULER_THREADS_REJECTION_RATIO_DEFAULT);
    if (ratio < 0 || ratio >= 1) {
      throw new IllegalArgumentException("threads rejection ratio must be " +
          "greater than or equal 0 and less than 1");
    }

    long handlersCount = conf.getLong(HTTP_MAX_THREADS_KEY,
        HTTP_MAX_THREADS_KEY_DEFAULT);
    return (long) (ratio * handlersCount);
  }

  private static long parseDecayPeriodMillis(OzoneConfiguration conf) {
    long period = conf.getLong(OZONE_S3G_DECAYSCHEDULER_PERIOD_KEY,
        OZONE_S3G_DECAYSCHEDULER_PERIOD_DEFAULT);
    if (period <= 0) {
      throw new IllegalArgumentException("Period millis must be > 0");
    }

    return period;
  }

  private long computeRequestsCount(Object identity) {
    long requestsCount = currentRequests.getOrDefault(identity,
        new AtomicLong(0)).get();
    LOG.debug("compute requests count for identity: {}={}", identity,
        requestsCount);
    return requestsCount;
  }

  private void decayCurrentRequests() {
    LOG.debug("Start to decay current requests.");

    try {
      maxRequests.set((long) (rejectionRatio * totalRequestsCount.longValue()));
      totalRequestsCount.set(0L);

      LOG.debug("maxRequests after a decay: " + maxRequests.longValue());

      Iterator<Map.Entry<Object, AtomicLong>> it =
          currentRequests.entrySet().iterator();

      while (it.hasNext()) {
        Map.Entry<Object, AtomicLong> entry = it.next();
        AtomicLong requests = entry.getValue();

        // Compute the next value by reducing it by the decayFactor
        long currentValue = requests.get();
        long nextValue = (long) (currentValue * decayFactor);
        requests.set(nextValue);

        LOG.debug("Decaying requests for the user: {}, " +
                "its decayedRequests: {}, earlierValue: {}",
            entry.getKey(), nextValue, currentValue);
        if (nextValue == 0) {
          LOG.debug("The decayed cost for the user {} is zero " +
              "and being cleaned.", entry.getKey());
          // We will clean up unused keys here. An interesting optimization
          // might be to have an upper bound on keyspace in callCosts and only
          // clean once we pass it.
          it.remove();
        }
      }
    } catch (Exception ex) {
      LOG.error("decayCurrentCosts exception: " + ex.getMessage());
      throw ex;
    }
  }

  @Override
  public boolean shouldReject(Request request) {
    String user = null;
    try {
      user = this.identityProvider.makeIdentity(request);
    } catch (OS3Exception e) {
      LOG.warn("Cannot makeIdentity of request: " + request);
      return true;
    }

    long rejectionThreshold = this.maxRequests.longValue();
    if (rejectionThreshold == 0) {
      LOG.debug("No requests made within last decay window, " + user +
          "'s request is accepted");
      return false;
    }

    long busyHandlersCount = this.handlersCount.longValue();
    long userRequestsCount = this.computeRequestsCount(user);
    boolean isRejected = busyHandlersCount > this.maxHandlersCount &&
        userRequestsCount > rejectionThreshold;

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          user + "'s request is " + (isRejected ? "rejected" : "accepted") +
              ", total busy handlers=" + busyHandlersCount +
              ", user's accumulated requests=" + userRequestsCount +
              ", rejection threshold=" + rejectionThreshold);
    }

    return isRejected;
  }

  @Override
  public void addRequest(Request request) {
    String user = null;
    try {
      user = identityProvider.makeIdentity(request);
    } catch (OS3Exception e) {
      LOG.warn("Cannot makeIdentity of request: " + request);
      return;
    }
    AtomicLong requests = this.currentRequests.get(user);
    if (requests == null) {
      requests = new AtomicLong(0);

      // Put it in, or get the AtomicInteger that was put in by another thread
      AtomicLong otherRequests =
          this.currentRequests.putIfAbsent(user, requests);
      if (otherRequests != null) {
        requests = otherRequests;
      }
    }
    requests.getAndIncrement();
    this.totalRequestsCount.incrementAndGet();
    this.handlersCount.incrementAndGet();
  }

  @Override
  public void removeRequest(Request request) {
    this.handlersCount.decrementAndGet();
  }

  /**
   * A task to decay requests count periodically.
   */
  public static class DecayTask extends TimerTask {
    private final Timer timer;
    private final WeakReference<DecayRequestScheduler> schedulerRef;

    public DecayTask(DecayRequestScheduler scheduler, Timer timer) {
      this.schedulerRef = new WeakReference<>(scheduler);
      this.timer = timer;
    }

    @Override
    public void run() {
      DecayRequestScheduler scheduler = schedulerRef.get();
      if (scheduler != null) {
        scheduler.decayCurrentRequests();
      } else {
        // Our scheduler was garbage collected since it is no longer in use,
        // so we should terminate the timer as well
        timer.cancel();
        timer.purge();
      }
    }
  }
}
