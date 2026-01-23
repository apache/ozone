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

package org.apache.hadoop.ozone.failure;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.MiniOzoneChaosCluster;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages all the failures in the MiniOzoneChaosCluster.
 */
public class FailureManager {

  static final Logger LOG =
      LoggerFactory.getLogger(Failures.class);

  private final MiniOzoneChaosCluster cluster;
  private final List<Failures> failures;
  private ScheduledFuture scheduledFuture;
  private final ScheduledExecutorService executorService;

  public FailureManager(MiniOzoneChaosCluster cluster,
                        Configuration conf,
                        Set<Class<? extends Failures>> clazzes) {
    this.cluster = cluster;
    this.executorService = Executors.newSingleThreadScheduledExecutor();

    failures = new ArrayList<>();
    for (Class<? extends Failures> clazz : clazzes) {
      Failures f = ReflectionUtils.newInstance(clazz, conf);
      f.validateFailure(cluster);
      failures.add(f);
    }

  }

  // Fail nodes randomly at configured timeout period.
  private void fail() {
    Failures f = failures.get(getBoundedRandomIndex(failures.size()));
    try {
      LOG.info("time failure with {}", f.getName());
      f.fail(cluster);
    } catch (Throwable t) {
      LOG.info("Caught exception while inducing failure:{}", f.getName(), t);
      throw new RuntimeException();
    }

  }

  public void start(long initialDelay, long period, TimeUnit timeUnit) {
    LOG.info("starting failure manager {} {} {}", initialDelay,
        period, timeUnit);
    scheduledFuture = executorService.scheduleAtFixedRate(this::fail,
        initialDelay, period, timeUnit);
  }

  public void stop() throws Exception {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
      scheduledFuture.get();
    }

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  public static boolean isFastRestart() {
    return RandomUtils.secure().randomBoolean();
  }

  public static int getBoundedRandomIndex(int size) {
    return RandomUtils.secure().randomInt(0, size);
  }
}
