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

package org.apache.hadoop.ozone.container.diskbalancer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

/**
 * A test class implementation for {@link DiskBalancerService}.
 */
public class DiskBalancerServiceTestImpl extends DiskBalancerService {

  // the service timeout
  private static final int SERVICE_TIMEOUT_IN_MILLISECONDS = 0;

  // tests only
  private CountDownLatch latch;
  private Thread testingThread;
  private AtomicInteger numOfProcessed = new AtomicInteger(0);

  public DiskBalancerServiceTestImpl(OzoneContainer container,
      int serviceInterval, ConfigurationSource conf, int threadCount)
      throws IOException {
    super(container, serviceInterval, SERVICE_TIMEOUT_IN_MILLISECONDS,
        TimeUnit.MILLISECONDS, threadCount, conf);
  }

  public void runBalanceTasks() {
    if (latch.getCount() > 0) {
      this.latch.countDown();
    } else {
      throw new IllegalStateException("Count already reaches zero");
    }
  }

  public boolean isStarted() {
    return latch != null && testingThread.isAlive();
  }

  public int getTimesOfProcessed() {
    return numOfProcessed.get();
  }

  // Override the implementation to start a single on-call control thread.
  @Override
  public void start() {
    PeriodicalTask svc = new PeriodicalTask(null);
    // In test mode, relies on a latch countdown to runDeletingTasks tasks.
    Runnable r = () -> {
      while (true) {
        latch = new CountDownLatch(1);
        try {
          latch.await();
        } catch (InterruptedException e) {
          break;
        }
        Future<?> future = this.getExecutorService().submit(svc);
        try {
          // for tests, we only wait for 3s for completion
          future.get(3000, TimeUnit.SECONDS);
          numOfProcessed.incrementAndGet();
        } catch (Exception e) {
          e.printStackTrace();
          return;
        }
      }
    };

    testingThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .build()
        .newThread(r);
    testingThread.start();
  }

  @Override
  public void shutdown() {
    if (testingThread != null) {
      testingThread.interrupt();
    }
    super.shutdown();
  }
}
