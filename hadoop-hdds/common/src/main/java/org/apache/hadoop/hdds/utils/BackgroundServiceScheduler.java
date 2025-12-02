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

package org.apache.hadoop.hdds.utils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Utility class to manage a shared background service using a {@link ScheduledExecutorService}
 * which is provided with a single-threaded {@link ScheduledThreadPoolExecutor}.
 * This class manages the lifecycle and reference counting for the executor
 * to ensure proper resource cleanup.
 *
 * The executor is lazily initialized on the first invocation of the {@code get()} method.
 * It is shut down and released when no longer referenced, ensuring efficient use
 * of system resources. The shutdown process includes cleaning the reference to the executor.
 *
 * This class is thread-safe.
 */
final class BackgroundServiceScheduler {
  private static ReferenceCountedObject<ScheduledExecutorService> executor;

  private BackgroundServiceScheduler() {

  }

  public static synchronized UncheckedAutoCloseableSupplier<ScheduledExecutorService> get() {
    if (executor == null) {
      ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
      executor = ReferenceCountedObject.wrap(scheduler, () -> { }, (shutdown) -> {
        if (shutdown) {
          synchronized (BackgroundServiceScheduler.class) {
            scheduler.shutdown();
            executor = null;
          }
        }
      });
    }
    return executor.retainAndReleaseOnClose();
  }
}
