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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.OpenSsl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes Netty, and for TLS netty-tcnative (BoringSSL), on a background thread, so that a client's first
 * datanode connection finds them ready instead of initializing them itself (about 0.1 s in a cold JVM).
 */
final class NettyWarmUp {
  private static final Logger LOG = LoggerFactory.getLogger(NettyWarmUp.class);
  private static final AtomicBoolean STARTED = new AtomicBoolean();

  private NettyWarmUp() {
  }

  /**
   * Starts the warm-up, at most once per JVM.
   *
   * @param tls whether to also load and initialize netty-tcnative
   * @return the warm-up thread, or null if the warm-up was started before
   */
  static Thread start(boolean tls) {
    if (!STARTED.compareAndSet(false, true)) {
      return null;
    }
    final Thread thread = new Thread(() -> warmUp(tls), "ozone-client-netty-warm-up");
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  static void warmUp(boolean tls) {
    try {
      PooledByteBufAllocator.DEFAULT.directBuffer(1).release();
      if (tls) {
        OpenSsl.isAvailable();
      }
    } catch (Throwable t) {
      // the first connection initializes whatever is missing, as without the warm-up
      LOG.debug("Netty warm-up failed", t);
    }
  }

  @VisibleForTesting
  static boolean isStarted() {
    return STARTED.get();
  }

  @VisibleForTesting
  static void reset() {
    STARTED.set(false);
  }
}
