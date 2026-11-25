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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager.XceiverClientManagerConfigBuilder;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link XceiverClientManager} closes clients that stay idle longer than
 * {@code scm.container.client.idle.threshold}, even when the cache is not used again.
 *
 * <p>An idle client holds its connections, and for Ratis data streaming the shared Netty worker group, whose
 * non-daemon threads keep the JVM from exiting (HDDS-17278).
 */
class TestXceiverClientManagerIdleEviction {

  private static final long IDLE_THRESHOLD_MS = 200;
  /** Long enough for an idle client to be closed many times over. */
  private static final int WAIT_MS = 20 * (int) IDLE_THRESHOLD_MS;

  private static XceiverClientManager newManager() throws Exception {
    ScmClientConfig clientConf = new XceiverClientManagerConfigBuilder()
        .setMaxCacheSize(256)
        .setStaleThresholdMs(IDLE_THRESHOLD_MS)
        .build();
    return new XceiverClientManager(new OzoneConfiguration(), clientConf, (ClientTrustManager) null);
  }

  @Test
  void idleClientIsClosedWithoutFurtherCacheAccess() throws Exception {
    try (XceiverClientManager manager = newManager()) {
      XceiverClientSpi client = manager.acquireClient(MockPipeline.createPipeline(1));
      manager.releaseClient(client, false);

      // nothing uses the cache any more, like a JVM whose application has finished writing
      GenericTestUtils.waitFor(client::isClosed, 50, WAIT_MS);
    }
  }

  @Test
  void idleClientIsClosedWhenTheCacheIsCleanedUp() throws Exception {
    try (XceiverClientManager manager = newManager()) {
      XceiverClientSpi client = manager.acquireClient(MockPipeline.createPipeline(1));
      manager.releaseClient(client, false);
      Thread.sleep(2 * IDLE_THRESHOLD_MS);

      manager.getClientCache().cleanUp();
      // evicted clients are closed in the background (HDDS-16812)
      GenericTestUtils.waitFor(client::isClosed, 10, WAIT_MS);
    }
  }

  @Test
  void idleClientInUseIsNotClosed() throws Exception {
    try (XceiverClientManager manager = newManager()) {
      XceiverClientSpi client = manager.acquireClient(MockPipeline.createPipeline(1));
      Thread.sleep(2 * IDLE_THRESHOLD_MS);
      manager.getClientCache().cleanUp();

      // expired, but still referenced, so it must stay open until it is released
      assertFalse(client.isClosed());
      manager.releaseClient(client, false);
      GenericTestUtils.waitFor(client::isClosed, 10, WAIT_MS);
    }
  }

  /**
   * Using the cache does not reliably evict idle clients either: the cache is split into segments, and a write cleans
   * up only the segment of its own key.
   */
  @Test
  void idleClientIsClosedWhenOtherClientsAreUsed() throws Exception {
    try (XceiverClientManager manager = newManager()) {
      List<XceiverClientSpi> idle = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        XceiverClientSpi client = manager.acquireClient(MockPipeline.createPipeline(1));
        manager.releaseClient(client, false);
        idle.add(client);
      }
      Thread.sleep(2 * IDLE_THRESHOLD_MS);

      XceiverClientSpi other = manager.acquireClient(MockPipeline.createPipeline(1));
      manager.releaseClient(other, false);
      GenericTestUtils.waitFor(() -> idle.stream().allMatch(XceiverClientSpi::isClosed), 50, WAIT_MS);
    }
  }

  /** The cleanup thread must not keep a JVM alive itself, and must stop with the manager. */
  @Test
  void cleanupThreadIsDaemonAndStopsOnClose() throws Exception {
    XceiverClientManager manager = newManager();
    List<Thread> threads = cleanupThreads();
    assertFalse(threads.isEmpty());
    assertTrue(threads.stream().allMatch(Thread::isDaemon));

    manager.close();
    GenericTestUtils.waitFor(() -> cleanupThreads().isEmpty(), 50, WAIT_MS);
  }

  private static List<Thread> cleanupThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.isAlive() && t.getName().startsWith("XceiverClientManager-cleanup-"))
        .collect(Collectors.toList());
  }
}
