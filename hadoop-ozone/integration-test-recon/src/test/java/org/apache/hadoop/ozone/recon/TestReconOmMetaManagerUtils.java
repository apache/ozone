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

package org.apache.hadoop.ozone.recon;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.OMUpdateEventBuffer;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Test Recon Utility methods.
 */
public class TestReconOmMetaManagerUtils {

  /**
   * Wait for all currently buffered events to be processed asynchronously.
   * This method returns a CompletableFuture that completes when the event buffer becomes empty.
   * Useful for testing to ensure async processing is complete before assertions.
   *
   * @return CompletableFuture that completes when buffer is empty
   */
  public CompletableFuture<Void> waitForEventBufferEmpty(OMUpdateEventBuffer eventBuffer) {
    return CompletableFuture.runAsync(() -> {
      try {
        GenericTestUtils.waitFor(() -> eventBuffer.getQueueSize() == 0, 100, 30000);
        Thread.sleep(500);
      } catch (Exception e) {
        throw new RuntimeException("Error waiting for event buffer to empty", e);
      }
    });
  }

  /**
   * Waits until Recon's container-key index reports at least the given number of keys
   * per container id. Use after OM sync when the event buffer can be empty while a
   * dequeued batch is still being processed.
   * <p>
   * IO failures from {@code mgr} reads (including temporary {@code RocksDatabaseException}
   * while Recon applies updates) are treated as "not ready yet"; the wait repeats until the
   * timeout if counts never converge.
   *
   * @param mgr                      Recon container metadata manager
   * @param minimumCountPerContainer map of container ID to minimum inclusive key count
   * @throws Exception               if the condition is not met within the timeout or on interrupt
   */
  public static void waitUntilReconKeyCounts(ReconContainerMetadataManager mgr,
      Map<Long, Integer> minimumCountPerContainer) throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        for (Map.Entry<Long, Integer> e : minimumCountPerContainer.entrySet()) {
          if (mgr.getKeyCountForContainer(e.getKey()) < e.getValue()) {
            return false;
          }
        }
        return true;
      } catch (IOException ex) {
        // Retry: concurrent Recon indexing can transiently expose a closed Rocks handle.
        return false;
      }
    }, 1000, 90000);
  }
}
