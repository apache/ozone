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

import java.util.concurrent.CompletableFuture;
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
}
