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

package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that streamlines request semaphore usage in KeyOutputStream.
 */
public class KeyOutputStreamSemaphore {

  private static final Logger LOG = LoggerFactory.getLogger(KeyOutputStreamSemaphore.class);
  private final Semaphore requestSemaphore;

  KeyOutputStreamSemaphore(int maxConcurrentWritePerKey) {
    LOG.debug("Initializing semaphore with maxConcurrentWritePerKey = {}", maxConcurrentWritePerKey);
    if (maxConcurrentWritePerKey > 0) {
      requestSemaphore = new Semaphore(maxConcurrentWritePerKey);
    } else if (maxConcurrentWritePerKey == 0) {
      throw new IllegalArgumentException("Invalid config. ozone.client.key.write.concurrency cannot be set to 0");
    } else {
      requestSemaphore = null;
    }
  }

  public int getQueueLength() {
    return requestSemaphore != null ? requestSemaphore.getQueueLength() : 0;
  }

  public void acquire() throws IOException {
    if (requestSemaphore != null) {
      try {
        LOG.debug("Acquiring semaphore");
        requestSemaphore.acquire();
        LOG.debug("Acquired semaphore");
      } catch (InterruptedException e) {
        final String errMsg = "Write aborted. Interrupted waiting for KeyOutputStream semaphore: " + e.getMessage();
        LOG.error(errMsg);
        Thread.currentThread().interrupt();
        throw new InterruptedIOException(errMsg);
      }
    }
  }

  public void release() {
    if (requestSemaphore != null) {
      LOG.debug("Releasing semaphore");
      requestSemaphore.release();
      LOG.debug("Released semaphore");
    }
  }
}
