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

package org.apache.hadoop.ozone.recon.tasks;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffer for OM update events during task reprocessing.
 * When tasks are being reprocessed on staging DB, this buffer holds
 * incoming delta updates to prevent blocking the OM sync process.
 */
public class OMUpdateEventBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(OMUpdateEventBuffer.class);
  
  private final BlockingQueue<OMUpdateEventBatch> eventQueue;
  private final int maxCapacity;
  private final AtomicLong totalBufferedEvents = new AtomicLong(0);
  private final AtomicLong droppedBatches = new AtomicLong(0);
  
  public OMUpdateEventBuffer(int maxCapacity) {
    this.maxCapacity = maxCapacity;
    this.eventQueue = new LinkedBlockingQueue<>(maxCapacity);
  }

  /**
   * Add an event batch to the buffer.
   * 
   * @param eventBatch The event batch to buffer
   * @return true if successfully buffered, false if queue full
   */
  public boolean offer(OMUpdateEventBatch eventBatch) {
    boolean added = eventQueue.offer(eventBatch);
    if (added) {
      totalBufferedEvents.addAndGet(eventBatch.getEvents().size());
      LOG.debug("Buffered event batch with {} events. Queue size: {}, Total buffered events: {}",
          eventBatch.getEvents().size(), eventQueue.size(), totalBufferedEvents.get());
    } else {
      droppedBatches.incrementAndGet();
      LOG.warn("Event buffer queue is full (capacity: {}). Dropping event batch with {} events. " +
              "Total dropped batches: {}",
          maxCapacity, eventBatch.getEvents().size(), droppedBatches.get());
    }
    return added;
  }
  
  /**
   * Poll an event batch from the buffer with timeout.
   * 
   * @param timeoutMs timeout in milliseconds
   * @return event batch or null if timeout
   */
  public OMUpdateEventBatch poll(long timeoutMs) {
    try {
      OMUpdateEventBatch batch = eventQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
      if (batch != null) {
        totalBufferedEvents.addAndGet(-batch.getEvents().size());
        LOG.debug("Polled event batch with {} events. Queue size: {}, Total buffered events: {}", 
            batch.getEvents().size(), eventQueue.size(), totalBufferedEvents.get());
      }
      return batch;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Get the current queue size.
   * 
   * @return number of batches currently in the queue
   */
  public int getQueueSize() {
    return eventQueue.size();
  }
  
  /**
   * Get the number of batches dropped due to queue overflow.
   * 
   * @return dropped batches count
   */
  public long getDroppedBatches() {
    return droppedBatches.get();
  }

  /**
   * Clear all buffered events.
   */
  @VisibleForTesting
  public void clear() {
    eventQueue.clear();
    totalBufferedEvents.set(0);
    // Note: We don't reset droppedBatches here to maintain overflow detection
  }

  /**
   * Reset the dropped batches counter. Used after full snapshot is triggered.
   */
  public void resetDroppedBatches() {
    droppedBatches.set(0);
  }
}
