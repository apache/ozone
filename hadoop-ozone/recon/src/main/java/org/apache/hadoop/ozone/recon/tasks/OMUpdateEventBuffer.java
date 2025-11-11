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
import org.apache.hadoop.ozone.recon.metrics.ReconTaskControllerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffer for Recon events during task reprocessing.
 * When tasks are being reprocessed on staging DB, this buffer holds
 * incoming events (OM delta updates and control events) to prevent blocking the OM sync process.
 */
public class OMUpdateEventBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(OMUpdateEventBuffer.class);

  private final BlockingQueue<ReconEvent> eventQueue;
  private final int maxCapacity;
  private final AtomicLong totalBufferedEvents = new AtomicLong(0);
  private final AtomicLong droppedBatches = new AtomicLong(0);
  private final ReconTaskControllerMetrics metrics;

  public OMUpdateEventBuffer(int maxCapacity, ReconTaskControllerMetrics metrics) {
    this.maxCapacity = maxCapacity;
    this.eventQueue = new LinkedBlockingQueue<>(maxCapacity);
    this.metrics = metrics;
  }

  /**
   * Add an event to the buffer.
   *
   * @param event The event to buffer
   * @return true if successfully buffered, false if queue full
   */
  public boolean offer(ReconEvent event) {
    boolean added = eventQueue.offer(event);
    if (added) {
      totalBufferedEvents.addAndGet(event.getEventCount());

      // Update metrics: track events buffered (entering queue)
      if (metrics != null) {
        metrics.incrEventBufferedCount(event.getEventCount());
        metrics.setEventCurrentQueueSize(eventQueue.size());
      }

      LOG.debug("Buffered event {} with {} events. Queue size: {}, Total buffered events: {}",
          event.getEventType(), event.getEventCount(), eventQueue.size(), totalBufferedEvents.get());
    } else {
      droppedBatches.incrementAndGet();

      // Update metrics: track dropped events
      if (metrics != null) {
        metrics.incrEventDropCount(event.getEventCount());
      }

      LOG.warn("Event buffer queue is full (capacity: {}). Dropping event {} with {} events. " +
              "Total dropped batches: {}",
          maxCapacity, event.getEventType(), event.getEventCount(), droppedBatches.get());
    }
    return added;
  }
  
  /**
   * Poll an event from the buffer with timeout.
   *
   * @param timeoutMs timeout in milliseconds
   * @return event or null if timeout
   */
  public ReconEvent poll(long timeoutMs) {
    try {
      ReconEvent event = eventQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
      if (event != null) {
        totalBufferedEvents.addAndGet(-event.getEventCount());

        // Update metrics: track events processed (exiting queue)
        if (metrics != null) {
          metrics.incrTotalEventCount(event.getEventCount());
          metrics.setEventCurrentQueueSize(eventQueue.size());
        }

        LOG.debug("Polled event {} with {} events. Queue size: {}, Total buffered events: {}",
            event.getEventType(), event.getEventCount(), eventQueue.size(), totalBufferedEvents.get());
      }
      return event;
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
   * Drain all buffered events to the provided collection.
   * 
   * @param drainedEvents Collection to drain events into
   * @return number of events drained
   */
  @VisibleForTesting
  public int drainTo(java.util.Collection<? super ReconEvent> drainedEvents) {
    int drained = eventQueue.drainTo(drainedEvents);
    if (drained > 0) {
      // Update total buffered events count
      long totalEventCount = drainedEvents.stream()
          .mapToLong(event -> ((ReconEvent) event).getEventCount())
          .sum();
      totalBufferedEvents.addAndGet(-totalEventCount);
      LOG.debug("Drained {} events from buffer. Remaining queue size: {}, Total buffered events: {}", 
          drained, eventQueue.size(), totalBufferedEvents.get());
    }
    return drained;
  }

  /**
   * Reset the dropped batches counter. Used after full snapshot is triggered.
   */
  public void resetDroppedBatches() {
    droppedBatches.set(0);
  }
}
