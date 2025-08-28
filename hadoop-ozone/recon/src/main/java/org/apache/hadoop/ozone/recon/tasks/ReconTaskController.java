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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

/**
 * Controller used by Recon to manage Tasks that are waiting on Recon events.
 */
public interface ReconTaskController {

  /**
   * Enum representing the result of queueing a reinitialization event.
   */
  enum ReInitializationResult {
    SUCCESS,           // Event was successfully queued
    RETRY_LATER,       // Failed but should retry in next iteration after delay
    MAX_RETRIES_EXCEEDED  // Maximum retries exceeded, caller should fallback to full snapshot
  }

  /**
   * Register API used by tasks to register themselves.
   * @param task task instance
   */
  void registerTask(ReconOmTask task);

  /**
   * Pass on a set of OM DB update events to the registered tasks.
   * @param events set of events
   */
  void consumeOMEvents(OMUpdateEventBatch events,
                       OMMetadataManager omMetadataManager);

  /**
   * Reinitializes the registered Recon OM tasks with a new OM Metadata Manager instance.
   *
   * @param omMetadataManager the OM Metadata Manager instance to be used for reinitialization.
   * @param reconOmTaskMap a map of Recon OM tasks, which we would like to reinitialize.
   *                       If {@code reconOmTaskMap} is null, all registered Recon OM tasks
   *                       will be reinitialized.
   */
  void reInitializeTasks(ReconOMMetadataManager omMetadataManager, Map<String, ReconOmTask> reconOmTaskMap);

  /**
   * Get set of registered tasks.
   * @return Map of Task name -&gt; Task.
   */
  Map<String, ReconOmTask> getRegisteredTasks();

  /**
   * Start the task scheduler.
   */
  void start();

  /**
   * Stop the task scheduler.
   */
  void stop();

  /**
   * Check if event buffer has overflowed and needs full snapshot fallback.
   * 
   * @return true if buffer has dropped events due to overflow
   */
  boolean hasEventBufferOverflowed();

  /**
   * Reset the event buffer overflow flag after full snapshot is completed.
   */
  void resetEventBufferOverflowFlag();

  /**
   * Check if delta tasks have failed and need reinitialization.
   * 
   * @return true if delta tasks failed after retry
   */
  boolean hasDeltaTasksFailed();

  /**
   * Reset the delta tasks failure flag after reinitialization is completed.
   */
  void resetDeltaTasksFailureFlag();

  /**
   * Clear all buffered events. Used before queuing a reinitialization event.
   */
  void resetEventBuffer();

  /**
   * Reset event flags used to track buffer overflow and delta task failures.
   * This is used before queuing a reinitialization event.
   *
   * @param reason the reason for reinitialization
   */
  void resetEventFlags(ReconTaskReInitializationEvent.ReInitializationReason reason);
  
  /**
   * Queue a task reinitialization event to be processed asynchronously.
   * This method creates a checkpoint of the current OM metadata manager,
   * clears the event buffer and queues a reinitialization event.
   * Includes internal retry logic with timing controls for checkpoint creation.
   * 
   * @param reason the reason for reinitialization
   * @return ReInitializationResult indicating success, retry needed, or max retries exceeded
   */
  ReInitializationResult queueReInitializationEvent(ReconTaskReInitializationEvent.ReInitializationReason reason);

  /**
   * Create a checkpoint of the current OM metadata manager.
   * This method creates a snapshot of the current OM database state 
   * to prevent data inconsistency during reinitialization.
   * 
   * @param omMetaManager the OM metadata manager to checkpoint
   * @return a checkpointed ReconOMMetadataManager instance
   * @throws IOException if checkpoint creation fails
   */
  ReconOMMetadataManager createOMCheckpoint(ReconOMMetadataManager omMetaManager) throws IOException;
  
  /**
   * Update the current OM metadata manager reference for reinitialization.
   * 
   * @param omMetadataManager the current OM metadata manager
   */
  void updateOMMetadataManager(ReconOMMetadataManager omMetadataManager);
  
  /**
   * Get the current size of the event buffer.
   * 
   * @return the number of events currently in the buffer
   */
  @VisibleForTesting
  int getEventBufferSize();
  
  /**
   * Get the number of batches that have been dropped due to buffer overflow.
   * This is used by the overflow detection logic.
   * 
   * @return the number of dropped batches
   */
  long getDroppedBatches();

  /**
   * Wait for all currently buffered events to be processed.
   * This returns a CompletableFuture that completes when the event buffer is empty.
   * Used in testing to ensure all events are processed before assertions.
   *
   * @return CompletableFuture that completes when event buffer is empty
   */
  CompletableFuture<Void> waitForEventBufferEmpty();
}
