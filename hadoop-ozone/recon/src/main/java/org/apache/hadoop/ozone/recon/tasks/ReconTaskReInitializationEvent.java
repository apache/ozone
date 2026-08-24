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

import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

/**
 * Custom event to trigger task reinitialization asynchronously.
 * This event is generated when the event buffer overflows or when tasks fail
 * and need reinitialization.
 */
public class ReconTaskReInitializationEvent implements ReconEvent {

  private final ReInitializationReason reason;
  private final long timestamp;
  private final ReconOMMetadataManager checkpointedOMMetadataManager;

  /**
   * Enum representing the reasons for task reinitialization.
   */
  public enum ReInitializationReason {
    BUFFER_OVERFLOW,
    TASK_FAILURES,
    MANUAL_TRIGGER
  }

  public ReconTaskReInitializationEvent(ReInitializationReason reason,
                                        ReconOMMetadataManager checkpointedOMMetadataManager) {
    this.reason = reason;
    this.timestamp = System.currentTimeMillis();
    this.checkpointedOMMetadataManager = checkpointedOMMetadataManager;
  }

  public ReInitializationReason getReason() {
    return reason;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public ReconOMMetadataManager getCheckpointedOMMetadataManager() {
    return checkpointedOMMetadataManager;
  }

  @Override
  public EventType getEventType() {
    return EventType.TASK_REINITIALIZATION;
  }

  @Override
  public int getEventCount() {
    return 1;
  }

  @Override
  public String toString() {
    return "ReconTaskReInitializationEvent{" +
        "reason=" + reason +
        ", timestamp=" + timestamp +
        '}';
  }
}
