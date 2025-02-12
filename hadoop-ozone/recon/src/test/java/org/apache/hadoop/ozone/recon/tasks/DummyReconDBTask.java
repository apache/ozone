/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import java.util.Collection;
import java.util.Collections;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.metrics.DummyDBTaskMetrics;

/**
 * Dummy Recon task that has 3 modes of operations.
 * ALWAYS_FAIL / FAIL_ONCE / ALWAYS_PASS
 */
public class DummyReconDBTask implements ReconOmTask {

  private int numFailuresAllowed = Integer.MIN_VALUE;
  private int callCtr = 0;
  private String taskName;
  private final DummyDBTaskMetrics metrics;

  DummyReconDBTask(String taskName, TaskType taskType) {
    this.taskName = taskName;
    if (taskType.equals(TaskType.FAIL_ONCE)) {
      numFailuresAllowed = 1;
    } else if (taskType.equals(TaskType.ALWAYS_FAIL)) {
      numFailuresAllowed = Integer.MAX_VALUE;
    }
    this.metrics = DummyDBTaskMetrics.register();
  }

  @VisibleForTesting
  public DummyDBTaskMetrics getMetricsInstance() {
    return this.metrics;
  }

  @Override
  public void stopMetricsCollection() {
    this.metrics.unregister();
  }

  @Override
  public String getTaskName() {
    return taskName;
  }

  public Collection<String> getTaskTables() {
    return Collections.singletonList("volumeTable");
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    metrics.incrTaskProcessCount();
    if (++callCtr <= numFailuresAllowed) {
      metrics.incrTaskProcessFailureCount();
      return new ImmutablePair<>(getTaskName(), false);
    } else {
      return new ImmutablePair<>(getTaskName(), true);
    }
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    metrics.incrTaskReprocessCount();
    if (++callCtr <= numFailuresAllowed) {
      metrics.incrTaskReprocessFailureCount();
      return new ImmutablePair<>(getTaskName(), false);
    } else {
      return new ImmutablePair<>(getTaskName(), true);
    }
  }

  /**
   * Type of the task.
   */
  public enum TaskType {
    ALWAYS_PASS,
    FAIL_ONCE,
    ALWAYS_FAIL
  }
}
