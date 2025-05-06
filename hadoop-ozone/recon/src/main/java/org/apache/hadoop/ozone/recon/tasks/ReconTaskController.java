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

import java.util.Map;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

/**
 * Controller used by Recon to manage Tasks that are waiting on Recon events.
 */
public interface ReconTaskController {

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
}
