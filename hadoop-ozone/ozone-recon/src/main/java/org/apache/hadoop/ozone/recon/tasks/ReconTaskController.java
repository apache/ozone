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

import java.util.Map;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;

/**
 * Controller used by Recon to manage Tasks that are waiting on Recon events.
 */
public interface ReconTaskController {

  /**
   * Register API used by tasks to register themselves.
   * @param task task instance
   */
  void registerTask(ReconDBUpdateTask task);

  /**
   * Pass on a set of OM DB update events to the registered tasks.
   * @param events set of events
   * @throws InterruptedException InterruptedException
   */
  void consumeOMEvents(OMUpdateEventBatch events,
                       OMMetadataManager omMetadataManager)
      throws InterruptedException;

  /**
   * Pass on the handle to a new OM DB instance to the registered tasks.
   * @param omMetadataManager OM Metadata Manager instance
   */
  void reInitializeTasks(OMMetadataManager omMetadataManager)
      throws InterruptedException;

  /**
   * Get set of registered tasks.
   * @return Map of Task name -> Task.
   */
  Map<String, ReconDBUpdateTask> getRegisteredTasks();

  /**
   * Get instance of ReconTaskStatusDao.
   * @return instance of ReconTaskStatusDao
   */
  ReconTaskStatusDao getReconTaskStatusDao();

  /**
   * Stop the tasks. Start API is not needed since it is implicit.
   */
  void stop();
}
