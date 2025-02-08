/*
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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;

import java.util.Map;

/**
 * Interface used to denote a Recon task that needs to act on OM DB events.
 */
public interface ReconOmTask {

  /**
   * Return task name.
   * @return task name
   */
  String getTaskName();

  /**
   * Processes a set of OM events on tables that the task is listening to.
   *
   * @param events            Set of events to be processed by the task.
   * @param subTaskSeekPosMap Position from where to start iterating events of the event iterator for a given sub-task.
   * @return A pair containing the task name mapped to a map of
   *         {@code sub-task name, its events iterator position} and a task success flag.
   */
  Pair<String, Pair<Map<String, Integer>, Boolean>> process(OMUpdateEventBatch events,
                                                            Map<String, Integer> subTaskSeekPosMap);

  /**
   * Process a  on tables that the task is listening on.
   *
   * @param omMetadataManager OM Metadata manager instance.
   * @return Pair of task name -&gt; map of <subtask, seek position>, task success.
   */
  Pair<String, Pair<Map<String, Integer>, Boolean>> reprocess(OMMetadataManager omMetadataManager);

}
