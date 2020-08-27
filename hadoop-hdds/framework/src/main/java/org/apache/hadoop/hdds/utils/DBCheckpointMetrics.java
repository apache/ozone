/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metrics;

/**
 * This interface is for maintaining DB checkpoint statistics.
 */
@InterfaceAudience.Private
@Metrics(about="DB checkpoint Metrics", context="dfs")
public interface DBCheckpointMetrics {

  @VisibleForTesting
  void setLastCheckpointCreationTimeTaken(long val);

  @VisibleForTesting
  void setLastCheckpointStreamingTimeTaken(long val);

  @VisibleForTesting
  void incNumCheckpoints();

  @VisibleForTesting
  void incNumCheckpointFails();

  @VisibleForTesting
  long getLastCheckpointCreationTimeTaken();

  @VisibleForTesting
  long getLastCheckpointStreamingTimeTaken();

  @VisibleForTesting
  long getNumCheckpoints();

  @VisibleForTesting
  long getNumCheckpointFails();
}
