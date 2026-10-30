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

package org.apache.hadoop.ozone.jobworker.volume;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.ozone.lock.ReadWriteLockable;

/**
 * Jobworker VolumeSet Interface, VolumeSet manages all the volumes used by JobWorker.
 */
public interface JobworkerVolumeSet extends ReadWriteLockable, Closeable {

  /**
   * Initialize a volume set with the given cluster ID.
   *
   * @param clusterID Cluster identifier
   * @throws IOException if initialization fails
   */
  void initializeVolumeSet(String clusterID) throws IOException;

  /**
   * Get a list of all healthy volumes.
   *
   * @return List of volumes
   */
  List<VolatileJobworkerVolume> getVolumesList();

  /**
   * Choose a volume for a new task based on available space.
   *
   * @return The chosen JobWorkerVolume or null if none available
   */
  VolatileJobworkerVolume chooseVolume();

  /**
   * Create a new task directory on a volume.
   *
   * @param taskId The task ID
   * @return The task directory
   * @throws IOException if there's an error creating the directory
   */
  File createTaskDirectory(String taskId) throws IOException;

  /**
   * Clean up all directories associated with a task across all volumes.
   *
   * @param taskId The task ID to clean up
   * @return true if cleanup was successful on all volumes
   */
  boolean cleanupTask(String taskId);

  /**
   * Get the Storage report list of the VolumeSet.
   * @return Storage report list.
   */
  List<JobworkerStorageReportProto> getStorageReport();

}
