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

package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * A subscriber can register with ContainerReplicaPendingOps to receive
 * updates on pending ops.
 */
public interface ContainerReplicaPendingOpsSubscriber {

  /**
   * Notifies that the specified op has been completed for the specified
   * containerID. Might have completed normally or timed out.
   *
   * @param op Add or Delete op
   * @param containerID container on which the operation is being performed
   * @param timedOut true if the timed out, else false
   */
  void opCompleted(ContainerReplicaOp op, ContainerID containerID,
      boolean timedOut);
}
