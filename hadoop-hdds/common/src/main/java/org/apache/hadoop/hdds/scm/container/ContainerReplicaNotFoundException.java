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

package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Signals that a ContainerReplica is missing from the Container in
 * ContainerManager.
 */
public class ContainerReplicaNotFoundException extends ContainerException {

  /**
   * Constructs an {@code ContainerReplicaNotFoundException} with {@code null}
   * as its error detail message.
   */
  public ContainerReplicaNotFoundException() {
    this(null, null);
  }

  /** Required by {@link org.apache.hadoop.ipc_.RemoteException#unwrapRemoteException()}. */
  public ContainerReplicaNotFoundException(String message) {
    super(message, ResultCodes.CONTAINER_REPLICA_NOT_FOUND);
  }

  public ContainerReplicaNotFoundException(ContainerID container, DatanodeDetails datanode) {
    super("Replica not found for container " + container + " and datanode " + datanode,
        ResultCodes.CONTAINER_REPLICA_NOT_FOUND);
  }
}
