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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Class to wrap details used to track pending replications.
 */
public class ContainerReplicaOp {

  /**
   * Enum representing different types of pending Ops.
   */
  public enum PendingOpType {
    ADD, DELETE
  }

  private final PendingOpType opType;
  private final DatanodeDetails target;
  private final int replicaIndex;
  private final SCMCommand<?> command;
  private final long deadlineEpochMillis;

  public static ContainerReplicaOp create(PendingOpType opType,
      DatanodeDetails target, int replicaIndex) {
    return new ContainerReplicaOp(opType, target, replicaIndex, null, System.currentTimeMillis());
  }

  public ContainerReplicaOp(PendingOpType opType,
      DatanodeDetails target, int replicaIndex, SCMCommand<?> command, long deadlineEpochMillis) {
    this.opType = opType;
    this.target = target;
    this.replicaIndex = replicaIndex;
    this.command = command;
    this.deadlineEpochMillis = deadlineEpochMillis;
  }

  public PendingOpType getOpType() {
    return opType;
  }

  public DatanodeDetails getTarget() {
    return target;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  public SCMCommand<?> getCommand() {
    return command;
  }

  public long getDeadlineEpochMillis() {
    return deadlineEpochMillis;
  }

}
