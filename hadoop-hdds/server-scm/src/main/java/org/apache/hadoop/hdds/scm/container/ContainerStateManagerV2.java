/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.metadata.Replicate;

import java.io.IOException;
import java.util.Set;

/**
 *
 * TODO: Add proper javadoc.
 *
 * Implementation of methods marked with {@code @Replicate} annotation should be
 *
 * 1. Idempotent
 * 2. Arguments should be of protobuf objects
 * 3. Return type should be of protobuf object
 * 4. The declaration should throw RaftException
 *
 */
public interface ContainerStateManagerV2 {

  /**
   *
   */
  ContainerID getNextContainerID();

  /**
   *
   */
  Set<ContainerID> getContainerIDs();

  /**
   *
   */
  Set<ContainerID> getContainerIDs(LifeCycleState state);

  /**
   *
   */
  ContainerInfo getContainer(ContainerID containerID)
      throws ContainerNotFoundException;

  /**
   *
   */
  Set<ContainerReplica> getContainerReplicas(ContainerID containerID)
      throws ContainerNotFoundException;

  /**
   *
   */
  @Replicate
  void addContainer(ContainerInfoProto containerInfo)
      throws IOException;

}
