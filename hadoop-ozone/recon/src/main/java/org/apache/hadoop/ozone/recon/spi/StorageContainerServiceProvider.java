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

package org.apache.hadoop.ozone.recon.spi;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;

/**
 * Interface to access SCM endpoints.
 */
public interface StorageContainerServiceProvider {

  /**
   * Returns the list of active Pipelines from SCM.
   *
   * @return list of Pipelines
   * @throws IOException in case of any exception
   */
  List<Pipeline> getPipelines() throws IOException;

  /**
   * Requests SCM for a pipeline with ID.
   * @return pipeline if present
   * @throws IOException in case of exception
   */
  Pipeline getPipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Requests SCM for a container given ID.
   * @param containerId containerId
   * @return ContainerInfo + Pipeline info
   * @throws IOException in case of any exception.
   */
  ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException;

  /**
   * Requests SCM for which containers in given ID list.
   * @param containerIDs containerId list
   * @return list of ContainerInfo + Pipeline info exists in SCM
   */
  List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs);

  /**
   * Returns list of nodes from SCM.
   */
  List<HddsProtos.Node> getNodes() throws IOException;

  /**
   * Requests SCM for container count.
   * @return Total number of containers in SCM.
   */
  long getContainerCount() throws IOException;

  /**
   * Requests SCM for DB Snapshot.
   * @return DBCheckpoint from SCM.
   */
  DBCheckpoint getSCMDBSnapshot();

  /**
   * Get the list of container IDs from SCM. This is an RPC call.
   *
   * @param startContainerID the start container id
   * @param count the number of containers to return
   * @param state the containers in given state to be returned
   * @return the list of container IDs from SCM in a given state
   * @throws IOException
   */
  List<ContainerID> getListOfContainerIDs(ContainerID startContainerID,
                                          int count,
                                          HddsProtos.LifeCycleState state)
      throws IOException;

  /**
   * Get the list of containers from SCM. This is a RPC call.
   *
   * @param startContainerID the start container id
   * @param count the number of containers to return
   * @param state the containers in given state to be returned
   * @return the list of containers from SCM in a given state
   * @throws IOException
   */
  List<ContainerInfo> getListOfContainers(long startContainerID,
                                          int count,
                                          HddsProtos.LifeCycleState state)
      throws IOException;

  /**
   * Requests SCM for container count for a given state.
   * @return Total number of containers in SCM.
   */
  long getContainerCount(HddsProtos.LifeCycleState state) throws IOException;
}
