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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Interface which exposes the api for pipeline management.
 */
public interface PipelineManager extends Closeable, PipelineManagerMXBean {

  Pipeline createPipeline(ReplicationConfig replicationConfig)
      throws IOException;

  Pipeline createPipeline(ReplicationConfig replicationConfig,
                          List<DatanodeDetails> excludedNodes,
                          List<DatanodeDetails> favoredNodes)
      throws IOException;

  Pipeline buildECPipeline(ReplicationConfig replicationConfig,
                           List<DatanodeDetails> excludedNodes,
                           List<DatanodeDetails> favoredNodes)
      throws IOException;

  void addEcPipeline(Pipeline pipeline) throws IOException;

  Pipeline createPipeline(
      ReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes
  );

  Pipeline createPipelineForRead(
      ReplicationConfig replicationConfig, Set<ContainerReplica> replicas);

  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException;

  boolean containsPipeline(PipelineID pipelineID);

  List<Pipeline> getPipelines();

  List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig
  );

  List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig, Pipeline.PipelineState state
  );

  List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state,
      Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines
  );

  /**
   * Returns the count of pipelines meeting the given ReplicationConfig and
   * state.
   * @param replicationConfig The ReplicationConfig of the pipelines to count
   * @param state The current state of the pipelines to count
   * @return The count of pipelines meeting the above criteria
   */
  int getPipelineCount(
      ReplicationConfig replicationConfig, Pipeline.PipelineState state
  );

  void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws PipelineNotFoundException, InvalidPipelineStateException;

  /**
   * Add container to pipeline during SCM Start.
   *
   * @param pipelineID ID of the pipeline to which container is added.
   * @param containerID ID of the container which is added to the pipeline.
   * @throws IOException in case of any Exception
   */
  void addContainerToPipelineSCMStart(PipelineID pipelineID, ContainerID containerID) throws PipelineNotFoundException;

  void removeContainerFromPipeline(PipelineID pipelineID, ContainerID containerID);

  NavigableSet<ContainerID> getContainersInPipeline(PipelineID pipelineID) throws PipelineNotFoundException;

  int getNumberOfContainers(PipelineID pipelineID) throws PipelineNotFoundException;

  void openPipeline(PipelineID pipelineId) throws IOException;

  void closePipeline(PipelineID pipelineID) throws IOException;

  void deletePipeline(PipelineID pipelineID) throws IOException;

  void closeStalePipelines(DatanodeDetails datanodeDetails);

  void scrubPipelines() throws IOException;

  void startPipelineCreator();

  void triggerPipelineCreation();

  void incNumBlocksAllocatedMetric(PipelineID id);

  int minHealthyVolumeNum(Pipeline pipeline);

  int minPipelineLimit(Pipeline pipeline);

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  void activatePipeline(PipelineID pipelineID)
      throws IOException;

  /**
   * Deactivates an active pipeline.
   *
   * @param pipelineID ID of the pipeline to deactivate.
   * @throws IOException in case of any Exception
   */
  void deactivatePipeline(PipelineID pipelineID)
      throws IOException;

  /**
   * Wait a pipeline to be OPEN.
   *
   * @param pipelineID ID of the pipeline to wait for.
   * @param timeout    wait timeout(millisecond), if 0, use default timeout
   * @throws IOException in case of any Exception, such as timeout
   */
  default void waitPipelineReady(PipelineID pipelineID, long timeout)
      throws IOException {
  }

  /**
   * Wait one pipeline to be OPEN among a collection pipelines.
   * @param pipelineIDs ID collection of the pipelines to wait for
   * @param timeout wait timeout(millisecond), if 0, use default timeout
   * @return Pipeline the pipeline which is OPEN
   * @throws IOException in case of any Exception, such as timeout
   */
  default Pipeline waitOnePipelineReady(Collection<PipelineID> pipelineIDs,
                                    long timeout)
          throws IOException {
    return null;
  }

  /**
   * Get SafeMode status.
   * @return boolean
   */
  boolean getSafeModeStatus();

  /**
   * Reinitialize the pipelineManager with the lastest pipeline store
   * during SCM reload.
   */
  void reinitialize(Table<PipelineID, Pipeline> pipelineStore)
      throws RocksDatabaseException, DuplicatedPipelineIdException, CodecException;

  /**
   * Ask pipeline manager to not create any new pipelines.
   */
  void freezePipelineCreation();

  /**
   * Ask pipeline manager to resume creating new pipelines.
   */
  void resumePipelineCreation();

  boolean isPipelineCreationFrozen();

  /**
   * Acquire read lock.
   */
  void acquireReadLock();

  /**
   * Release read lock.
   */
  void releaseReadLock();

  /**
   * Acquire write lock.
   */
  void acquireWriteLock();

  /**
   * Release write lock.
   */
  void releaseWriteLock();

  /**
   * Checks whether all Datanodes in the specified pipeline have greater than the specified space, containerSize.
   * @param pipeline pipeline to check
   * @param containerSize the required amount of space
   * @return false if all the volumes on any Datanode in the pipeline have space less than equal to the specified
   * containerSize, otherwise true
   */
  boolean hasEnoughSpace(Pipeline pipeline, long containerSize);

  /**
   * Get the pipeline metrics.
   */
  SCMPipelineMetrics getMetrics();
}
