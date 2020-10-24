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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.ratis.protocol.NotLeaderException;

/**
 * Interface which exposes the api for pipeline management.
 */
public interface PipelineManager extends Closeable, PipelineManagerMXBean,
    EventHandler<SafeModeStatus> {

  Pipeline createPipeline(ReplicationType type, ReplicationFactor factor)
      throws IOException;

  Pipeline createPipeline(ReplicationType type, ReplicationFactor factor,
      List<DatanodeDetails> nodes);

  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException;

  boolean containsPipeline(PipelineID pipelineID);

  List<Pipeline> getPipelines();

  List<Pipeline> getPipelines(ReplicationType type);

  List<Pipeline> getPipelines(ReplicationType type,
      ReplicationFactor factor);

  List<Pipeline> getPipelines(ReplicationType type,
      Pipeline.PipelineState state) throws NotLeaderException;

  List<Pipeline> getPipelines(ReplicationType type,
      ReplicationFactor factor, Pipeline.PipelineState state);

  List<Pipeline> getPipelines(ReplicationType type, ReplicationFactor factor,
      Pipeline.PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines);

  void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws IOException;

  void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException;

  NavigableSet<ContainerID> getContainersInPipeline(PipelineID pipelineID)
      throws IOException;

  int getNumberOfContainers(PipelineID pipelineID) throws IOException;

  void openPipeline(PipelineID pipelineId) throws IOException;

  void closePipeline(Pipeline pipeline, boolean onTimeout) throws IOException;

  void scrubPipeline(ReplicationType type, ReplicationFactor factor)
      throws IOException;

  void startPipelineCreator();

  void triggerPipelineCreation() throws NotLeaderException;

  void incNumBlocksAllocatedMetric(PipelineID id);

  int minHealthyVolumeNum(Pipeline pipeline);

  int minPipelineLimit(Pipeline pipeline);

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  void activatePipeline(PipelineID pipelineID) throws IOException;

  /**
   * Deactivates an active pipeline.
   *
   * @param pipelineID ID of the pipeline to deactivate.
   * @throws IOException in case of any Exception
   */
  void deactivatePipeline(PipelineID pipelineID) throws IOException;

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
   * Get SafeMode status.
   * @return boolean
   */
  boolean getSafeModeStatus();
}
