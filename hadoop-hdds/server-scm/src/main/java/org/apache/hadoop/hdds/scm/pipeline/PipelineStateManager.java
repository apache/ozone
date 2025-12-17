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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Manages the state of pipelines in SCM.
 */
public interface PipelineStateManager {

  /**
   * Adding pipeline would be replicated to Ratis.
   * @param pipelineProto
   * @throws IOException
   */
  @Replicate
  void addPipeline(HddsProtos.Pipeline pipelineProto)
      throws IOException;

  /**
   * Removing pipeline would be replicated to Ratis.
   * @param pipelineIDProto
   * @throws IOException
   */
  @Replicate
  void removePipeline(HddsProtos.PipelineID pipelineIDProto)
      throws IOException;

  /**
   * Updating pipeline state would be replicated to Ratis.
   * @param pipelineIDProto
   * @param newState
   * @throws IOException
   */
  @Replicate
  void updatePipelineState(HddsProtos.PipelineID pipelineIDProto,
      HddsProtos.PipelineState newState) throws IOException;

  void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws PipelineNotFoundException, InvalidPipelineStateException;

  /**
   * Adds container to SCM Pipeline without checking whether pipeline
   * is closed.
   */
  void addContainerToPipelineForce(PipelineID pipelineID, ContainerID containerID) throws PipelineNotFoundException;

  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException;

  List<Pipeline> getPipelines();

  List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig
  );

  List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state
  );

  List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state,
      Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines
  );

  int getPipelineCount(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state
  );

  NavigableSet<ContainerID> getContainers(PipelineID pipelineID) throws PipelineNotFoundException;

  int getNumberOfContainers(PipelineID pipelineID) throws PipelineNotFoundException;

  void removeContainerFromPipeline(PipelineID pipelineID, ContainerID containerID);

  void close();

  void reinitialize(Table<PipelineID, Pipeline> pipelineStore)
      throws RocksDatabaseException, DuplicatedPipelineIdException, CodecException;
}
