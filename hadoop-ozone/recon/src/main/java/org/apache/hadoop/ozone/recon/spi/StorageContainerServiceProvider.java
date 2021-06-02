/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

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

}
