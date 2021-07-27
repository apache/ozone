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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;

/**
 * Implementation for StorageContainerServiceProvider that talks with actual
 * cluster SCM.
 */
public class StorageContainerServiceProviderImpl
    implements StorageContainerServiceProvider {

  private StorageContainerLocationProtocol scmClient;

  @Inject
  public StorageContainerServiceProviderImpl(
      StorageContainerLocationProtocol scmClient) {
    this.scmClient = scmClient;
  }

  @Override
  public List<Pipeline> getPipelines() throws IOException {
    return scmClient.listPipelines();
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    return scmClient.getPipeline(pipelineID);
  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException {
    return scmClient.getContainerWithPipeline(containerId);
  }

  @Override
  public List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs) {
    return scmClient.getExistContainerWithPipelinesInBatch(containerIDs);
  }

  @Override
  public List<HddsProtos.Node> getNodes() throws IOException {
    return scmClient.queryNode(null, null, HddsProtos.QueryScope.CLUSTER,
        "", CURRENT_VERSION);
  }
}
