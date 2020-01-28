/**
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

import static org.apache.hadoop.hdds.scm.client.ContainerOperationClient.newContainerRpcClient;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for StorageContainerServiceProvider that talks with actual
 * cluster SCM.
 */
public class StorageContainerServiceProviderImpl
    implements StorageContainerServiceProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerServiceProviderImpl.class);

  private OzoneConfiguration configuration;
  private StorageContainerLocationProtocol scmClient;
  private volatile boolean isInitialized = false;

  @Inject
  public StorageContainerServiceProviderImpl(OzoneConfiguration configuration,
      StorageContainerLocationProtocol scmClient) {
    this.configuration = configuration;
    this.scmClient = scmClient;
    if (this.scmClient != null) {
      initialize();
      isInitialized = true;
    }
  }

  private void initialize() {
    if (!isInitialized) {
      try {
        this.scmClient = newContainerRpcClient(configuration);
      } catch (IOException ioEx) {
        LOG.error("Exception encountered while creating SCM client.", ioEx);
      }
      isInitialized = true;
    }
  }

  @Override
  public List<Pipeline> getPipelines() throws IOException {
    if (isInitialized) {
      return scmClient.listPipelines();
    } else {
      throw new IOException("SCM client not initialized");
    }
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    if (isInitialized) {
      return scmClient.getPipeline(pipelineID);
    } else {
      throw new IOException("SCM client not initialized");
    }
  }

}
