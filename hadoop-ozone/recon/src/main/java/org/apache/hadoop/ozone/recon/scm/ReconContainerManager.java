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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_CONTAINER_DB;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's overriding implementation of SCM's Container Manager.
 */
public class ReconContainerManager extends SCMContainerManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerManager.class);

  /**
   * Constructs a mapping class that creates mapping between container names
   * and pipelines.
   * <p>
   * passed to LevelDB and this memory is allocated in Native code space.
   * CacheSize is specified
   * in MB.
   *
   * @param conf            - {@link Configuration}
   * @param pipelineManager - {@link PipelineManager}
   * @throws IOException on Failure.
   */
  public ReconContainerManager(
      Configuration conf, PipelineManager pipelineManager) throws IOException {
    super(conf, pipelineManager);
  }

  @Override
  protected File getContainerDBPath(Configuration conf) {
    File metaDir = ReconUtils.getReconScmDbDir(conf);
    return new File(metaDir, RECON_SCM_CONTAINER_DB);
  }

  /**
   * Adds a new container to Recon's container manager.
   * @param containerId id
   * @param containerWithPipeline containerInfo with pipeline info
   * @throws IOException on Error.
   */
  public void addNewContainer(long containerId,
                              ContainerWithPipeline containerWithPipeline)
      throws IOException {
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();
    getLock().lock();
    try {
      if (getPipelineManager().containsPipeline(
          containerWithPipeline.getPipeline().getId())) {
        getContainerStateManager().addContainerInfo(containerId, containerInfo,
            getPipelineManager(), containerWithPipeline.getPipeline());
        addContainerToDB(containerInfo);
      }
    } catch (IOException ex) {
      LOG.info("Exception while adding container {} .",
          containerInfo.containerID(), ex);
      getPipelineManager().removeContainerFromPipeline(
          containerInfo.getPipelineID(),
          new ContainerID(containerInfo.getContainerID()));
      throw ex;
    } finally {
      getLock().unlock();
    }
  }
}
