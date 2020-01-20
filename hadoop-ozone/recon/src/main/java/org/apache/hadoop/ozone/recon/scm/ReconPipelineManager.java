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

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_PIPELINE_DB;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.ReconUtils;

/**
 * Recon's overriding implementation of SCM's Pipeline Manager.
 */
public class ReconPipelineManager extends SCMPipelineManager {

  public ReconPipelineManager(Configuration conf,
                              NodeManager nodeManager,
                              EventPublisher eventPublisher)
      throws IOException {
    super(conf, nodeManager, eventPublisher);
  }

  @Override
  protected File getPipelineDBPath(Configuration conf) {
    File metaDir = ReconUtils.getReconScmDbDir(conf);
    return new File(metaDir, RECON_SCM_PIPELINE_DB);
  }

  @Override
  public void triggerPipelineCreation() {
    // Don't do anything in Recon.
  }

  @Override
  protected void destroyPipeline(Pipeline pipeline) throws IOException {
    // remove the pipeline from the pipeline manager
    removePipeline(pipeline.getId());
  }
}
