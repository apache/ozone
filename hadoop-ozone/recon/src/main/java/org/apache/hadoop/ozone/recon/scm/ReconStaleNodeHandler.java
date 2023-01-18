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

package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's handling of Stale node.
 */
public class ReconStaleNodeHandler extends StaleNodeHandler {

  private static final Logger LOG =
        LoggerFactory.getLogger(ReconStaleNodeHandler.class);
  private PipelineSyncTask pipelineSyncTask;

  public ReconStaleNodeHandler(NodeManager nodeManager,
                                 PipelineManager pipelineManager,
                                 OzoneConfiguration conf,
                                 PipelineSyncTask pipelineSyncTask) {
    super(nodeManager, pipelineManager, conf);
    this.pipelineSyncTask = pipelineSyncTask;
  }

  @Override
  public void onMessage(final DatanodeDetails datanodeDetails,
                          final EventPublisher publisher) {
    super.onMessage(datanodeDetails, publisher);
    try {
      pipelineSyncTask.triggerPipelineSyncTask();
    } catch (Exception exp) {
      LOG.error("Error trying to trigger pipeline sync task..",
          exp);
    }
  }
}
