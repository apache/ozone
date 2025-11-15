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

package org.apache.hadoop.ozone.recon.scm;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineReportHandler;
import org.apache.hadoop.hdds.scm.safemode.SafeModeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's implementation of Pipeline Report handler.
 */
public class ReconPipelineReportHandler extends PipelineReportHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReconPipelineReportHandler.class);

  private StorageContainerServiceProvider scmServiceProvider;

  public ReconPipelineReportHandler(SafeModeManager scmSafeModeManager,
      PipelineManager pipelineManager,
      SCMContext scmContext,
      ConfigurationSource conf,
      StorageContainerServiceProvider scmServiceProvider) {
    super(scmSafeModeManager, pipelineManager, scmContext, conf);
    this.scmServiceProvider = scmServiceProvider;
  }

  @Override
  protected void processPipelineReport(PipelineReport report,
      DatanodeDetails dn, EventPublisher publisher)
      throws IOException, TimeoutException {
    ReconPipelineManager reconPipelineManager =
        (ReconPipelineManager)getPipelineManager();

    PipelineID pipelineID = PipelineID.getFromProtobuf(report.getPipelineID());
    if (!reconPipelineManager.containsPipeline(pipelineID)) {
      LOG.info("Unknown pipeline {}. Trying to get from SCM.", pipelineID);
      Pipeline pipelineFromScm;

      try {
        pipelineFromScm =
            scmServiceProvider.getPipeline(report.getPipelineID());
      } catch (IOException ex) {
        if (ex instanceof RemoteException) {
          IOException ioe = ((RemoteException) ex)
                  .unwrapRemoteException(PipelineNotFoundException.class);
          if (ioe instanceof PipelineNotFoundException) {
            LOG.error("Could not find pipeline {} at SCM.", pipelineID);
            throw new PipelineNotFoundException();
          }
        }
        throw ex;
      }

      if (reconPipelineManager.addPipeline(pipelineFromScm)) {
        LOG.info("Pipeline {} verified from SCM and added to Recon pipeline metadata.",
            pipelineFromScm);
      }
    }

    Pipeline pipeline;
    try {
      pipeline = reconPipelineManager.getPipeline(pipelineID);
    } catch (PipelineNotFoundException ex) {
      LOG.warn("Pipeline {} not found in Recon.", pipelineID);
      return;
    }

    setReportedDatanode(pipeline, dn);
    setPipelineLeaderId(report, pipeline, dn);

    if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
      LOG.info("Pipeline {} {} reported by {}", pipeline.getReplicationConfig(),
          pipeline.getId(), dn);
      if (pipeline.isHealthy()) {
        reconPipelineManager.openPipeline(pipelineID);
      }
    }
  }
}
