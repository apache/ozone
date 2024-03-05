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

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.recon.scm.ReconScmMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Manages Pipeline Table info of SCM MetaData DB, updates Recon's in-memory
 * Pipeline manager objects.
 */
public class PipelineInfoHandler implements SCMMetaDataTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineInfoHandler.class);

  private ReconScmMetadataManager scmMetadataManager;

  public PipelineInfoHandler(ReconScmMetadataManager scmMetadataManager) {
    this.scmMetadataManager = scmMetadataManager;
  }

  /**
   * Handles a PUT event on scm metadata DB tables.
   *
   * @param event The PUT event to be processed.
   */
  @Override
  public void handlePutEvent(RocksDBUpdateEvent<?, Object> event) {
    PipelineID pipelineID = (PipelineID) event.getKey();
    Pipeline pipeline = (Pipeline) event.getValue();
    try {
      LOG.info("handlePutEvent for add new pipeline event for pipelineId: {}", pipelineID);
      handlePutPipelineEvent(pipeline);
    } catch (IOException ioe) {
      LOG.error("Unexpected error while handling add new pipeline event for pipelineId: {} - ", pipelineID, ioe);
    }
  }

  private void handlePutPipelineEvent(Pipeline pipeline) throws IOException {
    scmMetadataManager.getOzoneStorageContainerManager().getPipelineManager().initialize(pipeline);
  }

  /**
   * Handles a DELETE event on scm metadata DB tables.
   *
   * @param event The DELETE event to be processed.
   */
  @Override
  public void handleDeleteEvent(RocksDBUpdateEvent<?, Object> event) {
    PipelineID pipelineID = (PipelineID) event.getKey();
    Pipeline pipeline = (Pipeline) event.getValue();
    try {
      handleDeletePipelineEvent(pipeline);
    } catch (IOException ioe) {
      LOG.error("Unexpected error while handling delete pipeline event for pipelineId: {} - ", pipelineID, ioe);
    }
  }

  private void handleDeletePipelineEvent(Pipeline pipeline) throws IOException {
    scmMetadataManager.getOzoneStorageContainerManager().getPipelineManager().deletePipeline(pipeline.getId());
  }

  /**
   * Handles an UPDATE event on scm metadata DB tables.
   *
   * @param event The UPDATE event to be processed.
   */
  @Override
  public void handleUpdateEvent(RocksDBUpdateEvent<?, Object> event) {
    PipelineID pipelineID = (PipelineID) event.getKey();
    Pipeline pipeline = (Pipeline) event.getValue();
    try {
      handleUpdatePipelineEvent(pipeline);
    } catch (IOException ioe) {
      LOG.error("Unexpected error while handling update of pipeline event for  pipelineID: {} - ", pipelineID, ioe);
    }
  }

  private void handleUpdatePipelineEvent(Pipeline pipeline) throws IOException {
    scmMetadataManager.getOzoneStorageContainerManager().getPipelineManager()
        .updatePipelineState(pipeline.getId().getProtobuf(),
            pipeline.getProtobufMessage(ClientVersion.CURRENT_VERSION).getState());
  }

  /**
   * Return handler name.
   *
   * @return handler name
   */
  @Override
  public String getHandler() {
    return "PipelineInfoHandler";
  }

  /**
   * Iterates all the rows of desired SCM metadata DB table to capture
   * and process the information further by sending to any downstream class.
   *
   * @param reconScmMetadataManager
   * @return
   * @throws IOException
   */
  @Override
  public Pair<String, Boolean> reprocess(ReconScmMetadataManager reconScmMetadataManager) throws IOException {
    LOG.info("Starting a 'reprocess' run of {}", getHandler());
    // Left NoOps implementation as pipeline manager reinitialization is being done
    // during full SCM metadata DB sync already. Later, in future this impl can be extended
    // for capturing or processing any further stats specific to Recon, related to pipeline.
    return new ImmutablePair<>(getHandler(), true);
  }
}
