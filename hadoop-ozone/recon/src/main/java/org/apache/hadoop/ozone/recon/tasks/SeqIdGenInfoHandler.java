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
import org.apache.hadoop.ozone.recon.scm.ReconScmMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Manages sequenceId Table info of SCM MetaData DB, updates Recon's in-memory
 * SequenceIdGenerator objects.
 */
public class SeqIdGenInfoHandler implements SCMMetaDataTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SeqIdGenInfoHandler.class);

  private ReconScmMetadataManager scmMetadataManager;

  public SeqIdGenInfoHandler(ReconScmMetadataManager scmMetadataManager) {
    this.scmMetadataManager = scmMetadataManager;
  }

  /**
   * Handles a PUT event on scm metadata DB tables.
   *
   * @param event The PUT event to be processed.
   */
  @Override
  public void handlePutEvent(RocksDBUpdateEvent<?, Object> event) {
    final String sequenceIdName = (String) event.getKey();
    final Long lastId = (Long) event.getValue();
    try {
      handlePutSequenceIdGenEvent(sequenceIdName, lastId);
    } catch (IOException ioe) {
      LOG.error("Unexpected error while handling add new seqIdGen event for sequence name: {} - ", sequenceIdName, ioe);
    }
  }

  private void handlePutSequenceIdGenEvent(String sequenceIdName, Long lastId) throws IOException {
    scmMetadataManager.getSequenceIdGen().initialize(sequenceIdName, lastId);
  }

  /**
   * Handles a DELETE event on scm metadata DB tables.
   *
   * @param event The DELETE event to be processed.
   */
  @Override
  public void handleDeleteEvent(RocksDBUpdateEvent<?, Object> event) {
    final String sequenceIdName = (String) event.getKey();
    LOG.error("Delete event should not be raised on sequenceId table for sequence name: {} - ", sequenceIdName);
  }

  /**
   * Handles an UPDATE event on scm metadata DB tables.
   *
   * @param event The UPDATE event to be processed.
   */
  @Override
  public void handleUpdateEvent(RocksDBUpdateEvent<?, Object> event) {
    final String sequenceIdName = (String) event.getKey();
    LOG.error("Update event should not be raised on sequenceId table for sequence name: {} - ", sequenceIdName);
  }

  /**
   * Return handler name.
   *
   * @return handler name
   */
  @Override
  public String getHandler() {
    return "SeqIdGenInfoHandler";
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
    // Left NoOps implementation as SequenceIdGenerator reinitialization is being done
    // during full SCM metadata DB sync already. Later, in future this impl can be extended
    // for capturing or processing any further stats specific to Recon, related to SequenceIdGen.
    return new ImmutablePair<>(getHandler(), true);
  }
}
