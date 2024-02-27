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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.recon.scm.ReconScmMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Manages Nodes Table info of SCM MetaData DB, updates Recon's in-memory
 * Node manager objects and global stats table to update various nodes stats.
 */
public class NodeInfoHandler implements SCMMetaDataTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeInfoHandler.class);

  /**
   * Handles a PUT event on scm metadata DB tables.
   *
   * @param event     The PUT event to be processed.
   * @param tableName Table name associated with the event.
   */
  @Override
  public void handlePutEvent(RocksDBUpdateEvent<?, Object> event, String tableName) {

  }

  /**
   * Handles a DELETE event on scm metadata DB tables.
   *
   * @param event     The DELETE event to be processed.
   * @param tableName Table name associated with the event.
   */
  @Override
  public void handleDeleteEvent(RocksDBUpdateEvent<?, Object> event, String tableName) {

  }

  /**
   * Handles an UPDATE event on scm metadata DB tables.
   *
   * @param event     The UPDATE event to be processed.
   * @param tableName Table name associated with the event.
   */
  @Override
  public void handleUpdateEvent(RocksDBUpdateEvent<?, Object> event, String tableName) {

  }

  /**
   * Return handler name.
   *
   * @return handler name
   */
  @Override
  public String getHandler() {
    return null;
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
    return null;
  }
}
