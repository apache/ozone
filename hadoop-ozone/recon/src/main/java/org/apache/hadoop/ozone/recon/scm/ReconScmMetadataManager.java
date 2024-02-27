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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Interface for the SCM Metadata Manager + SCM DB store maintained by
 * Recon.
 */
public interface ReconScmMetadataManager extends SCMMetadataStore {

  public static final String CONTAINERS_TABLE = "containers";
  public static final String PIPELINES_TABLE = "pipelines";
  public static final String NODES_TABLE = "nodes";

  /**
   * Refresh the DB instance to point to a new location. Get rid of the old
   * DB instance.
   *
   * @param dbLocation  New location of the SCM Snapshot DB.
   */
  void updateScmDB(File dbLocation) throws IOException;

  /**
   * Get the most recent sequence number from the SCM Metadata Database.
   */
  long getLastSequenceNumberFromDB();

  /**
   * Return table mapped to the specified table name.
   * @param tableName
   * @return Table
   */
  Table getTable(String tableName);

  void setOzoneStorageContainerManager(OzoneStorageContainerManager ozoneStorageContainerManager);

  void setSequenceIdGen(SequenceIdGenerator sequenceIdGen);

  void setNodeManager(ReconNodeManager nodeManager);

  OzoneStorageContainerManager getOzoneStorageContainerManager();

  SequenceIdGenerator getSequenceIdGen();

  ReconNodeManager getNodeManager();

  Table<UUID, DatanodeDetails> getNodesTable();
}
