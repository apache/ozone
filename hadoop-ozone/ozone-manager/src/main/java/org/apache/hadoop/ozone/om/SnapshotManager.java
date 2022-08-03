/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

import java.io.IOException;

/**
 * This class is used to manage/create OM snapshots.
 */
public final class SnapshotManager {

  
  /**
   * Creates snapshot checkpoint that corresponds with SnapshotInfo.
   * @param OMMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createSnapshot(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    return store.getSnapshot(snapshotInfo.getCheckpointDirName());
  }

  private SnapshotManager() { }

}
