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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.getChkptCompactionLogFilename;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.getCurrentCompactionLogFilename;

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

    final DBCheckpoint dbCheckpoint;
    try {
      // The call blocks waiting for all ongoing background work to finish,
      // and pauses any future compactions.
//      store.getDb().pauseBackgroundWork();

      final String compactionLogPathStr = getCurrentCompactionLogFilename();
      final String destFilename =
          getChkptCompactionLogFilename(snapshotInfo.getCheckpointDirName());

      if (new File(compactionLogPathStr).exists()) {
        // TODO: Alternatively, if file does not exist (no compaction happened),
        //  create empty file instead.
        Files.move(Paths.get(compactionLogPathStr),
            Paths.get(store.getSnapshotsParentDir(), destFilename));
      }

      // Create DB checkpoint
      dbCheckpoint = store.getSnapshot(snapshotInfo.getCheckpointDirName());

      // Rename compaction log to correspond to the checkpoint dir name
      // The compaction log contains all the compaction history since last
      // checkpoint, which is used to track all the SST file diffs.

      // TODO: Double check failure recovery paths

    } finally {
//      store.getDb().continueBackgroundWork();
    }

    return dbCheckpoint;
  }

  private SnapshotManager() { }

}
