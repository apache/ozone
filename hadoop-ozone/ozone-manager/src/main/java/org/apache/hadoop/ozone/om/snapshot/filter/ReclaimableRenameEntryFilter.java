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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.snapshot.filter;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;

import java.io.IOException;

/**
 * Filter to return rename table entries which are reclaimable based on the key presence in previous snapshot's
 * keyTable/DirectoryTable in the snapshot chain.
 */
public class ReclaimableRenameEntryFilter extends ReclaimableFilter<String> {

  /**
   *
   *
   * @param omSnapshotManager
   * @param snapshotChainManager
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param metadataManager      : MetadataManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableRenameEntryFilter(OzoneManager ozoneManager,
                                      OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                                      SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                                      IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 1);
  }

  @Override
  protected Boolean isReclaimable(Table.KeyValue<String, String> renameEntry) throws IOException {
    ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
    Table<String, OmKeyInfo> previousKeyTable = null;
    Table<String, OmDirectoryInfo> prevDirTable = null;
    if (previousSnapshot != null) {
      previousKeyTable = previousSnapshot.get().getMetadataManager().getKeyTable(getBucketInfo().getBucketLayout());
      prevDirTable = previousSnapshot.get().getMetadataManager().getDirectoryTable();
    }
    return isRenameEntryReclaimable(renameEntry, prevDirTable, previousKeyTable);
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, String> keyValue) throws IOException {
    return getMetadataManager().splitRenameKey(keyValue.getKey())[0];
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, String> keyValue) throws IOException {
    return getMetadataManager().splitRenameKey(keyValue.getKey())[1];
  }

  private boolean isRenameEntryReclaimable(Table.KeyValue<String, String> renameEntry,
                                           Table<String, OmDirectoryInfo> previousDirTable,
                                           Table<String, OmKeyInfo> prevKeyInfoTable) throws IOException {

    if (previousDirTable == null && prevKeyInfoTable == null) {
      return true;
    }
    String prevDbKey = renameEntry.getValue();


    if (previousDirTable != null) {
      OmDirectoryInfo prevDirectoryInfo = previousDirTable.getIfExist(prevDbKey);
      if (prevDirectoryInfo != null) {
        return false;
      }
    }

    if (prevKeyInfoTable != null) {
      OmKeyInfo omKeyInfo = prevKeyInfoTable.getIfExist(prevDbKey);
      return omKeyInfo == null;
    }
    return true;
  }
}
