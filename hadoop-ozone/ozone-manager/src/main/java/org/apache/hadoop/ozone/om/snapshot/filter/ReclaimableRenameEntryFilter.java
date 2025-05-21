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

package org.apache.hadoop.ozone.om.snapshot.filter;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Class to filter out rename table entries which are reclaimable based on the key presence in previous snapshot's
 * keyTable/DirectoryTable in the snapshot chain.
 */
public class ReclaimableRenameEntryFilter extends ReclaimableFilter<String> {

  public ReclaimableRenameEntryFilter(OzoneManager ozoneManager,
                                      OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                                      SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
                                      IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock, 1);
  }

  /**
   * Function which checks whether the objectId corresponding to the rename entry exists in the previous snapshot. If
   * the entry doesn't exist in the previous keyTable/directoryTable then the rename entry can be deleted since there
   * is no reference for this rename entry.
   * @return true if there is no reference for the objectId based on the rename entry otherwise false.
   * @throws IOException
   */
  @Override
  protected Boolean isReclaimable(Table.KeyValue<String, String> renameEntry) throws IOException {
    UncheckedAutoCloseableSupplier<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
    Table<String, OmKeyInfo> previousKeyTable = null;
    Table<String, OmDirectoryInfo> prevDirTable = null;
    if (previousSnapshot != null) {
      previousKeyTable = previousSnapshot.get().getMetadataManager().getKeyTable(getBucketInfo().getBucketLayout());
      if (getBucketInfo().getBucketLayout().isFileSystemOptimized()) {
        prevDirTable = previousSnapshot.get().getMetadataManager().getDirectoryTable();
      }
    }
    return isRenameEntryReclaimable(renameEntry, prevDirTable, previousKeyTable);
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, String> keyValue) throws IOException {
    return getKeyManager().getMetadataManager().splitRenameKey(keyValue.getKey())[0];
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, String> keyValue) throws IOException {
    return getKeyManager().getMetadataManager().splitRenameKey(keyValue.getKey())[1];
  }

  @SafeVarargs
  private final boolean isRenameEntryReclaimable(Table.KeyValue<String, String> renameEntry,
                                                 Table<String, ? extends WithObjectID>... previousTables)
      throws IOException {
    for (Table<String, ? extends  WithObjectID> previousTable : previousTables) {
      if (previousTable != null) {
        String prevDbKey = renameEntry.getValue();
        WithObjectID withObjectID = previousTable.getIfExist(prevDbKey);
        if (withObjectID != null) {
          return false;
        }
      }
    }
    return true;
  }
}
