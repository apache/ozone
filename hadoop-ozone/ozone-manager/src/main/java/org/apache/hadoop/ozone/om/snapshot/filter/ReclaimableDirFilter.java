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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;

/**
 * Filter to return deleted directories which are reclaimable based on their presence in previous snapshot in
 * the snapshot chain.
 */
public class ReclaimableDirFilter extends ReclaimableFilter<OmKeyInfo> {

  /**
   * Filter to return deleted directories which are reclaimable based on their presence in previous snapshot in
   * the snapshot chain.
   *
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param metadataManager      : MetadataManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableDirFilter(OzoneManager ozoneManager,
                              OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                              SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                              IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 1);
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getVolumeName();
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getBucketName();
  }

  @Override
  protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedDirInfo) throws IOException {
    ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
    Table<String, OmDirectoryInfo> prevDirTable = previousSnapshot == null ? null :
        previousSnapshot.get().getMetadataManager().getDirectoryTable();
    return isDirReclaimable(deletedDirInfo.getValue(), getVolumeId(), getBucketInfo(), prevDirTable,
        getMetadataManager().getSnapshotRenamedTable());
  }

  private boolean isDirReclaimable(OmKeyInfo dirInfo,
                                   long volumeId, OmBucketInfo bucketInfo,
                                   Table<String, OmDirectoryInfo> previousDirTable,
                                   Table<String, String> renamedTable) throws IOException {
    if (previousDirTable == null) {
      return true;
    }
    String dbRenameKey = getOzoneManager().getMetadataManager().getRenameKey(
        dirInfo.getVolumeName(), dirInfo.getBucketName(), dirInfo.getObjectID());

    // snapshotRenamedTable: /volumeName/bucketName/objectID -> /volumeId/bucketId/parentId/dirName
    String dbKeyBeforeRename = renamedTable.getIfExist(dbRenameKey);
    String prevDbKey;
    if (dbKeyBeforeRename != null) {
      prevDbKey = dbKeyBeforeRename;
    } else {
      prevDbKey = getOzoneManager().getMetadataManager().getOzonePathKey(
          volumeId, bucketInfo.getObjectID(), dirInfo.getParentObjectID(), dirInfo.getFileName());
    }

    OmDirectoryInfo prevDirectoryInfo = previousDirTable.get(prevDbKey);
    return prevDirectoryInfo == null || prevDirectoryInfo.getObjectID() != dirInfo.getObjectID();
  }
}
