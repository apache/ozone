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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Class to filter out deleted directories which are reclaimable based on their presence in previous snapshot in
 * the snapshot chain.
 */
public class ReclaimableDirFilter extends ReclaimableFilter<OmKeyInfo> {

  public ReclaimableDirFilter(OzoneManager ozoneManager,
                              OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                              SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
                              IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock, 1);
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
    UncheckedAutoCloseableSupplier<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
    KeyManager prevKeyManager = previousSnapshot == null ? null : previousSnapshot.get().getKeyManager();
    return isDirReclaimable(getVolumeId(), getBucketInfo(), deletedDirInfo.getValue(), getKeyManager(), prevKeyManager);
  }

  private boolean isDirReclaimable(long volumeId, OmBucketInfo bucketInfo, OmKeyInfo dirInfo,
                                   KeyManager keyManager, KeyManager previousKeyManager) throws IOException {
    if (previousKeyManager == null) {
      return true;
    }
    OmDirectoryInfo prevDirectoryInfo =
        keyManager.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, dirInfo).apply(previousKeyManager);
    return prevDirectoryInfo == null || prevDirectoryInfo.getObjectID() != dirInfo.getObjectID();
  }
}
