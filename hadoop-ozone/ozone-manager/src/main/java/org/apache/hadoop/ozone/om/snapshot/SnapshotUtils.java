/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;

/**
 * Util class for snapshot diff APIs.
 */
public final class SnapshotUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotUtils.class);

  private SnapshotUtils() {
    throw new IllegalStateException("SnapshotUtils should not be initialized.");
  }

  public static SnapshotInfo getSnapshotInfo(final OzoneManager ozoneManager,
                                             final String volumeName,
                                             final String bucketName,
                                             final String snapshotName)
      throws IOException {
    return getSnapshotInfo(ozoneManager,
        SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
  }

  public static SnapshotInfo getSnapshotInfo(final OzoneManager ozoneManager,
                                             final String key)
      throws IOException {
    SnapshotInfo snapshotInfo;
    try {
      snapshotInfo = ozoneManager.getMetadataManager()
          .getSnapshotInfoTable()
          .get(key);
    } catch (IOException e) {
      LOG.error("Snapshot {}: not found: {}", key, e);
      throw e;
    }
    if (snapshotInfo == null) {
      throw new OMException(KEY_NOT_FOUND);
    }
    return snapshotInfo;
  }

  public static void dropColumnFamilyHandle(
      final ManagedRocksDB rocksDB,
      final ColumnFamilyHandle columnFamilyHandle) {

    if (columnFamilyHandle == null) {
      return;
    }

    try {
      rocksDB.get().dropColumnFamily(columnFamilyHandle);
    } catch (RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }


  /**
   * Throws OMException FILE_NOT_FOUND if snapshot is not in active status.
   * @param snapshotTableKey snapshot table key
   */
  public static void checkSnapshotActive(OzoneManager ozoneManager,
                                         String snapshotTableKey)
      throws IOException {
    checkSnapshotActive(getSnapshotInfo(ozoneManager, snapshotTableKey));
  }

  public static void checkSnapshotActive(SnapshotInfo snapInfo)
      throws OMException {

    if (snapInfo.getSnapshotStatus() != SnapshotStatus.SNAPSHOT_ACTIVE) {
      if (isCalledFromSnapshotDeletingService()) {
        LOG.debug("Permitting {} to load snapshot {} even in status: {}",
            SnapshotDeletingService.class.getSimpleName(),
            snapInfo.getTableKey(),
            snapInfo.getSnapshotStatus());
      } else {
        throw new OMException("Unable to load snapshot. " +
            "Snapshot with table key '" + snapInfo.getTableKey() +
            "' is no longer active", FILE_NOT_FOUND);
      }
    }
  }

  /**
   * Helper method to check whether the loader is called from
   * SnapshotDeletingTask (return true) or not (return false).
   */
  private static boolean isCalledFromSnapshotDeletingService() {

    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    for (StackTraceElement elem : stackTrace) {
      // Allow as long as loader is called from SDS. e.g. SnapshotDeletingTask
      if (elem.getClassName().startsWith(
          SnapshotDeletingService.class.getName())) {
        return true;
      }
    }

    return false;
  }

}
