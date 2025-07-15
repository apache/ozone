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

package org.apache.hadoop.ozone.om.request.snapshot;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

/**
 * Utility class for snapshot move requests.
 */
public final class OMSnapshotMoveUtils {

  private OMSnapshotMoveUtils() {
  }

  public static void updateCache(OzoneManager ozoneManager, SnapshotInfo fromSnapshot, SnapshotInfo toSnapshot,
      ExecutionContext context) throws IOException {
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();

    // Update lastTransactionInfo for fromSnapshot and the nextSnapshot.
    fromSnapshot.setLastTransactionInfo(TransactionInfo.valueOf(
        context.getTermIndex()).toByteString());
    omMetadataManager.getSnapshotInfoTable().addCacheEntry(
        new CacheKey<>(fromSnapshot.getTableKey()),
        CacheValue.get(context.getIndex(), fromSnapshot));
    if (toSnapshot != null) {
      toSnapshot.setLastTransactionInfo(TransactionInfo.valueOf(
          context.getTermIndex()).toByteString());
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(toSnapshot.getTableKey()),
          CacheValue.get(context.getIndex(), toSnapshot));
    }
  }
}
