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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Class for handling OBS specific tasks.
 */
public class NSSummaryTaskWithOBS extends NSSummaryTaskDbEventHandler {

  private static final BucketLayout BUCKET_LAYOUT = BucketLayout.OBJECT_STORE;

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithOBS.class);


  public NSSummaryTaskWithOBS(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager,
      OzoneConfiguration ozoneConfiguration) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
  }


  public boolean reprocessWithOBS(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    try {
      Table<String, OmKeyInfo> keyTable =
          omMetadataManager.getKeyTable(BUCKET_LAYOUT);

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               keyTableIter = keyTable.iterator()) {

        while (keyTableIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
          OmKeyInfo keyInfo = kv.getValue();

          // KeyTable entries belong to both Legacy and OBS buckets.
          // Check bucket layout and if it's anything other than OBS,
          // continue to the next iteration.
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          String bucketDBKey = omMetadataManager
              .getBucketKey(volumeName, bucketName);
          // Get bucket info from bucket table
          OmBucketInfo omBucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketDBKey);

          if (omBucketInfo.getBucketLayout() != BucketLayout.OBJECT_STORE) {
            continue;
          }

          setKeyParentID(keyInfo);

          handlePutKeyEvent(keyInfo, nsSummaryMap);
          if (!checkAndCallFlushToDB(nsSummaryMap)) {
            return false;
          }
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
          ioEx);
      return false;
    }

    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }
    LOG.info("Completed a reprocess run of NSSummaryTaskWithOBS");
    return true;
  }

  /**
   * KeyTable entries don't have the parentId set.
   * In order to reuse the existing methods that rely on
   * the parentId, we have to set it explicitly.
   * Note: For an OBS key, the parentId will always correspond to the ID of the
   * OBS bucket in which it is located.
   *
   * @param keyInfo
   * @throws IOException
   */
  private void setKeyParentID(OmKeyInfo keyInfo)
      throws IOException {
    String bucketKey = getReconOMMetadataManager()
        .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());
    OmBucketInfo parentBucketInfo =
        getReconOMMetadataManager().getBucketTable().getSkipCache(bucketKey);

    if (parentBucketInfo != null) {
      keyInfo.setParentObjectID(parentBucketInfo.getObjectID());
    } else {
      throw new IOException("ParentKeyInfo for " +
          "NSSummaryTaskWithLegacy is null");
    }
  }

}