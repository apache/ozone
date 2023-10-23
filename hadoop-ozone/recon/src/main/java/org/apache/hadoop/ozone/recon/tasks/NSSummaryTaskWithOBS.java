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
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Class for handling OBS specific tasks.
 */
public class NSSummaryTaskWithOBS extends NSSummaryTaskDbEventHandler {

  private static final BucketLayout BUCKET_LAYOUT = BucketLayout.OBJECT_STORE;

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithOBS.class);

  private boolean enableFileSystemPaths;

  public NSSummaryTaskWithOBS(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager,
                              ReconOMMetadataManager
                                 reconOMMetadataManager,
                              OzoneConfiguration
                                 ozoneConfiguration) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
    // true if FileSystemPaths enabled
    enableFileSystemPaths = ozoneConfiguration
        .getBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
            OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT);
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
          // Check bucket layout and if it's Legacy-FS
          // continue to the next iteration.
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          String bucketDBKey = omMetadataManager
              .getBucketKey(volumeName, bucketName);
          // Get bucket info from bucket table
          OmBucketInfo omBucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketDBKey);

          if (!omBucketInfo.getBucketLayout()
              .isObjectStore(enableFileSystemPaths)) {
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
   * In order to reuse the existing FSO methods that rely on
   * the parentId, we have to set it explicitly.
   * @param keyInfo
   * @throws IOException
   */
  private void setKeyParentID(OmKeyInfo keyInfo) throws IOException {
    System.out.println("#### INSIDE NSSummaryTaskWithOBS #### ");
    // keyPath: [key1-legacy]
    // OM_KEY_PREFIX: /
    System.out.println("keyName: " + keyInfo.getKeyName());
    System.out.println("OM_KEY_PREFIX: " + OM_KEY_PREFIX);

      String bucketKey = getReconOMMetadataManager()
          .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());

      // bucketKey: /s3v/legacy-bucket
      System.out.println("bucketKey: " + bucketKey);

      OmBucketInfo parentBucketInfo =
          getReconOMMetadataManager().getBucketTable().getSkipCache(bucketKey);

      System.out.println("parentBucketInfo: " + parentBucketInfo);

      if (parentBucketInfo != null) {
        keyInfo.setParentObjectID(parentBucketInfo.getObjectID());
      } else {
        throw new IOException("ParentKeyInfo for " +
            "NSSummaryTaskWithOBS is null");
    }
    System.out.println("#### GOING OUTSIDE NSSummaryTaskWithOBS #### ");

  }

}