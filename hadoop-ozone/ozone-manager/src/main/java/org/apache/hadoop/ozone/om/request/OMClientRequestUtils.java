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

package org.apache.hadoop.ozone.om.request;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for OMClientRequest. Validates that the bucket layout expected
 * by the Request class is the same as the layout of the bucket being worked on.
 */
public final class OMClientRequestUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMClientRequestUtils.class);

  private OMClientRequestUtils() {
  }

  public static void checkClientRequestPrecondition(
      BucketLayout dbBucketLayout, BucketLayout reqClassBucketLayout)
      throws OMException {
    if (dbBucketLayout.isFileSystemOptimized() !=
        reqClassBucketLayout.isFileSystemOptimized()) {
      String errMsg =
          "BucketLayout mismatch. DB BucketLayout " + dbBucketLayout +
              " and OMRequestClass BucketLayout " + reqClassBucketLayout;
      LOG.error(errMsg);
      throw new OMException(
          errMsg,
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  public static boolean isSnapshotBucket(OMMetadataManager omMetadataManager,
      OmKeyInfo keyInfo) throws IOException {

    String dbSnapshotBucketKey = omMetadataManager.getBucketKey(
        keyInfo.getVolumeName(), keyInfo.getBucketName())
        + OM_KEY_PREFIX;

    return checkInSnapshotCache(omMetadataManager, dbSnapshotBucketKey) ||
        checkInSnapshotDB(omMetadataManager, dbSnapshotBucketKey);
  }

  private static boolean checkInSnapshotDB(OMMetadataManager omMetadataManager,
      String dbSnapshotBucketKey) throws IOException {
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        iterator = omMetadataManager.getSnapshotInfoTable().iterator()) {
      iterator.seek(dbSnapshotBucketKey);
      return iterator.hasNext() && iterator.next().getKey()
          .startsWith(dbSnapshotBucketKey);
    }
  }

  private static boolean checkInSnapshotCache(
      OMMetadataManager omMetadataManager,
      String dbSnapshotBucketKey) {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<SnapshotInfo>>> cacheIter =
        omMetadataManager.getSnapshotInfoTable().cacheIterator();

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<SnapshotInfo>> cacheKeyValue =
          cacheIter.next();
      String cacheKey = cacheKeyValue.getKey().getCacheKey();
      SnapshotInfo cacheValue = cacheKeyValue.getValue().getCacheValue();
      if (cacheKey.startsWith(dbSnapshotBucketKey) && cacheValue != null) {
        return true;
      }
    }
    return false;
  }

  public static boolean shouldLogClientRequestFailure(IOException exception) {
    if (!(exception instanceof OMException)) {
      return true;
    }
    OMException omException = (OMException) exception;
    switch (omException.getResult()) {
    case KEY_NOT_FOUND:
      return false;
    default:
      return true;
    }
  }
}
