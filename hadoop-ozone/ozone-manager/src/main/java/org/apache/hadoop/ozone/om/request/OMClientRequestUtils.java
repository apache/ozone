/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

  /**
   * Validates the bucket associated with the request - to make sure it did
   * not change since the request started processing.
   *
   * @param bucketId  - bucket ID of the associated bucket when the request
   *                  is being processed.
   * @param omRequest - request to be validated, contains the bucket ID of the
   *                  associated bucket when the request was created.
   * @throws OMException
   */
  public static void validateAssociatedBucketId(long bucketId,
                                                OMRequest omRequest)
      throws OMException {
    if (omRequest.hasAssociatedBucketId()) {
      if (bucketId != omRequest.getAssociatedBucketId()) {
        throw new OMException(
            "Bucket ID mismatch. Associated bucket was modified concurrently" +
                " while " + omRequest.getCmdType() +
                " request was being processed. Please retry the request.",
            OMException.ResultCodes.BUCKET_ID_MISMATCH);
      }
    }
  }

  /**
   * Returns bucket ID of the given bucket. In case the given bucket is a link -
   * returns the bucket ID of the source bucket.
   *
   * @param ozoneManager - Ozone Manager instance.
   * @param volume       - volume name.
   * @param bucket       - bucket name.
   * @return bucket ID of the resolved bucket.
   * @throws IOException
   */
  public static long getResolvedBucketId(OzoneManager ozoneManager,
                                  String volume,
                                  String bucket) throws IOException {
    // Fetch the bucket ID for request validation.
    // We fetch source bucket ID in case of link buckets.
    ResolvedBucket resolvedBucket =
        ozoneManager.resolveBucketLink(Pair.of(volume, bucket));
    long bucketId = ozoneManager.getMetadataManager()
        .getBucketId(resolvedBucket.realVolume(),
            resolvedBucket.realBucket());
    return bucketId;
  }
}
