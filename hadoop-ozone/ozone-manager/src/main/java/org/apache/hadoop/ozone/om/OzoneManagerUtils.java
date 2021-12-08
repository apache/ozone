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

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;

/**
 * Ozone Manager utility class.
 */
public final class OzoneManagerUtils {

  private OzoneManagerUtils() {
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerUtils.class);

  /**
   * All the client requests are executed through
   * OzoneManagerStateMachine#runCommand function and ensures sequential
   * execution path.
   * Below is the call trace to perform OM client request operation:
   * OzoneManagerStateMachine#applyTransaction ->
   * OzoneManagerStateMachine#runCommand ->
   * OzoneManagerRequestHandler#handleWriteRequest ->
   * OzoneManagerRatisUtils#createClientRequest ->
   * OMKeyRequestFactory#createRequest ->
   * ...
   * OzoneManagerUtils#getBucketLayout ->
   * OzoneManagerUtils#getOmBucketInfo ->
   * omMetadataManager().getBucketTable().get(buckKey)
   */

  private static OmBucketInfo getOmBucketInfo(OzoneManager ozoneManager,
      String volName, String buckName) {
    String buckKey =
        ozoneManager.getMetadataManager().getBucketKey(volName, buckName);
    OmBucketInfo buckInfo = null;
    try {
      buckInfo =
          ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    } catch (IOException e) {
      LOG.debug("Failed to get the value for the key: " + buckKey);
    }
    return buckInfo;
  }

  /**
   * Get bucket layout for the given volume and bucket name.
   *
   * @param volName      volume name
   * @param buckName     bucket name
   * @param ozoneManager ozone manager
   * @param visited      set contains visited bucket details
   * @return bucket layout
   * @throws IOException
   */
  public static BucketLayout getBucketLayout(String volName,
      String buckName, OzoneManager ozoneManager,
      Set<Pair<String, String>> visited) throws IOException {

    OmBucketInfo buckInfo = getOmBucketInfo(ozoneManager, volName, buckName);

    if (buckInfo != null) {
      // If this is a link bucket, we fetch the BucketLayout from the
      // source bucket.
      if (buckInfo.isLink()) {
        // Check if this bucket was already visited - to avoid loops
        if (!visited.add(Pair.of(volName, buckName))) {
          throw new OMException("Detected loop in bucket links. Bucket name: " +
              buckName + ", Volume name: " + volName,
              DETECTED_LOOP_IN_BUCKET_LINKS);
        }
        OmBucketInfo sourceBuckInfo =
            getOmBucketInfo(ozoneManager, buckInfo.getSourceVolume(),
                buckInfo.getSourceBucket());
        if (sourceBuckInfo != null) {
          /** If the source bucket is again a link, we recursively resolve the
           * link bucket.
           *
           * For example:
           * buck-link1 -> buck-link2 -> buck-link3 -> buck-src
           * buck-src has the actual BucketLayout that will be used by the
           * links.
           */
          if (sourceBuckInfo.isLink()) {
            return getBucketLayout(sourceBuckInfo.getVolumeName(),
                sourceBuckInfo.getBucketName(), ozoneManager, visited);
          }
          return sourceBuckInfo.getBucketLayout();
        }
      }
      return buckInfo.getBucketLayout();
    } else {
      LOG.error("Bucket not found: {}/{} ", volName, buckName);
      // TODO: Handle bucket validation
    }
    return BucketLayout.DEFAULT;
  }
}
