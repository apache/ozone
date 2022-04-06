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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
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
   * BucketLayoutAwareOMKeyRequestFactory#createRequest ->
   * ...
   * OzoneManagerUtils#getBucketLayout ->
   * OzoneManagerUtils#getOmBucketInfo ->
   * omMetadataManager().getBucketTable().get(buckKey)
   */

  private static OmBucketInfo getOmBucketInfo(OzoneManager ozoneManager,
      String volName, String buckName) throws IOException {
    String buckKey =
        ozoneManager.getMetadataManager().getBucketKey(volName, buckName);
    return ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
  }

  /**
   * Get bucket layout for the given volume and bucket name.
   *
   * @param ozoneManager ozone manager
   * @param volName      volume name
   * @param buckName     bucket name
   * @return bucket layout
   * @throws IOException
   */
  public static BucketLayout getBucketLayout(OzoneManager ozoneManager,
                                             String volName,
                                             String buckName)
      throws IOException {
    return getBucketLayout(ozoneManager, volName, buckName, new HashSet<>());
  }

  /**
   * Get bucket layout for the given volume and bucket name.
   *
   * @param volName      volume name
   * @param buckName     bucket name
   * @param ozoneManager ozone manager
   * @return bucket layout
   * @throws IOException
   */
  private static BucketLayout getBucketLayout(OzoneManager ozoneManager,
                                              String volName,
                                              String buckName,
                                              Set<Pair<String, String>> visited)
      throws IOException {

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
            return getBucketLayout(ozoneManager, sourceBuckInfo.getVolumeName(),
                sourceBuckInfo.getBucketName(), visited);
          }
          return sourceBuckInfo.getBucketLayout();
        }
      }
      return buckInfo.getBucketLayout();
    }

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    if (!omMetadataManager.getVolumeTable()
        .isExist(omMetadataManager.getVolumeKey(volName))) {
      throw new OMException("Volume not found: " + volName,
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }

    throw new OMException("Bucket not found: " + volName + "/" + buckName,
        OMException.ResultCodes.BUCKET_NOT_FOUND);
  }

  /**
   * Resolve bucket layout for a given link bucket's OmBucketInfo.
   *
   * @param bucketInfo
   * @return {@code OmBucketInfo} with
   * @throws IOException
   */
  public static OmBucketInfo resolveLinkBucketLayout(OmBucketInfo bucketInfo,
                                                     OMMetadataManager
                                                         metadataManager,
                                                     Set<Pair<String,
                                                         String>> visited)
      throws IOException {

    if (bucketInfo.isLink()) {
      if (!visited.add(Pair.of(bucketInfo.getVolumeName(),
          bucketInfo.getBucketName()))) {
        throw new OMException("Detected loop in bucket links. Bucket name: " +
            bucketInfo.getBucketName() + ", Volume name: " +
            bucketInfo.getVolumeName(),
            DETECTED_LOOP_IN_BUCKET_LINKS);
      }
      String sourceBucketKey = metadataManager
          .getBucketKey(bucketInfo.getSourceVolume(),
              bucketInfo.getSourceBucket());
      OmBucketInfo sourceBucketInfo =
          metadataManager.getBucketTable().get(sourceBucketKey);

      // If the Link Bucket's source bucket exists, we get its layout.
      if (sourceBucketInfo != null) {

        /** If the source bucket is again a link, we recursively resolve the
         * link bucket.
         *
         * For example:
         * buck-link1 -> buck-link2 -> buck-link3 -> buck-src
         * buck-src has the actual BucketLayout that will be used by the links.
         *
         * Finally - we return buck-link1's OmBucketInfo, with buck-src's
         * bucket layout.
         */
        if (sourceBucketInfo.isLink()) {
          sourceBucketInfo =
              resolveLinkBucketLayout(sourceBucketInfo, metadataManager,
                  visited);
        }

        OmBucketInfo.Builder buckInfoBuilder = bucketInfo.toBuilder();
        buckInfoBuilder.setBucketLayout(sourceBucketInfo.getBucketLayout());
        bucketInfo = buckInfoBuilder.build();
      }
    }
    return bucketInfo;
  }

  /**
   * Builds an audit map based on the Delegation Token passed to the method.
   * @param token Delegation Token
   * @return AuditMap
   */
  public static Map<String, String> buildTokenAuditMap(
      Token<OzoneTokenIdentifier> token) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.DELEGATION_TOKEN_KIND,
        token.getKind() == null ? "" : token.getKind().toString());
    auditMap.put(OzoneConsts.DELEGATION_TOKEN_SERVICE,
        token.getService() == null ? "" : token.getService().toString());

    return auditMap;
  }

}
