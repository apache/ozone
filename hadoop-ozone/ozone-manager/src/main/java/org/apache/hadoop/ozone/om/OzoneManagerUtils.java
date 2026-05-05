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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * Ozone Manager utility class.
 */
public final class OzoneManagerUtils {

  private OzoneManagerUtils() {
  }

  /**
   * All the client requests are executed through
   * OzoneManagerStateMachine#runCommand function and ensures sequential
   * execution path.
   * Below is the call trace to perform OM client request operation:
   * <pre>
   * {@code
   * OzoneManagerStateMachine#applyTransaction ->
   * OzoneManagerStateMachine#runCommand ->
   * OzoneManagerRequestHandler#handleWriteRequest ->
   * OzoneManagerRatisUtils#createClientRequest ->
   * BucketLayoutAwareOMKeyRequestFactory#createRequest ->
   * ...
   * OzoneManagerUtils#getBucketLayout ->
   * OzoneManagerUtils#getOmBucketInfo ->
   * omMetadataManager().getBucketTable().get(buckKey)
   * }
   * </pre>
   */

  public static OmBucketInfo getBucketInfo(OMMetadataManager metaMgr,
                                           String volName,
                                           String buckName)
      throws IOException {
    String buckKey = metaMgr.getBucketKey(volName, buckName);
    OmBucketInfo bucketInfo = metaMgr.getBucketTable().get(buckKey);
    if (bucketInfo == null) {
      reportNotFound(metaMgr, volName, buckName);
    }
    return bucketInfo;
  }

  private static void reportNotFound(OMMetadataManager metaMgr,
                                     String volName,
                                     String buckName)
      throws IOException {
    if (!metaMgr.getVolumeTable()
        .isExist(metaMgr.getVolumeKey(volName))) {
      throw new OMException("Volume not found: " + volName,
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }

    throw new OMException("Bucket not found: " + volName + "/" + buckName,
        OMException.ResultCodes.BUCKET_NOT_FOUND);
  }

  /**
   * Get bucket layout for the given volume and bucket name.
   *
   * @param metadataManager OMMetadataManager
   * @param volName         volume name
   * @param buckName        bucket name
   * @return bucket layout
   * @throws IOException
   */
  public static BucketLayout getBucketLayout(OMMetadataManager metadataManager,
                                             String volName,
                                             String buckName)
      throws IOException {
    return getResolvedBucketInfo(metadataManager, volName, buckName)
        .getBucketLayout();
  }

  /**
   * Get bucket info for the given volume and bucket name.
   *
   * @param metadataManager metadata manager
   * @param volName         volume name
   * @param buckName        bucket name
   * @return bucket info
   * @throws IOException
   */
  public static OmBucketInfo getResolvedBucketInfo(
      OMMetadataManager metadataManager,
      String volName,
      String buckName)
      throws IOException {
    return resolveBucketInfoLink(metadataManager, volName, buckName,
        new HashSet<>());
  }

  /**
   * Get bucket info for the given volume and bucket name, following all links
   * and returns the last bucket in the chain.
   *
   * @param metadataManager metadata manager
   * @param volName         volume name
   * @param buckName        bucket name
   * @return bucket info. If the bucket is a linked one,
   * returns the info of the last one in the chain.
   * @throws IOException if the bucket does not exist, if it is a link and
   * there is a loop or the link is pointing to a missing bucket.
   */
  private static OmBucketInfo resolveBucketInfoLink(
      OMMetadataManager metadataManager,
      String volName,
      String buckName,
      Set<Pair<String, String>> visited)
      throws IOException {

    OmBucketInfo buckInfo =
        getBucketInfo(metadataManager, volName, buckName);

    // If this is a link bucket, we fetch the BucketLayout from the
    // source bucket.
    if (buckInfo.isLink()) {
      // Check if this bucket was already visited - to avoid loops
      if (!visited.add(Pair.of(volName, buckName))) {
        throw new OMException("Detected loop in bucket links. Bucket name: " +
            buckName + ", Volume name: " + volName,
            DETECTED_LOOP_IN_BUCKET_LINKS);
      }
      /* If the source bucket is again a link, we recursively resolve the
       * link bucket.
       *
       * For example:
       * buck-link1 -> buck-link2 -> buck-link3 -> buck-src
       * buck-src has the actual BucketLayout that will be used by the
       * links.
       */
      return resolveBucketInfoLink(metadataManager, buckInfo.getSourceVolume(),
          buckInfo.getSourceBucket(), visited);
    }
    return buckInfo;
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
