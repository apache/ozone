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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

/**
 * Bundles information about a bucket, which is possibly a symlink,
 * and the real bucket that it resolves to, if it is indeed a link.
 * For regular buckets, both {@code requested} and {@code resolved} point to
 * the same bucket.
 */
public class ResolvedBucket {

  private final String requestedVolume;
  private final String requestedBucket;
  private final String realVolume;
  private final String realBucket;
  private final String bucketOwner;
  private final BucketLayout bucketLayout;

  public ResolvedBucket(String requestedVolume, String requestedBucket,
      OmBucketInfo resolved) {
    this.requestedVolume = requestedVolume;
    this.requestedBucket = requestedBucket;
    if (resolved != null) {
      this.realVolume = resolved.getVolumeName();
      this.realBucket = resolved.getBucketName();
      this.bucketOwner = resolved.getOwner();
      this.bucketLayout = resolved.getBucketLayout();
    } else {
      this.realVolume = null;
      this.realBucket = null;
      this.bucketOwner = null;
      this.bucketLayout = null;
    }
  }

  public ResolvedBucket(String requestedVolume, String requestedBucket,
      String realVolume, String realBucket, String bucketOwner,
      BucketLayout bucketLayout) {
    this.requestedVolume = requestedVolume;
    this.requestedBucket = requestedBucket;
    this.realVolume = realVolume;
    this.realBucket = realBucket;
    this.bucketOwner = bucketOwner;
    this.bucketLayout = bucketLayout;
  }

  public ResolvedBucket(Pair<String, String> requested,
      Pair<String, String> real, String owner, BucketLayout bucketLayout) {
    this(requested.getLeft(), requested.getRight(),
        real.getLeft(), real.getRight(),
        owner, bucketLayout);
  }

  public String requestedVolume() {
    return requestedVolume;
  }

  public String requestedBucket() {
    return requestedBucket;
  }

  public String realVolume() {
    return realVolume;
  }

  public String realBucket() {
    return realBucket;
  }

  public String bucketOwner() {
    return bucketOwner;
  }

  public BucketLayout bucketLayout() {
    return bucketLayout;
  }

  public OmKeyArgs update(OmKeyArgs args) {
    return isLink()
        ? args.toBuilder()
            .setVolumeName(realVolume())
            .setBucketName(realBucket())
            .build()
        : args;
  }

  public KeyArgs update(KeyArgs args) {
    return isLink()
        ? args.toBuilder()
            .setVolumeName(realVolume())
            .setBucketName(realBucket())
            .build()
        : args;
  }

  public OzoneObj update(OzoneObj ozoneObj) {
    return isLink()
        ? OzoneObjInfo.Builder.fromOzoneObj(ozoneObj)
            .setVolumeName(realVolume())
            .setBucketName(realBucket())
            .build()
        : ozoneObj;
  }

  public boolean isLink() {
    return !Objects.equals(requestedVolume, realVolume)
        || !Objects.equals(requestedBucket, realBucket);
  }

  public boolean isDangling() {
    return realVolume == null || realBucket == null;
  }

  public Map<String, String> audit() {
    return audit(new LinkedHashMap<>());
  }

  /**
   * Adds audit information about the bucket (and if it's a link, then the
   * real bucket, too) to {@code auditMap}.
   * @return the same map for convenience
   */
  public Map<String, String> audit(Map<String, String> auditMap) {
    auditMap.putIfAbsent(OzoneConsts.VOLUME, requestedVolume());
    auditMap.putIfAbsent(OzoneConsts.BUCKET, requestedBucket());
    if (isLink()) {
      auditMap.put(OzoneConsts.SOURCE_VOLUME, realVolume());
      auditMap.put(OzoneConsts.SOURCE_BUCKET, realBucket());
    }
    return auditMap;
  }

}
