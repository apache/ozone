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
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bundles information about a bucket, which is possibly a symlink,
 * and the real bucket that it resolves to, if it is indeed a link.
 * For regular buckets, both {@code requested} and {@code resolved} point to
 * the same bucket.
 */
public class ResolvedBucket {

  private final Pair<String, String> requested;
  private final Pair<String, String> resolved;

  public ResolvedBucket(Pair<String, String> requested,
      Pair<String, String> resolved) {
    this.requested = requested;
    this.resolved = resolved;
  }

  public Pair<String, String> requested() {
    return requested;
  }

  public Pair<String, String> resolved() {
    return resolved;
  }

  public String requestedVolume() {
    return requested.getLeft();
  }

  public String requestedBucket() {
    return requested.getRight();
  }

  public String realVolume() {
    return resolved.getLeft();
  }

  public String realBucket() {
    return resolved.getRight();
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

  public boolean isLink() {
    return !Objects.equals(requested, resolved);
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
