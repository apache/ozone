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

package org.apache.hadoop.ozone.om.helpers;

import jakarta.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NoncurrentVersionExpiration;

/**
 * Reclaims the noncurrent versions of a key on a bucket with S3-compatible
 * versioning, by age, by count, or by both.
 *
 * <p>The current version is never touched: it is what a plain read returns, and
 * expiring it is the {@link OmLCExpiration} action's job. Only versions the
 * versionedKeyTable holds are candidates.
 */
public final class OmLCNoncurrentVersionExpiration implements OmLCAction {

  /** S3's ceiling on NewerNoncurrentVersions. */
  public static final int MAX_NEWER_NONCURRENT_VERSIONS = 100;

  private final Integer noncurrentDays;
  private final Integer newerNoncurrentVersions;
  /**
   * Derived from noncurrentDays alone, so it is settled here rather than in
   * {@link #valid()}: an instance whose validation has not run yet would
   * otherwise read as zero, and every version would be past a rule it is not
   * past.
   */
  private final long noncurrentDaysInMillis;

  private OmLCNoncurrentVersionExpiration(Builder builder) {
    this.noncurrentDays = builder.noncurrentDays;
    this.newerNoncurrentVersions = builder.newerNoncurrentVersions;
    this.noncurrentDaysInMillis = noncurrentDays == null ? 0L
        : TimeUnit.DAYS.toMillis(noncurrentDays);
  }

  @Nullable
  public Integer getNoncurrentDays() {
    return noncurrentDays;
  }

  @Nullable
  public Integer getNewerNoncurrentVersions() {
    return newerNoncurrentVersions;
  }

  /**
   * @param becameNoncurrentAt when the version stopped being current
   * @return whether it has been noncurrent longer than the rule allows. Always
   *     false when the rule only limits how many versions are kept.
   */
  public boolean isExpired(long becameNoncurrentAt) {
    return noncurrentDays != null
        && System.currentTimeMillis() > becameNoncurrentAt + noncurrentDaysInMillis;
  }

  @Override
  public ActionType getActionType() {
    return ActionType.NONCURRENT_VERSION_EXPIRATION;
  }

  @Override
  public void valid(long creationTime) throws OMException {
    if (noncurrentDays == null && newerNoncurrentVersions == null) {
      throw new OMException("Invalid lifecycle configuration: "
          + "NoncurrentVersionExpiration must specify 'NoncurrentDays', "
          + "'NewerNoncurrentVersions', or both.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (noncurrentDays != null) {
      if (noncurrentDays <= 0) {
        throw new OMException("'NoncurrentDays' must be a positive integer "
            + "greater than zero.", OMException.ResultCodes.INVALID_REQUEST);
      }
    }

    if (newerNoncurrentVersions != null
        && (newerNoncurrentVersions <= 0
            || newerNoncurrentVersions > MAX_NEWER_NONCURRENT_VERSIONS)) {
      throw new OMException("'NewerNoncurrentVersions' must be between 1 and "
          + MAX_NEWER_NONCURRENT_VERSIONS + ".",
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  @Override
  public LifecycleAction getProtobuf() {
    NoncurrentVersionExpiration.Builder builder =
        NoncurrentVersionExpiration.newBuilder();
    if (noncurrentDays != null) {
      builder.setNoncurrentDays(noncurrentDays);
    }
    if (newerNoncurrentVersions != null) {
      builder.setNewerNoncurrentVersions(newerNoncurrentVersions);
    }
    return LifecycleAction.newBuilder()
        .setNoncurrentVersionExpiration(builder).build();
  }

  public static OmLCNoncurrentVersionExpiration getFromProtobuf(
      NoncurrentVersionExpiration proto) {
    Builder builder = new Builder();
    if (proto.hasNoncurrentDays()) {
      builder.setNoncurrentDays(proto.getNoncurrentDays());
    }
    if (proto.hasNewerNoncurrentVersions()) {
      builder.setNewerNoncurrentVersions(proto.getNewerNoncurrentVersions());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return "OmLCNoncurrentVersionExpiration{"
        + "noncurrentDays=" + noncurrentDays
        + ", newerNoncurrentVersions=" + newerNoncurrentVersions
        + '}';
  }

  /**
   * Builder of OmLCNoncurrentVersionExpiration.
   */
  public static class Builder {
    private Integer noncurrentDays = null;
    private Integer newerNoncurrentVersions = null;

    public Builder setNoncurrentDays(int days) {
      this.noncurrentDays = days;
      return this;
    }

    public Builder setNewerNoncurrentVersions(int versions) {
      this.newerNoncurrentVersions = versions;
      return this;
    }

    public OmLCNoncurrentVersionExpiration build() {
      return new OmLCNoncurrentVersionExpiration(this);
    }
  }
}
