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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketVersioningStatusProto;

/**
 * S3-compatible bucket versioning state machine:
 * UNVERSIONED -&gt; ENABLED &lt;-&gt; SUSPENDED.
 * Once versioning has been enabled, a bucket can never return to UNVERSIONED.
 */
public enum BucketVersioningStatus {
  UNVERSIONED,
  ENABLED,
  SUSPENDED;

  public static BucketVersioningStatus fromProto(BucketVersioningStatusProto proto) {
    switch (proto) {
    case VERSIONING_ENABLED:
      return ENABLED;
    case VERSIONING_SUSPENDED:
      return SUSPENDED;
    case UNVERSIONED:
    default:
      return UNVERSIONED;
    }
  }

  public BucketVersioningStatusProto toProto() {
    switch (this) {
    case ENABLED:
      return BucketVersioningStatusProto.VERSIONING_ENABLED;
    case SUSPENDED:
      return BucketVersioningStatusProto.VERSIONING_SUSPENDED;
    default:
      return BucketVersioningStatusProto.UNVERSIONED;
    }
  }

  /** Maps the legacy isVersionEnabled flag of buckets without an explicit status. */
  public static BucketVersioningStatus fromVersionEnabledFlag(boolean isVersionEnabled) {
    return isVersionEnabled ? ENABLED : UNVERSIONED;
  }

  /** The legacy isVersionEnabled flag value kept in sync with this status. */
  public boolean toVersionEnabledFlag() {
    return this == ENABLED;
  }

  /**
   * Whether a bucket may transition from this status to {@code target}.
   * The only forbidden transition is back to UNVERSIONED after versioning
   * has been enabled or suspended (matches the S3 state machine).
   */
  public boolean canTransitionTo(BucketVersioningStatus target) {
    return this == UNVERSIONED || target != UNVERSIONED;
  }
}
