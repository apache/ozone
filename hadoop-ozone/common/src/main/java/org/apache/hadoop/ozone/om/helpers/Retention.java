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

import com.google.protobuf.TextFormat;
import java.util.Objects;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRetention;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionDuration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionMode;

/**
 * Immutable default retention configuration for a bucket.
 */
public final class Retention {
  private final RetentionMode retentionMode;
  private final RetentionDuration duration;
  private final RetentionDuration defaultEventHold;

  public Retention(RetentionMode retentionMode, RetentionDuration duration, RetentionDuration defaultEventHold) {
    this.retentionMode = Objects.requireNonNull(retentionMode, "retention mode is null");
    this.duration = duration;
    if (defaultEventHold != null) {
      RetentionUtil.validateEventHoldRule(defaultEventHold);
    }
    this.defaultEventHold = defaultEventHold;
  }

  public RetentionMode getRetentionMode() {
    return retentionMode;
  }

  public RetentionDuration getDuration() {
    return duration;
  }

  public RetentionDuration getDefaultEventHold() {
    return defaultEventHold;
  }

  public static Retention fromProto(BucketRetention proto) {
    Objects.requireNonNull(proto, "retention config is null");
    return new Retention(proto.getRetentionMode(), proto.hasDuration() ? proto.getDuration() : null,
        proto.hasDefaultEventHold() ? proto.getDefaultEventHold() : null);
  }

  public BucketRetention toProto() {
    BucketRetention.Builder builder = BucketRetention.newBuilder().setRetentionMode(retentionMode);
    if (duration != null) {
      builder.setDuration(duration);
    }
    if (defaultEventHold != null) {
      builder.setDefaultEventHold(defaultEventHold);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Retention)) {
      return false;
    }
    Retention that = (Retention) obj;
    return retentionMode == that.retentionMode && Objects.equals(duration, that.duration) &&
        Objects.equals(defaultEventHold, that.defaultEventHold);
  }

  @Override
  public String toString() {
    return "Retention{" + TextFormat.shortDebugString(toProto()) + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(retentionMode, duration, defaultEventHold);
  }
}
