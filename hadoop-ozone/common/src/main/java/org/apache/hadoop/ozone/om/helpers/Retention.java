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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EventHold;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionMode;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Rule;

/**
 * Immutable default retention configuration for a bucket.
 */
public final class Retention {
  private final RetentionMode retentionMode;
  private final Rule rule;
  private final EventHold eventHold;

  public Retention(RetentionMode retentionMode, Rule rule, EventHold eventHold) {
    this.retentionMode = Objects.requireNonNull(retentionMode, "retention mode is null");
    this.rule = rule;
    if (eventHold != null) {
      RetentionUtil.validateEventHoldRule(eventHold.getRule());
    }
    this.eventHold = eventHold;
  }

  public RetentionMode getRetentionMode() {
    return retentionMode;
  }

  public Rule getRule() {
    return rule;
  }

  public EventHold getEventHold() {
    return eventHold;
  }

  public static Retention fromProto(BucketRetention proto) {
    Objects.requireNonNull(proto, "retention config is null");
    return new Retention(proto.getRetentionMode(), proto.hasRule() ? proto.getRule() : null,
        proto.hasEventHold() ? proto.getEventHold() : null);
  }

  public BucketRetention toProto() {
    BucketRetention.Builder builder = BucketRetention.newBuilder().setRetentionMode(retentionMode);
    if (rule != null) {
      builder.setRule(rule);
    }
    if (eventHold != null) {
      builder.setEventHold(eventHold);
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
    return retentionMode == that.retentionMode && Objects.equals(rule, that.rule) &&
        Objects.equals(eventHold, that.eventHold);
  }

  @Override
  public String toString() {
    return "Retention{" + TextFormat.shortDebugString(toProto()) + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(retentionMode, rule, eventHold);
  }
}
