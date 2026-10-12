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

import java.time.Instant;
import java.util.Objects;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EventHold;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Retention;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionMode;

/**
 * Immutable retention state applied to a key, with an optional expiration time in epoch milliseconds.
 */
public final class ObjectRetention {
  private final RetentionMode retentionMode;
  private final Long retentionDate;
  private final EventHold eventHold;

  public ObjectRetention(RetentionMode retentionMode, Long retentionDate, EventHold eventHold) {
    this.retentionMode = Objects.requireNonNull(retentionMode, "retention mode is null");
    this.retentionDate = retentionDate;
    this.eventHold = eventHold;
  }

  public RetentionMode getRetentionMode() {
    return retentionMode;
  }

  public Long getRetentionDate() {
    return retentionDate;
  }

  public EventHold getEventHold() {
    return eventHold;
  }

  public static ObjectRetention fromProto(Retention proto) {
    Objects.requireNonNull(proto, "retention is null");
    return new ObjectRetention(proto.getRetentionMode(),
        proto.hasRetentionDate() ? Instant.parse(proto.getRetentionDate()).toEpochMilli() : null,
        proto.hasEventHold() ? proto.getEventHold() : null);
  }

  public Retention toProto() {
    Retention.Builder builder = Retention.newBuilder().setRetentionMode(retentionMode);
    if (retentionDate != null) {
      builder.setRetentionDate(Instant.ofEpochMilli(retentionDate).toString());
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
    if (!(obj instanceof ObjectRetention)) {
      return false;
    }
    ObjectRetention that = (ObjectRetention) obj;
    return retentionMode == that.retentionMode && Objects.equals(retentionDate, that.retentionDate) &&
        Objects.equals(eventHold, that.eventHold);
  }

  @Override
  public int hashCode() {
    return Objects.hash(retentionMode, retentionDate, eventHold);
  }
}
