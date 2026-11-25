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

import java.util.Objects;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Immutable event hold with an optional enabled flag that defaults to true.
 */
public final class EventHold {
  private final boolean enabled;
  private final RetentionDuration duration;

  public EventHold(boolean enabled, RetentionDuration duration) {
    this.enabled = enabled;
    this.duration = Objects.requireNonNull(duration, "event hold duration is null");
  }

  public boolean getEnabled() {
    return enabled;
  }

  public RetentionDuration getDuration() {
    return duration;
  }

  public static EventHold fromProto(OzoneManagerProtocolProtos.EventHold proto) {
    Objects.requireNonNull(proto, "event hold is null");
    return new EventHold(proto.getEnabled(), RetentionDuration.fromProto(proto.getDuration()));
  }

  public OzoneManagerProtocolProtos.EventHold toProto() {
    OzoneManagerProtocolProtos.EventHold.Builder builder = OzoneManagerProtocolProtos.EventHold.newBuilder()
        .setDuration(duration.toProto());
    if (!enabled) {
      builder.setEnabled(false);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof EventHold)) {
      return false;
    }
    EventHold that = (EventHold) obj;
    return enabled == that.enabled && duration.equals(that.duration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, duration);
  }
}
