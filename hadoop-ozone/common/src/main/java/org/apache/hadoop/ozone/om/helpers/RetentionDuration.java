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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionTimeUnit;

/**
 * Immutable retention duration in days or calendar years.
 */
public final class RetentionDuration {
  private final RetentionTimeUnit timeUnit;
  private final long duration;

  public RetentionDuration(RetentionTimeUnit timeUnit, long duration) {
    this.timeUnit = Objects.requireNonNull(timeUnit, "retention time unit is null");
    this.duration = duration;
  }

  public RetentionTimeUnit getTimeUnit() {
    return timeUnit;
  }

  public long getDuration() {
    return duration;
  }

  public static RetentionDuration fromProto(OzoneManagerProtocolProtos.RetentionDuration proto) {
    Objects.requireNonNull(proto, "retention duration is null");
    if (!proto.isInitialized()) {
      throw new IllegalArgumentException("Retention duration requires a time unit and duration");
    }
    return new RetentionDuration(proto.getTimeUnit(), proto.getDuration());
  }

  public OzoneManagerProtocolProtos.RetentionDuration toProto() {
    return OzoneManagerProtocolProtos.RetentionDuration.newBuilder()
        .setTimeUnit(timeUnit).setDuration(duration).build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RetentionDuration)) {
      return false;
    }
    RetentionDuration that = (RetentionDuration) obj;
    return timeUnit == that.timeUnit && duration == that.duration;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeUnit, duration);
  }
}
