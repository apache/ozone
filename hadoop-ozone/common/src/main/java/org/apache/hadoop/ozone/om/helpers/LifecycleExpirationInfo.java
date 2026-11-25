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
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * When a key expires under the bucket lifecycle configuration, and which rule expires it.
 */
@Immutable
public final class LifecycleExpirationInfo {
  private final Instant expiryTime;
  private final String ruleId;

  public LifecycleExpirationInfo(Instant expiryTime, String ruleId) {
    this.expiryTime = Objects.requireNonNull(expiryTime, "expiryTime == null");
    this.ruleId = Objects.requireNonNull(ruleId, "ruleId == null");
  }

  public Instant getExpiryTime() {
    return expiryTime;
  }

  public String getRuleId() {
    return ruleId;
  }

  public OzoneManagerProtocolProtos.LifecycleExpirationInfo getProtobuf() {
    return OzoneManagerProtocolProtos.LifecycleExpirationInfo.newBuilder()
        .setExpiryTime(expiryTime.toEpochMilli())
        .setRuleId(ruleId)
        .build();
  }

  public static LifecycleExpirationInfo getFromProtobuf(OzoneManagerProtocolProtos.LifecycleExpirationInfo proto) {
    return new LifecycleExpirationInfo(Instant.ofEpochMilli(proto.getExpiryTime()), proto.getRuleId());
  }

  @Override
  public String toString() {
    return "LifecycleExpirationInfo{expiryTime=" + expiryTime + ", ruleId='" + ruleId + "'}";
  }
}
