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

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * The replication factor to be used while writing key into ozone.
 */
public enum ReplicationFactor {
  ONE,
  THREE;

  /**
   * Returns enum value corresponding to the int value.
   * @param value replication value
   * @return ReplicationFactor
   */
  public static ReplicationFactor valueOf(int value) {
    if (value == 1) {
      return ONE;
    }
    if (value == 3) {
      return THREE;
    }
    throw new IllegalArgumentException("Unsupported value: " + value);
  }

  public static ReplicationFactor fromProto(
      HddsProtos.ReplicationFactor replicationFactor) {
    if (replicationFactor == null) {
      return null;
    }
    switch (replicationFactor) {
    case ONE:
      return ReplicationFactor.ONE;
    case THREE:
      return ReplicationFactor.THREE;
    default:
      throw new IllegalArgumentException(
          "Unsupported ProtoBuf replication factor: " + replicationFactor);
    }
  }

  public static HddsProtos.ReplicationFactor toProto(
       ReplicationFactor replicationFactor) {
    if (replicationFactor == null) {
      return null;
    }
    return replicationFactor.toProto();
  }

  public HddsProtos.ReplicationFactor toProto() {
    switch (this) {
    case ONE:
      return HddsProtos.ReplicationFactor.ONE;
    case THREE:
      return HddsProtos.ReplicationFactor.THREE;
    default:
      throw new IllegalStateException("Unexpected enum value: " + this);
    }
  }

  /** @return the number of replication(s). */
  public int getValue() {
    switch (this) {
    case ONE:
      return 1;
    case THREE:
      return 3;
    default:
      throw new IllegalStateException("Unexpected enum value: " + this);
    }
  }
}
