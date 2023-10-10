/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hdds.protocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.HashMap;
import java.util.UUID;

/**
 * This class represents the primary identifier for a Datanode.
 * Datanodes are identified by a unique ID.
 */
public final class DatanodeID implements Comparable<DatanodeID> {

  private final UUID uuid;

  private DatanodeID(UUID uuid) {
    this.uuid = uuid;
  }

  // Do not use this, this method is used only to support backward compatibility
  // This method will be removed.
  @Deprecated
  public UUID getUuid() {
    return uuid;
  }

  @Override
  public int compareTo(DatanodeID that) {
    return this.uuid.compareTo(that.uuid);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DatanodeID &&
        uuid.equals(((DatanodeID) obj).uuid);
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public String toString() {
    return uuid.toString();
  }

  public HddsProtos.DatanodeIDProto toProto() {
    HddsProtos.UUID uuidProto = HddsProtos.UUID.newBuilder()
        .setMostSigBits(uuid.getMostSignificantBits())
        .setLeastSigBits(uuid.getLeastSignificantBits())
        .build();
    return HddsProtos.DatanodeIDProto.newBuilder()
        .setUuid(uuidProto).build();
  }

  public static DatanodeID getFromProto(HddsProtos.DatanodeIDProto proto) {
    HddsProtos.UUID uuidProto = proto.getUuid();
    return DatanodeID.of(new UUID(
        uuidProto.getMostSigBits(), uuidProto.getLeastSigBits()));
  }

  private static final HashMap<UUID, DatanodeID> CACHE = new HashMap<>();
  public static DatanodeID of(UUID id) {
    return CACHE.computeIfAbsent(id, DatanodeID::new);
  }

  public static DatanodeID fromUuidString(String id) {
    return of(UUID.fromString(id));
  }
  public static DatanodeID randomID() {
    return new DatanodeID(UUID.randomUUID());
  }
}
