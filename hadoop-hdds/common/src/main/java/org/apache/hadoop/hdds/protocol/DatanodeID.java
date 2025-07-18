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

package org.apache.hadoop.hdds.protocol;

import com.google.protobuf.ByteString;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeIDProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.util.StringWithByteString;

/**
 * DatanodeID is the primary identifier of the Datanode.
 * They are unique for every Datanode in the cluster.
 * <p>
 * This class is immutable and thread safe.
 */
public final class DatanodeID implements Comparable<DatanodeID> {

  private static final ConcurrentMap<UUID, DatanodeID> CACHE = new ConcurrentHashMap<>();

  private final UUID uuid;
  private final StringWithByteString uuidByteString;

  private DatanodeID(final UUID uuid) {
    this.uuid = uuid;
    this.uuidByteString = StringWithByteString.valueOf(uuid.toString());
  }

  // Mainly used for JSON conversion
  public String getID() {
    return toString();
  }

  @Override
  public int compareTo(final DatanodeID that) {
    return this.uuid.compareTo(that.uuid);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof DatanodeID &&
        uuid.equals(((DatanodeID) obj).uuid);
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public String toString() {
    return uuidByteString.getString();
  }

  /**
   * This will be removed once the proto structure is refactored 
   * to remove deprecated fields.
   */
  @Deprecated
  public ByteString getByteString() {
    return uuidByteString.getBytes();
  }

  public PipelineID toPipelineID() {
    return PipelineID.valueOf(uuid);
  }

  public DatanodeIDProto toProto() {
    return DatanodeIDProto.newBuilder().setUuid(toProto(uuid)).build();
  }

  public static DatanodeID fromProto(final DatanodeIDProto proto) {
    return of(fromProto(proto.getUuid()));
  }

  public static DatanodeID fromUuidString(final String id) {
    return of(UUID.fromString(id));
  }

  public static DatanodeID of(final UUID id) {
    return CACHE.computeIfAbsent(id, DatanodeID::new);
  }

  public static DatanodeID of(final HddsProtos.UUID uuid) {
    return of(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
  }

  /**
   * Returns a random DatanodeID.
   */
  public static DatanodeID randomID() {
    // We don't want to add Random ID to cache.
    return new DatanodeID(UUID.randomUUID());
  }

  private static UUID fromProto(final HddsProtos.UUID id) {
    return new UUID(id.getMostSigBits(), id.getLeastSigBits());
  }

  private static HddsProtos.UUID toProto(final UUID id) {
    return HddsProtos.UUID.newBuilder()
        .setMostSigBits(id.getMostSignificantBits())
        .setLeastSigBits(id.getLeastSignificantBits())
        .build();
  }

  // TODO: Remove this in follow-up Jira. (HDDS-12015)
  //   Exposing this temporarily to help with refactoring.
  @Deprecated
  public UUID getUuid() {
    return uuid;
  }
}
