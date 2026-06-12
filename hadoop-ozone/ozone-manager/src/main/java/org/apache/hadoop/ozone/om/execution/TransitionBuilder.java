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

package org.apache.hadoop.ozone.om.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBDelta;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeltaType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReplicatedStateTransition;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Request-scoped builder for constructing a {@link ReplicatedStateTransition}.
 * Provides a type-safe API for declaring DB mutations using existing codecs.
 */
public final class TransitionBuilder {

  private final long managedIndex;
  private final List<DBDelta> deltas = new ArrayList<>();
  private OMResponse response;

  public TransitionBuilder(long managedIndex) {
    this.managedIndex = managedIndex;
  }

  public TransitionBuilder() {
    this(0);
  }

  public long getManagedIndex() {
    return managedIndex;
  }

  public <V> void put(String tableName, String key, V value,
      Codec<V> codec) throws IOException {
    byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] valueBytes = codec.toPersistedFormat(value);
    deltas.add(DBDelta.newBuilder()
        .setTableName(tableName)
        .setKey(ByteString.copyFrom(keyBytes))
        .setValue(ByteString.copyFrom(valueBytes))
        .setType(DeltaType.PUT)
        .build());
  }

  public void delete(String tableName, String key) {
    byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    deltas.add(DBDelta.newBuilder()
        .setTableName(tableName)
        .setKey(ByteString.copyFrom(keyBytes))
        .setType(DeltaType.DELETE)
        .build());
  }

  public void setResponse(OMResponse omResponse) {
    this.response = omResponse;
  }

  public ReplicatedStateTransition build(long index, Type cmdType) {
    ReplicatedStateTransition.Builder builder = ReplicatedStateTransition.newBuilder()
        .setManagedIndex(index)
        .setCmdType(cmdType);
    builder.addAllDeltas(deltas);
    if (response != null) {
      builder.setResponse(response);
    }
    return builder.build();
  }
}
