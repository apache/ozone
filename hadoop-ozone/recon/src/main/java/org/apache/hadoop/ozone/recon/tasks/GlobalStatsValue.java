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

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.GlobalStatsValueProto;

/**
 * Value class for global statistics stored in RocksDB.
 * Contains only the statistic value for efficient storage in the GLOBAL_STATS column family.
 */
public class GlobalStatsValue {
  private static final Codec<GlobalStatsValue> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(GlobalStatsValueProto.getDefaultInstance()),
      GlobalStatsValue::fromProto,
      GlobalStatsValue::toProto,
      GlobalStatsValue.class);

  private final Long value;

  public GlobalStatsValue(Long value) {
    this.value = value;
  }

  public static Codec<GlobalStatsValue> getCodec() {
    return CODEC;
  }

  public Long getValue() {
    return value;
  }

  public GlobalStatsValueProto toProto() {
    GlobalStatsValueProto.Builder builder = GlobalStatsValueProto.newBuilder();
    if (value != null) {
      builder.setValue(value);
    } else {
      builder.setValue(0L);
    }
    return builder.build();
  }

  public static GlobalStatsValue fromProto(GlobalStatsValueProto proto) {
    return new GlobalStatsValue(proto.getValue());
  }

  @Override
  public String toString() {
    return "GlobalStatsValue{" +
        "value=" + value +
        '}';
  }
}
