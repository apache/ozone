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

package org.apache.hadoop.ozone.recon.spi.impl;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;

/**
 * Codec to serialize/deserialize {@link GlobalStatsValue}.
 */
public final class GlobalStatsValueCodec implements Codec<GlobalStatsValue> {

  private static final Codec<GlobalStatsValue> INSTANCE = new GlobalStatsValueCodec();

  public static Codec<GlobalStatsValue> get() {
    return INSTANCE;
  }

  private GlobalStatsValueCodec() {
    // singleton
  }

  @Override
  public Class<GlobalStatsValue> getTypeClass() {
    return GlobalStatsValue.class;
  }

  @Override
  public byte[] toPersistedFormat(GlobalStatsValue value) {
    Preconditions.checkNotNull(value, "Null object can't be converted to byte array.");
    
    // Fixed 16-byte format: 8-byte value + 8-byte timestamp
    Long val = value.getValue() != null ? value.getValue() : 0L;
    Long timestamp = value.getLastUpdatedTimestamp() != null ? 
        value.getLastUpdatedTimestamp() : 0L;
    
    byte[] valueBytes = Longs.toByteArray(val);
    byte[] timestampBytes = Longs.toByteArray(timestamp);
    
    return ArrayUtils.addAll(valueBytes, timestampBytes);
  }

  @Override
  public GlobalStatsValue fromPersistedFormat(byte[] rawData) {
    if (rawData.length != 16) {
      throw new IllegalArgumentException("GlobalStatsValue data must be exactly 16 bytes, got: " + rawData.length);
    }
    
    // First 8 bytes is the value
    byte[] valueBytes = ArrayUtils.subarray(rawData, 0, Long.BYTES);
    long value = Longs.fromByteArray(valueBytes);
    
    // Last 8 bytes is the timestamp
    byte[] timestampBytes = ArrayUtils.subarray(rawData, Long.BYTES, 16);
    long timestamp = Longs.fromByteArray(timestampBytes);
    
    return new GlobalStatsValue(value, timestamp);
  }

  @Override
  public GlobalStatsValue copyObject(GlobalStatsValue object) {
    return new GlobalStatsValue(
        object.getValue(),
        object.getLastUpdatedTimestamp()
    );
  }
}
