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

    // Simple 8-byte format: just the value
    Long val = value.getValue() != null ? value.getValue() : 0L;
    return Longs.toByteArray(val);
  }

  @Override
  public GlobalStatsValue fromPersistedFormat(byte[] rawData) {
    if (rawData.length == 8) {
      // New format: 8-byte value only
      long value = Longs.fromByteArray(rawData);
      return new GlobalStatsValue(value);
    } else if (rawData.length == 16) {
      // Legacy format: 8-byte value + 8-byte timestamp (ignore timestamp)
      byte[] valueBytes = ArrayUtils.subarray(rawData, 0, Long.BYTES);
      long value = Longs.fromByteArray(valueBytes);
      return new GlobalStatsValue(value);
    } else {
      throw new IllegalArgumentException("GlobalStatsValue data must be 8 or 16 bytes, got: " + rawData.length);
    }
  }

  @Override
  public GlobalStatsValue copyObject(GlobalStatsValue object) {
    return new GlobalStatsValue(object.getValue());
  }
}
