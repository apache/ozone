/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.utils.db.Table.KeyValue;

import java.util.Arrays;

/**
 * {@link KeyValue} for a raw type.
 *
 * @param <RAW> The raw type.
 */
public abstract class RawKeyValue<RAW> implements KeyValue<RAW, RAW> {
  /**
   * Create a KeyValue pair.
   *
   * @param key   - Key Bytes
   * @param value - Value bytes
   * @return KeyValue object.
   */
  public static ByteArray create(byte[] key, byte[] value) {
    return new ByteArray(key, value);
  }

  /** Implement {@link RawKeyValue} with byte[]. */
  public static final class ByteArray extends RawKeyValue<byte[]> {
    static byte[] copy(byte[] bytes) {
      return Arrays.copyOf(bytes, bytes.length);
    }

    private ByteArray(byte[] key, byte[] value) {
      super(key, value);
    }

    @Override
    public byte[] getKey() {
      return copy(super.getKey());
    }

    @Override
    public byte[] getValue() {
      return copy(super.getValue());
    }
  }

  private final RAW key;
  private final RAW value;

  private RawKeyValue(RAW key, RAW value) {
    this.key = key;
    this.value = value;
  }

  /**
   * Return key.
   */
  @Override
  public RAW getKey() {
    return key;
  }

  /**
   * Return value.
   */
  @Override
  public RAW getValue() {
    return value;
  }
}
