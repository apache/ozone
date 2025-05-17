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

package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Containers written using schema version 1 wrote unprefixed block ID keys
 * as longs, and metadata or prefixed block IDs as Strings. This was done
 * before codecs were introduced, so callers could serialize their data
 * however they wanted. This codec handles this by checking for string
 * prefixes in the data, and determining which format it should be
 * encoded/decoded to/from.
 */
public final class SchemaOneKeyCodec implements Codec<String> {
  private static final Logger LOG = LoggerFactory.getLogger(
      SchemaOneKeyCodec.class);

  private static final Codec<String> INSTANCE = new SchemaOneKeyCodec();

  public static Codec<String> get() {
    return INSTANCE;
  }

  private SchemaOneKeyCodec() {
    // singleton
  }

  @Override
  public Class<String> getTypeClass() {
    return String.class;
  }

  @Override
  public byte[] toPersistedFormat(String stringObject) throws CodecException {
    try {
      // If the caller's string has no prefix, it should be stored as a long
      // to be encoded as a long to be consistent with the schema one
      // container format.
      long longObject = Long.parseLong(stringObject);
      return LongCodec.get().toPersistedFormat(longObject);
    } catch (NumberFormatException ex) {
      // If long parsing fails, the caller used a prefix and the data should
      // be encoded as a String.
      return StringCodec.get().toPersistedFormat(stringObject);
    }
  }

  /**
   * Determines whether the byte array was originally stored as a string or a
   * long, decodes the data in the corresponding format, and returns it as a
   * String. Data is determined to have been stored as a string if it matches
   * the known format of a metadata key or a prefixed block ID key. If the
   * data does not match one of these formats, it will be parsed as a long
   * only if it is 8 bytes long. Otherwise, it will be parsed as a
   * String.
   * <p>
   * Note that it is technically possible, although highly unlikely, that
   * {@code rawData} was originally encoded as a long, but also happens to match
   * the regex of a known string format when decoded. In this case this method
   * will decode the data as a string. Log trace messages have been added to
   * help debug these errors if necessary.
   *
   * @param rawData Byte array from the key/value store. Should not be null.
   */
  @Override
  public String fromPersistedFormat(byte[] rawData) {
    final String prefixedBlockRegex = "^#[a-zA-Z]+#[0-9]+$";
    final String metadataRegex = "^#[a-zA-Z]$";

    final String stringData = StringCodec.get().fromPersistedFormat(rawData);

    if (stringData.matches(prefixedBlockRegex)
        || stringData.matches(metadataRegex)) {

      LOG.trace("Byte array {} matched the format for a string key." +
          " It will be parsed as the string {}", rawData, stringData);
      return stringData;
    } else if (rawData.length == Long.BYTES) {
      final long longData = LongCodec.get().fromPersistedFormat(rawData);
      LOG.trace("Byte array {} did not match the format for a string key " +
              "and has {} bytes. It will be parsed as the long {}",
          rawData, Long.BYTES, longData);

      return Long.toString(longData);
    } else {
      LOG.trace("Byte array {} did not match the format for a string key " +
          "and does not have {} bytes. It will be parsed as the string {}",
          rawData, Long.BYTES, stringData);

      return stringData;
    }
  }

  @Override
  public String copyObject(String object) {
    return object;
  }
}
