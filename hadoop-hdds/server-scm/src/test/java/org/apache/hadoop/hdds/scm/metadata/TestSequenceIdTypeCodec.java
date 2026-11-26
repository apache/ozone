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

package org.apache.hadoop.hdds.scm.metadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.scm.ha.SequenceIdType;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.junit.jupiter.api.Test;

/**
 * Testing serialization and deserialization of SequenceIdType objects to/from RocksDB.
 */
public class TestSequenceIdTypeCodec {

  private final Codec<SequenceIdType> enumCodec = SequenceIdType.getCodec();
  private final Codec<String> stringCodec = StringCodec.get();

  @Test
  public void testCodecBuffersWithOzoneTestUtil() throws Exception {
    for (SequenceIdType type : SequenceIdType.values()) {
      // Verify codec compatibility with heap and direct byte buffers.
      CodecTestUtil.runTest(enumCodec, type, type.getByteArray().length, null);
    }
  }

  @Test
  public void testSerializedBytesMatchStringCodec() throws Exception {
    for (SequenceIdType type : SequenceIdType.values()) {
      byte[] expectedStringBytes = stringCodec.toPersistedFormat(type.name());
      byte[] computedEnumBytes = enumCodec.toPersistedFormat(type);

      // Verify exact match for on-disk binary format representation.
      assertArrayEquals(expectedStringBytes, computedEnumBytes,
          "Serialized bytes must match the StringCodec exactly");
    }
  }

  @Test
  public void testSequenceIdTypeCodecCanReadStringCodecBytes() throws Exception {
    for (SequenceIdType type : SequenceIdType.values()) {
      byte[] legacyBytes = stringCodec.toPersistedFormat(type.name());

      // Verify deserialization compatibility for cluster upgrade path.
      SequenceIdType decodedEnum = enumCodec.fromPersistedFormat(legacyBytes);
      assertEquals(type, decodedEnum, "SequenceIdTypeCodec failed to read legacy string bytes");
    }
  }

  @Test
  public void testStringCodecCanReadSequenceIdTypeCodecBytes() throws Exception {
    for (SequenceIdType type : SequenceIdType.values()) {
      byte[] newBytes = enumCodec.toPersistedFormat(type);

      // Verify deserialization compatibility for cluster downgrade path.
      String decodedString = stringCodec.fromPersistedFormat(newBytes);
      assertEquals(type.name(), decodedString, "StringCodec failed to read new enum bytes");
    }
  }
}
