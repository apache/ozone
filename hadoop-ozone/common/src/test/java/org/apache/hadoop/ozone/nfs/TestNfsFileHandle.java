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

package org.apache.hadoop.ozone.nfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.ozone.nfs.NfsFileHandle.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the backend handle format, not object resolution or NFS authorization. */
class TestNfsFileHandle {
  private static final UUID EXPORT_ID = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

  @Test
  void goldenFileHandles() throws IOException {
    int vectors = 0;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        getClass().getResourceAsStream("/nfs/file-handles.txt"), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        String[] fields = line.split(" ");
        NfsFileHandle handle = new NfsFileHandle(UUID.fromString(fields[0]), Long.parseUnsignedLong(fields[1], 16),
            Long.parseUnsignedLong(fields[2], 16), Long.parseUnsignedLong(fields[3], 16), Kind.valueOf(fields[4]));
        String hex = fields[5];
        byte[] expected = new byte[hex.length() / 2];
        for (int i = 0; i < expected.length; i++) {
          expected[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        assertArrayEquals(expected, handle.toBytes());
        assertEquals(handle, NfsFileHandle.fromBytes(expected));
        vectors++;
      }
    }
    assertEquals(4, vectors, "Shared Java/C handle vectors must be exercised");
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void preservesIdentityAndEpochBits(Kind kind) {
    NfsFileHandle handle = new NfsFileHandle(EXPORT_ID, Long.MIN_VALUE, Long.MAX_VALUE, -1, kind);
    NfsFileHandle decoded = NfsFileHandle.fromBytes(handle.toBytes());
    assertEquals(handle, decoded);
    assertEquals(handle.hashCode(), decoded.hashCode());
    assertEquals(EXPORT_ID, decoded.getExportId());
    assertEquals(Long.MIN_VALUE, decoded.getVolumeId());
    assertEquals(Long.MAX_VALUE, decoded.getBucketId());
    assertEquals(-1, decoded.getObjectId());
    assertEquals(kind, decoded.getKind());
    assertEquals(NfsFileHandle.ENCODED_LENGTH, handle.toBytes().length);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 47, 49, 128})
  void rejectsWrongLength(int length) {
    byte[] payload = Arrays.copyOf(new NfsFileHandle(EXPORT_ID, 1, 2, 3, Kind.FILE).toBytes(), length);
    assertThrows(IllegalArgumentException.class, () -> NfsFileHandle.fromBytes(payload));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 4, 5, 6, 7})
  void rejectsMalformedHeader(int offset) {
    byte[] payload = new NfsFileHandle(EXPORT_ID, 1, 2, 3, Kind.FILE).toBytes();
    payload[offset] = (byte) 0xff;
    assertThrows(IllegalArgumentException.class, () -> NfsFileHandle.fromBytes(payload));
  }

  @Test
  void rejectsNullPayload() {
    assertThrows(IllegalArgumentException.class, () -> NfsFileHandle.fromBytes(null));
  }

  @Test
  void isolatesExportsAndObjectIncarnations() {
    NfsFileHandle handle = new NfsFileHandle(EXPORT_ID, 1, 2, 3, Kind.FILE);
    assertNotEquals(handle, new NfsFileHandle(new UUID(0, 1), 1, 2, 3, Kind.FILE));
    assertNotEquals(handle, new NfsFileHandle(EXPORT_ID, 4, 2, 3, Kind.FILE));
    assertNotEquals(handle, new NfsFileHandle(EXPORT_ID, 1, 4, 3, Kind.FILE));
    assertNotEquals(handle, new NfsFileHandle(EXPORT_ID, 1, 2, 4, Kind.FILE));
    assertNotEquals(handle, new NfsFileHandle(EXPORT_ID, 1, 2, 3, Kind.DIRECTORY));
  }

  @Test
  void doesNotRetainMutablePayloads() {
    NfsFileHandle handle = new NfsFileHandle(EXPORT_ID, 1, 2, 3, Kind.FILE);
    byte[] payload = handle.toBytes();
    NfsFileHandle decoded = NfsFileHandle.fromBytes(payload);
    Arrays.fill(payload, (byte) 0);
    assertEquals(handle, decoded);
    assertArrayEquals(handle.toBytes(), decoded.toBytes());
  }
}
