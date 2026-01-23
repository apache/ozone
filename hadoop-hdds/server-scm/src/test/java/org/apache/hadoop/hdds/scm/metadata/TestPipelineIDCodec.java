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

import java.util.UUID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.junit.jupiter.api.Test;

/**
 * Testing serialization of PipelineID objects to/from RocksDB.
 */
public class TestPipelineIDCodec {
  private final OldPipelineIDCodecForTesting oldCodec
      = new OldPipelineIDCodecForTesting();
  private final Codec<PipelineID> newCodec = PipelineID.getCodec();

  @Test
  public void testPersistingZeroAsUUID() throws Exception {
    long leastSigBits = 0x0000_0000_0000_0000L;
    long mostSigBits = 0x0000_0000_0000_0000L;
    byte[] expected = new byte[] {
        b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00),
        b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00)
    };

    checkPersisting(leastSigBits, mostSigBits, expected);
  }

  @Test
  public void testPersistingFFAsUUID() throws Exception {
    long leastSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    long mostSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    byte[] expected = new byte[] {
        b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF),
        b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF)
    };

    checkPersisting(leastSigBits, mostSigBits, expected);
  }

  @Test
  public void testPersistingARandomUUID() throws Exception {
    for (int i = 0; i < 100; i++) {
      UUID uuid = UUID.randomUUID();

      long mask = 0x0000_0000_0000_00FFL;

      byte[] expected = new byte[] {
          b(((int) (uuid.getMostSignificantBits() >> 56 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 48 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 40 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 32 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 24 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 16 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 8 & mask))),
          b(((int) (uuid.getMostSignificantBits() & mask))),

          b(((int) (uuid.getLeastSignificantBits() >> 56 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 48 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 40 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 32 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 24 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 16 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 8 & mask))),
          b(((int) (uuid.getLeastSignificantBits() & mask))),
      };

      checkPersisting(
          uuid.getMostSignificantBits(),
          uuid.getLeastSignificantBits(),
          expected
      );
    }
  }

  @Test
  public void testConvertAndReadBackZeroAsUUID() throws Exception {
    long mostSigBits = 0x0000_0000_0000_0000L;
    long leastSigBits = 0x0000_0000_0000_0000L;
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    assertUuid(uuid);
  }

  @Test
  public void testConvertAndReadBackFFAsUUID() throws Exception {
    long mostSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    long leastSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    assertUuid(uuid);
  }

  @Test
  public void testConvertAndReadBackRandomUUID() throws Exception {
    UUID uuid = UUID.randomUUID();
    assertUuid(uuid);
  }

  void assertUuid(UUID uuid) throws Exception {
    PipelineID pid = PipelineID.valueOf(uuid);


    final byte[] expected = oldCodec.toPersistedFormat(pid);
    final byte[] computed = newCodec.toPersistedFormat(pid);
    assertArrayEquals(expected, computed);

    assertEquals(pid, oldCodec.fromPersistedFormat(expected));
    assertEquals(pid, newCodec.fromPersistedFormat(expected));

    CodecTestUtil.runTest(newCodec, pid, 16, oldCodec);
  }

  private void checkPersisting(
      long mostSigBits, long leastSigBits, byte[] expected
  ) throws Exception {
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    PipelineID pid = PipelineID.valueOf(uuid);

    final byte[] encoded = oldCodec.toPersistedFormat(pid);
    assertArrayEquals(expected, encoded);

    final byte[] computed = newCodec.toPersistedFormat(pid);
    assertArrayEquals(expected, computed);
  }

  private byte b(int i) {
    return (byte) (i & 0x0000_00FF);
  }
}
