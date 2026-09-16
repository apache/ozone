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

package org.apache.hadoop.ozone.client.checksum;

import static org.apache.hadoop.ozone.client.checksum.TestCrcUtil.assertContains;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Test {@link CrcComposer}. */
@Timeout(10)
public class TestCrcComposer {
  private static final int DATA_SIZE = 75;
  private static final int CELL_SIZE = 20;
  private static final int CHUNK_SIZE = 10;

  private final Random rand = new Random(1234);

  private final DataChecksum.Type type = DataChecksum.Type.CRC32C;
  private final DataChecksum checksum = DataChecksum.newDataChecksum(type, Integer.MAX_VALUE);
  private final byte[] data = new byte[DATA_SIZE];

  private int fullCrc;
  private int[] crcsByChunk;

  private byte[] crcBytesByChunk;
  private byte[] crcBytesByCell;

  @BeforeEach
  public void setup() {
    rand.nextBytes(data);
    fullCrc = getRangeChecksum(data, 0, DATA_SIZE);

    // 7 chunks of size chunkSize, 1 chunk of size (dataSize % chunkSize).
    crcsByChunk = new int[8];
    for (int i = 0; i < 7; ++i) {
      crcsByChunk[i] = getRangeChecksum(data, i * CHUNK_SIZE, CHUNK_SIZE);
    }
    crcsByChunk[7] = getRangeChecksum(data, (crcsByChunk.length - 1) * CHUNK_SIZE, DATA_SIZE % CHUNK_SIZE);

    // 3 cells of size cellSize, 1 cell of size (dataSize % cellSize).
    int[] crcsByCell = new int[4];
    for (int i = 0; i < 3; ++i) {
      crcsByCell[i] = getRangeChecksum(data, i * CELL_SIZE, CELL_SIZE);
    }
    crcsByCell[3] = getRangeChecksum(data, (crcsByCell.length - 1) * CELL_SIZE, DATA_SIZE % CELL_SIZE);

    crcBytesByChunk = intArrayToByteArray(crcsByChunk);
    crcBytesByCell = intArrayToByteArray(crcsByCell);
  }

  private int getRangeChecksum(byte[] buf, int offset, int length) {
    checksum.reset();
    checksum.update(buf, offset, length);
    return (int) checksum.getValue();
  }

  private byte[] intArrayToByteArray(int[] values) {
    byte[] bytes = new byte[values.length * 4];
    for (int i = 0; i < values.length; ++i) {
      CrcUtil.writeInt(bytes, i * 4, values[i]);
    }
    return bytes;
  }

  @Test
  public void testUnstripedIncorrectChunkSize() {
    CrcComposer digester = CrcComposer.newCrcComposer(type, CHUNK_SIZE);

    // If we incorrectly specify that all CRCs ingested correspond to chunkSize
    // when the last CRC in the array actually corresponds to
    // dataSize % chunkSize then we expect the resulting CRC to not be equal to
    // the fullCrc.
    digester.update(crcBytesByChunk, 0, crcBytesByChunk.length, CHUNK_SIZE);
    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertNotEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUnstripedByteArray() {
    CrcComposer digester = CrcComposer.newCrcComposer(type, CHUNK_SIZE);
    digester.update(crcBytesByChunk, 0, crcBytesByChunk.length - 4, CHUNK_SIZE);
    digester.update(crcBytesByChunk, crcBytesByChunk.length - 4, 4, DATA_SIZE % CHUNK_SIZE);

    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUnstripedDataInputStream() throws IOException {
    CrcComposer digester = CrcComposer.newCrcComposer(type, CHUNK_SIZE);
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(crcBytesByChunk));
    digester.update(input, crcsByChunk.length - 1, CHUNK_SIZE);
    digester.update(input, 1, DATA_SIZE % CHUNK_SIZE);

    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUnstripedSingleCrcs() {
    CrcComposer digester = CrcComposer.newCrcComposer(type, CHUNK_SIZE);
    for (int i = 0; i < crcsByChunk.length - 1; ++i) {
      digester.update(crcsByChunk[i], CHUNK_SIZE);
    }
    digester.update(crcsByChunk[crcsByChunk.length - 1], DATA_SIZE % CHUNK_SIZE);

    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testStripedByteArray() {
    CrcComposer digester = CrcComposer.newStripedCrcComposer(type, CHUNK_SIZE, CELL_SIZE);
    digester.update(crcBytesByChunk, 0, crcBytesByChunk.length - 4, CHUNK_SIZE);
    digester.update(crcBytesByChunk, crcBytesByChunk.length - 4, 4, DATA_SIZE % CHUNK_SIZE);

    byte[] digest = digester.digest();
    assertArrayEquals(crcBytesByCell, digest);
  }

  @Test
  public void testStripedDataInputStream() throws IOException {
    CrcComposer digester = CrcComposer.newStripedCrcComposer(type, CHUNK_SIZE, CELL_SIZE);
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(crcBytesByChunk));
    digester.update(input, crcsByChunk.length - 1, CHUNK_SIZE);
    digester.update(input, 1, DATA_SIZE % CHUNK_SIZE);

    byte[] digest = digester.digest();
    assertArrayEquals(crcBytesByCell, digest);
  }

  @Test
  public void testStripedSingleCrcs() {
    CrcComposer digester = CrcComposer.newStripedCrcComposer(type, CHUNK_SIZE, CELL_SIZE);
    for (int i = 0; i < crcsByChunk.length - 1; ++i) {
      digester.update(crcsByChunk[i], CHUNK_SIZE);
    }
    digester.update(crcsByChunk[crcsByChunk.length - 1], DATA_SIZE % CHUNK_SIZE);

    byte[] digest = digester.digest();
    assertArrayEquals(crcBytesByCell, digest);
  }

  @Test
  public void testMultiStageMixed() throws IOException {
    CrcComposer digester = CrcComposer.newStripedCrcComposer(type, CHUNK_SIZE, CELL_SIZE);

    // First combine chunks into cells.
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(crcBytesByChunk));
    digester.update(input, crcsByChunk.length - 1, CHUNK_SIZE);
    digester.update(input, 1, DATA_SIZE % CHUNK_SIZE);
    byte[] digest = digester.digest();

    // Second, individually combine cells into full crc.
    digester = CrcComposer.newCrcComposer(type, CELL_SIZE);
    for (int i = 0; i < digest.length - 4; i += 4) {
      int cellCrc = CrcUtil.readInt(digest, i);
      digester.update(cellCrc, CELL_SIZE);
    }
    digester.update(digest, digest.length - 4, 4, DATA_SIZE % CELL_SIZE);
    digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUpdateMismatchesStripe() {
    CrcComposer digester = CrcComposer.newStripedCrcComposer(type, CHUNK_SIZE, CELL_SIZE);
    digester.update(crcsByChunk[0], CHUNK_SIZE);

    // Going from chunkSize to chunkSize + cellSize will cross a cellSize
    // boundary in a single CRC, which is not allowed, since we'd lack a
    // CRC corresponding to the actual cellSize boundary.
    final IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> digester.update(crcsByChunk[1], CELL_SIZE));
    assertContains(e, "stripe");
  }

  @Test
  public void testUpdateByteArrayLengthUnalignedWithCrcSize() {
    CrcComposer digester = CrcComposer.newCrcComposer(type, CHUNK_SIZE);

    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> digester.update(crcBytesByChunk, 0, 6, CHUNK_SIZE));
    assertContains(e, "length");
  }
}
