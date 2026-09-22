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

package org.apache.hadoop.ozone.common;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link Checksum} class.
 */
public class TestChecksum {

  private static final int BYTES_PER_CHECKSUM = 10;
  private static final ContainerProtos.ChecksumType CHECKSUM_TYPE_DEFAULT =
      ContainerProtos.ChecksumType.SHA256;

  private Checksum getChecksum(ContainerProtos.ChecksumType type, boolean allowChecksumCache) {
    if (type == null) {
      type = CHECKSUM_TYPE_DEFAULT;
    }
    return new Checksum(type, BYTES_PER_CHECKSUM, allowChecksumCache);
  }

  /**
   * Tests {@link Checksum#verifyChecksum(ByteBuffer, ChecksumData, int)}.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testVerifyChecksum(boolean useChecksumCache) throws Exception {
    Checksum checksum = getChecksum(null, useChecksumCache);
    int dataLen = 55;
    byte[] data = RandomStringUtils.secure().nextAlphabetic(dataLen).getBytes(UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);

    ChecksumData checksumData = checksum.computeChecksum(byteBuffer, useChecksumCache);

    // A checksum is calculate for each bytesPerChecksum number of bytes in
    // the data. Since that value is 10 here and the data length is 55, we
    // should have 6 checksums in checksumData.
    assertEquals(6, checksumData.getChecksums().size());

    // Checksum verification should pass
    Checksum.verifyChecksum(ByteBuffer.wrap(data), checksumData, 0);
  }

  /**
   * Tests that if data is modified, then the checksums should not match.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testIncorrectChecksum(boolean useChecksumCache) throws Exception {
    Checksum checksum = getChecksum(null, useChecksumCache);
    byte[] data = RandomStringUtils.secure().nextAlphabetic(55).getBytes(UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    ChecksumData originalChecksumData = checksum.computeChecksum(byteBuffer, useChecksumCache);

    // Change the data and check if new checksum matches the original checksum.
    // Modifying one byte of data should be enough for the checksum data to
    // mismatch
    data[50] = (byte) (data[50] + 1);
    ChecksumData newChecksumData = checksum.computeChecksum(data);
    assertNotEquals(originalChecksumData, newChecksumData, "Checksums should not match for different data");
  }

  /**
   * Tests that checksum calculated using two different checksumTypes should
   * not match.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testChecksumMismatchForDifferentChecksumTypes(boolean useChecksumCache) {
    // Checksum1 of type SHA-256
    Checksum checksum1 = getChecksum(null, useChecksumCache);

    // Checksum2 of type CRC32
    Checksum checksum2 = getChecksum(ContainerProtos.ChecksumType.CRC32, useChecksumCache);

    // The two checksums should not match as they have different types
    assertNotEquals(checksum1, checksum2, "Checksums should not match for different checksum types");
  }

  @Test
  public void testChecksumFromNonzeroPosition() throws Exception {
    final ChunkBuffer data = mock(ChunkBuffer.class);
    when(data.position()).thenReturn(Integer.MAX_VALUE - 1);
    when(data.limit()).thenReturn(Integer.MAX_VALUE);
    when(data.remaining()).thenReturn(1);
    when(data.asByteBufferList()).thenReturn(
        Collections.singletonList(ByteBuffer.wrap(new byte[] {1})));

    final Checksum checksum = getChecksum(null, false);
    assertEquals(1, checksum.computeChecksum(data).getChecksums().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRejectsIncrementalBufferInWriteMode(boolean useChecksumCache) {
    try (ChunkBuffer data = ChunkBuffer.allocate(32, 8)) {
      data.put(new byte[10]);

      final Checksum checksum = getChecksum(null, useChecksumCache);
      final IllegalStateException exception = assertThrows(
          IllegalStateException.class,
          () -> checksum.computeChecksum(data, useChecksumCache));
      assertEquals("ChunkBuffer remaining byte count is 22, but its underlying buffers expose 6 bytes",
          exception.getMessage());
    }
  }

  private static ContainerProtos.ChunkInfo chunk(byte[] data, int offset, int length, int interval) throws Exception {
    return ContainerProtos.ChunkInfo.newBuilder().setChunkName("chunk-" + offset).setOffset(offset).setLen(length)
        .setChecksumData(new Checksum(ContainerProtos.ChecksumType.CRC32, interval)
            .computeChecksum(ByteBuffer.wrap(data, offset, length)).getProtoBufMessage()).build();
  }

  @Test
  void testChunkRelativeRangesPreserveBuffer() throws Exception {
    byte[] data = "ABCDEFGHIJKL".getBytes(UTF_8);
    List<ContainerProtos.ChunkInfo> chunks = Arrays.asList(chunk(data, 0, 3, 4), chunk(data, 3, 9, 4));
    for (int start : new int[] {0, 3, 7, 11}) {
      ByteBuffer buffer = ByteBuffer.wrap(data, start, data.length - start);
      buffer.mark();
      Checksum.validateChecksums(buffer, start, start == 0 ? 0 : 1, chunks);
      assertEquals(start, buffer.position());
      assertEquals(data.length, buffer.limit());
      buffer.reset();
    }
    Checksum.validateChecksums(ByteBuffer.allocate(0), 0, 0, Collections.emptyList());
    data[8]++;
    assertThrows(OzoneChecksumException.class,
        () -> Checksum.validateChecksums(ByteBuffer.wrap(data), 0, 0, chunks));
  }

  @Test
  void testMalformedChunkCoverageAndAlignment() throws Exception {
    byte[] data = "ABCDEFGHIJKL".getBytes(UTF_8);
    ContainerProtos.ChunkInfo first = chunk(data, 0, 3, 4);
    ContainerProtos.ChunkInfo second = chunk(data, 3, 9, 4);
    for (List<ContainerProtos.ChunkInfo> chunks : Arrays.asList(
        Collections.<ContainerProtos.ChunkInfo>emptyList(), Collections.singletonList(first),
        Arrays.asList(second, first),
        Arrays.asList(first, second.toBuilder().setOffset(2).build()),
        Arrays.asList(first, second.toBuilder().setOffset(4).build()))) {
      assertThrows(OzoneChecksumException.class,
          () -> Checksum.validateChecksums(ByteBuffer.wrap(data), 0, 0, chunks));
    }
    assertThrows(OzoneChecksumException.class, () -> Checksum.validateChecksums(
        ByteBuffer.wrap(data), -1, 0, Arrays.asList(first, second)));
    assertThrows(OzoneChecksumException.class, () -> Checksum.validateChecksums(
        ByteBuffer.wrap(data), 0, -1, Arrays.asList(first, second)));
    ContainerProtos.ChunkInfo last = first.toBuilder().setOffset(Long.MAX_VALUE - first.getLen()).build();
    assertThrows(OzoneChecksumException.class, () -> Checksum.validateChecksums(
        ByteBuffer.wrap(data), last.getOffset(), 0, Collections.singletonList(last)));
    for (int[] range : new int[][] {{4, 4}, {3, 3}}) {
      assertThrows(OzoneChecksumException.class, () -> Checksum.validateChecksums(
          ByteBuffer.wrap(data, range[0], range[1]), range[0], 0, Collections.singletonList(second)));
    }
    ContainerProtos.ChunkInfo invalid = second.toBuilder().setChecksumData(
        second.getChecksumData().toBuilder().setBytesPerChecksum(0)).build();
    assertThrows(OzoneChecksumException.class, () -> Checksum.validateChecksums(
        ByteBuffer.wrap(data, 3, 9), 3, 0, Collections.singletonList(invalid)));
  }

  @Test
  void testNoneAndDifferentChunkParameters() throws Exception {
    byte[] data = "ABCDEFGHIJKL".getBytes(UTF_8);
    ContainerProtos.ChunkInfo first = chunk(data, 0, 3, 2).toBuilder()
        .setChecksumData(Checksum.getNoChecksumDataProto()).build();
    List<ContainerProtos.ChunkInfo> chunks = Arrays.asList(first, chunk(data, 3, 9, 3));
    Checksum.validateChecksums(ByteBuffer.wrap(data, 1, 11), 1, 0, chunks);
    Checksum.validateChecksums(ByteBuffer.wrap(data), 0, 0,
        Arrays.asList(chunk(data, 0, 3, 2), chunk(data, 3, 9, 3)));
  }

}
