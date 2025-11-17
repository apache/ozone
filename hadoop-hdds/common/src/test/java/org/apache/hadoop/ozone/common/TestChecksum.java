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

import java.nio.ByteBuffer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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
}
