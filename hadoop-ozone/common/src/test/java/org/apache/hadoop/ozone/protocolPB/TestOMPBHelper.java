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

package org.apache.hadoop.ozone.protocolPB;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ChecksumTypeProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MD5MD5Crc32FileChecksumProto;
import org.junit.jupiter.api.Test;

/**
 * Test {@link OMPBHelper}.
 */
public final class TestOMPBHelper {
  /**
   * This is to test backward compatibility for a bug fixed by HDDS-12954
   * for {@link OMPBHelper#convertMD5MD5FileChecksum(MD5MD5Crc32FileChecksumProto)}.
   * Previously, the proto md5 was created using a 20-byte buffer with the last 4 bytes unused.
   * This test verifies the new code can handle the previous (buggy) case.
   */
  @Test
  void testConvertMD5MD5FileChecksum() throws Exception {
    runTestConvertMD5MD5FileChecksum(MD5Hash.MD5_LEN);
    // for testing backward compatibility
    runTestConvertMD5MD5FileChecksum(20);
  }

  void runTestConvertMD5MD5FileChecksum(int n) throws Exception {
    System.out.println("n=" + n);
    // random bytesPerCrc and crcPerBlock
    final Random random = ThreadLocalRandom.current();
    final int bytesPerCrc = random.nextInt(1 << 20) + 1;
    final int crcPerBlock = random.nextInt(1 << 20) + 1;

    // random md5
    final byte[] md5bytes = new byte[n];
    random.nextBytes(md5bytes);
    Arrays.fill(md5bytes, MD5Hash.MD5_LEN, n, (byte) 0); // set extra bytes to zeros.
    final ByteString md5 = ByteString.copyFrom(md5bytes);
    System.out.println("md5     : " + StringUtils.bytes2Hex(md5.asReadOnlyByteBuffer()));
    assertEquals(n, md5.size());

    // build proto
    final MD5MD5Crc32FileChecksumProto proto = MD5MD5Crc32FileChecksumProto.newBuilder()
        .setChecksumType(ChecksumTypeProto.CHECKSUM_CRC32)
        .setBytesPerCRC(bytesPerCrc)
        .setCrcPerBlock(crcPerBlock)
        .setMd5(md5)
        .build();

    // covert proto
    final MD5MD5CRC32FileChecksum checksum = OMPBHelper.convertMD5MD5FileChecksum(proto);
    assertEquals(bytesPerCrc, checksum.getChecksumOpt().getBytesPerChecksum());

    // get bytes
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    checksum.write(new DataOutputStream(byteArrayOutputStream));
    final byte[] bytes = byteArrayOutputStream.toByteArray();
    assertEquals(checksum.getLength(), bytes.length);

    // assert bytes
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertEquals(bytesPerCrc, buffer.getInt());
    assertEquals(crcPerBlock, buffer.getLong());
    final ByteString computed = ByteString.copyFrom(buffer);
    System.out.println("computed: " + StringUtils.bytes2Hex(computed.asReadOnlyByteBuffer()));
    assertEquals(MD5Hash.MD5_LEN, computed.size());
    assertEquals(md5.substring(0, MD5Hash.MD5_LEN), computed);
  }
}
