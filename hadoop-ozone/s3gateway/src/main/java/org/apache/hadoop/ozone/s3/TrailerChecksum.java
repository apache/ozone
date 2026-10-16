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

package org.apache.hadoop.ozone.s3;

import java.security.MessageDigest;
import java.util.zip.Checksum;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/** Incrementally verifies a checksum over the decoded payload, independently of chunk signatures. */
final class TrailerChecksum {
  private final MessageDigest digest;
  private final Checksum checksum;
  private final int checksumLength;

  TrailerChecksum(MessageDigest digest) {
    this.digest = digest;
    this.checksum = null;
    this.checksumLength = digest.getDigestLength();
  }

  TrailerChecksum(Checksum checksum, int checksumLength) {
    this.digest = null;
    this.checksum = checksum;
    this.checksumLength = checksumLength;
  }

  void update(byte value) {
    if (digest != null) {
      digest.update(value);
    } else {
      checksum.update(value);
    }
  }

  void update(byte[] bytes, int offset, int length) {
    if (digest != null) {
      digest.update(bytes, offset, length);
    } else {
      checksum.update(bytes, offset, length);
    }
  }

  void verify(byte[] expected, String resource) {
    byte[] actual;
    if (digest != null) {
      actual = digest.digest();
    } else {
      actual = new byte[checksumLength];
      long value = checksum.getValue();
      for (int i = actual.length - 1; i >= 0; i--) {
        actual[i] = (byte) value;
        value >>>= Byte.SIZE;
      }
    }
    if (!MessageDigest.isEqual(expected, actual)) {
      throw S3ErrorTable.newError(S3ErrorTable.BAD_DIGEST, resource);
    }
  }

  /** CRC-64/NVME: reflected polynomial 0x9a6c9329ac4bc9b5, initial value and final XOR all ones. */
  static final class Crc64Nvme implements Checksum {
    private static final long[] TABLE = createTable();
    private long crc = -1L;

    private static long[] createTable() {
      long[] table = new long[256];
      for (int i = 0; i < table.length; i++) {
        long value = i;
        for (int bit = 0; bit < Byte.SIZE; bit++) {
          value = (value >>> 1) ^ ((value & 1) == 0 ? 0 : 0x9a6c9329ac4bc9b5L);
        }
        table[i] = value;
      }
      return table;
    }

    @Override
    public void update(int value) {
      crc = TABLE[((int) crc ^ value) & 0xff] ^ (crc >>> Byte.SIZE);
    }

    @Override
    public void update(byte[] bytes, int offset, int length) {
      for (int i = offset; i < offset + length; i++) {
        update(bytes[i]);
      }
    }

    @Override
    public long getValue() {
      return ~crc;
    }

    @Override
    public void reset() {
      crc = -1L;
    }
  }
}
