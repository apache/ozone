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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Base64;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests payload checksums with independent AWS CRT and hashlib reference values. */
class TestTrailerChecksum {

  @ParameterizedTest
  @CsvSource({"CRC32,y/Q5Jg==,AAAAAA==",
      "CRC32C,4waSgw==,AAAAAA==",
      "CRC64NVME,rosUhgp5mIg=,AAAAAAAAAAA=",
      "SHA1,98O8HYCOBHMq32eZZczDTKeuNEE=,2jmj7l5rSw0yVb/vlWAYkK/YBwk=",
      "SHA256,FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=,47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
      "SHA512,2eZ2LdHI6vbWGzxhkvxAjU1tXxF20MKRabwk5xw/J0rSf81YEbMT1oH35V7ALXPUmclUVba1u1A6z1dPuo/+hQ==,"
          + "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==",
      "XXHASH64,jLhB20DmroM=,70bbN1HY6Zk=",
      "XXHASH3,ctyxi2ehff8=,LQaABTjTlMI=",
      "XXHASH128,MxGUd+3l3NXpcWQnaB1YYA==,maoG0wFHmNhgAcMkRo1Jfw=="})
  void verifiesKnownVectorsAcrossReadBoundaries(TrailerHeader header, String expected, String empty) {
    header.newChecksum().verify(Base64.getDecoder().decode(empty), "key");
    byte[] payload = "!123456789!".getBytes(UTF_8);
    for (int split = 0; split <= 9; split++) {
      TrailerChecksum checksum = header.newChecksum();
      checksum.update(payload, 1, split);
      for (int i = 1 + split; i <= 9; i++) {
        checksum.update(payload[i]);
      }
      checksum.verify(Base64.getDecoder().decode(expected), "key");
    }

    TrailerChecksum wrong = header.newChecksum();
    wrong.update(payload, 0, payload.length);
    OS3Exception error = assertThrows(OS3Exception.class,
        () -> wrong.verify(Base64.getDecoder().decode(expected), "key"));
    assertThat(error.getCode()).isEqualTo(S3ErrorTable.BAD_DIGEST.getCode());
  }

  @ParameterizedTest
  @CsvSource({"XXHASH64,niilpeyUrvg=", "XXHASH3,2bjpPvP75BY=", "XXHASH128,8VoTlV1TZ+jZuOk+8/vkFg=="})
  void xxHashMatchesLongPayloadAcrossUpdates(TrailerHeader header, String expected) {
    byte[] payload = new byte[1027];
    for (int i = 0; i < 1025; i++) {
      payload[i + 1] = (byte) (i * 131 + 17);
    }
    TrailerChecksum checksum = header.newChecksum();
    int offset = 1;
    for (int length : new int[]{7, 1, 241, 3, 773}) {
      if (length == 1) {
        checksum.update(payload[offset]);
      } else {
        checksum.update(payload, offset, length);
      }
      offset += length;
    }
    checksum.verify(Base64.getDecoder().decode(expected), "key");
  }

  @Test
  void crc64MatchesAwsVectorsAndResets() {
    TrailerChecksum.Crc64Nvme checksum = new TrailerChecksum.Crc64Nvme();
    byte[] payload = new byte[32];
    checksum.update(payload, 0, payload.length);
    assertThat(checksum.getValue()).isEqualTo(0xcf3473434d4ecf3bL);
    checksum.reset();
    assertThat(checksum.getValue()).isZero();
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    checksum.update(payload, 0, payload.length);
    assertThat(checksum.getValue()).isEqualTo(0xb9d9d4a8492cbd7fL);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 255, 256, 257, 8192, 8199})
  void crc64BulkUpdatesMatchSingleBytes(int length) {
    byte[] payload = new byte[length + 8];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i * 131 + 17);
    }
    for (int offset = 0; offset < 8; offset++) {
      TrailerChecksum.Crc64Nvme expected = new TrailerChecksum.Crc64Nvme();
      for (int i = offset; i < offset + length; i++) {
        expected.update(payload[i]);
      }
      for (int split = 0; split <= Math.min(length, 16); split++) {
        TrailerChecksum.Crc64Nvme actual = new TrailerChecksum.Crc64Nvme();
        actual.update(payload, offset, split);
        actual.update(payload, offset + split, length - split);
        assertThat(actual.getValue()).isEqualTo(expected.getValue());
      }
    }
  }
}
