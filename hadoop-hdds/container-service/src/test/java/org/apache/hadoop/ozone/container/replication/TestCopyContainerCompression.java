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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.commons.io.IOUtils.readFully;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPLICATION_COMPRESSION;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.fromProto;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.getDefaultCompression;
import static org.apache.hadoop.ozone.container.replication.GrpcOutputStreamTest.getRandomBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test for {@link CopyContainerCompression}.
 */
class TestCopyContainerCompression {

  @ParameterizedTest
  @EnumSource
  void protoConversion(CopyContainerCompression compression) {
    assertEquals(compression, fromProto(compression.toProto()));
  }

  @ParameterizedTest
  @EnumSource
  void getConfReturnsValidSetting(CopyContainerCompression compression) {
    MutableConfigurationSource conf = new OzoneConfiguration();
    compression.setOn(conf);

    assertEquals(compression, CopyContainerCompression.getConf(conf));
  }

  @Test
  void getConfReturnsDefaultForUnknown() {
    MutableConfigurationSource conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPLICATION_COMPRESSION, "garbage");

    assertEquals(getDefaultCompression(),
        CopyContainerCompression.getConf(conf));
  }

  @ParameterizedTest
  @EnumSource
  void testInputOutput(CopyContainerCompression compression) throws Exception {
    byte[] original = getRandomBytes(16);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (OutputStream compressed = compression.wrap(out)) {
      compressed.write(original);
    }

    byte[] written = out.toByteArray();
    if (compression == CopyContainerCompression.NO_COMPRESSION) {
      assertArrayEquals(original, written);
    } else {
      assertNotEquals(original, written);
    }

    ByteArrayInputStream input = new ByteArrayInputStream(written);
    try (InputStream uncompressed = compression.wrap(input)) {
      byte[] read = new byte[original.length];
      readFully(uncompressed, read);
      assertArrayEquals(original, read);
      assertEquals(0, uncompressed.available());
    }
  }

}
