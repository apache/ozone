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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

class TestOzoneClientConfig {

  @Test
  void missingSizeSuffix() {
    final int bytes = 1024;

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt("ozone.client.bytes.per.checksum", bytes);

    OzoneClientConfig subject = conf.getObject(OzoneClientConfig.class);

    assertEquals(OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE, subject.getBytesPerChecksum());
  }

  @Test
  void testClientHBaseEnhancementsAllowedTrue() {
    // When ozone.client.hbase.enhancements.allowed = true,
    // related client configs should be effective as-is.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);

    // Note: ozone.fs.hsync.enabled is checked by OzoneFSUtils.canEnableHsync(), thus not checked here
    conf.setBoolean("ozone.client.incremental.chunk.list", true);
    conf.setBoolean("ozone.client.stream.putblock.piggybacking", true);
    conf.setInt("ozone.client.key.write.concurrency", -1);

    OzoneClientConfig subject = conf.getObject(OzoneClientConfig.class);

    assertTrue(subject.getIncrementalChunkList());
    assertTrue(subject.getEnablePutblockPiggybacking());
    assertEquals(-1, subject.getMaxConcurrentWritePerKey());
  }

  @Test
  void testClientHBaseEnhancementsAllowedFalse() {
    // When ozone.client.hbase.enhancements.allowed = false,
    // related client configs should be reverted back to default.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", false);

    // Note: ozone.fs.hsync.enabled is checked by OzoneFSUtils.canEnableHsync(), thus not checked here
    conf.setBoolean("ozone.client.incremental.chunk.list", true);
    conf.setBoolean("ozone.client.stream.putblock.piggybacking", true);
    conf.setInt("ozone.client.key.write.concurrency", -1);

    OzoneClientConfig subject = conf.getObject(OzoneClientConfig.class);

    assertFalse(subject.getIncrementalChunkList());
    assertFalse(subject.getEnablePutblockPiggybacking());
    assertEquals(1, subject.getMaxConcurrentWritePerKey());
  }

  @Test
  public void testStreamReadConfigParsing() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.client.stream.read.pre-read-size", "67108864");
    conf.set("ozone.client.stream.read.response-data-size", "2097152");
    conf.set("ozone.client.stream.read.timeout", "5s");

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);

    assertEquals(64L << 20, clientConfig.getStreamReadPreReadSize());
    assertEquals(2 << 20, clientConfig.getStreamReadResponseDataSize());
    assertEquals(Duration.ofSeconds(5), clientConfig.getStreamReadTimeout());
  }
}
