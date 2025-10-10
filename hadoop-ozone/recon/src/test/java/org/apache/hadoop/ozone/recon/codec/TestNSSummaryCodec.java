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

package org.apache.hadoop.ozone.recon.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.junit.jupiter.api.Test;

/**
 * Tests for NSSummaryCodec CodecBuffer implementation.
 */
public class TestNSSummaryCodec {

  private final Codec<NSSummary> codec = NSSummaryCodec.get();

  @Test
  public void testCodecBufferRoundTrip() throws Exception {
    NSSummary original = createTestNSSummary();

    CodecBuffer buffer = codec.toCodecBuffer(original, CodecBuffer.Allocator.DIRECT);
    try {
      NSSummary decoded = codec.fromCodecBuffer(buffer);
      assertNSSummaryEquals(original, decoded);
    } finally {
      buffer.close();
    }
  }

  @Test
  public void testCodecBufferEmptyDirectory() throws Exception {
    NSSummary original = new NSSummary();
    original.setDirName("empty");
    original.setParentId(42L);

    CodecBuffer buffer = codec.toCodecBuffer(original, CodecBuffer.Allocator.DIRECT);
    try {
      NSSummary decoded = codec.fromCodecBuffer(buffer);
      assertNSSummaryEquals(original, decoded);
    } finally {
      buffer.close();
    }
  }

  @Test
  public void testCodecBufferLargeDirectory() throws Exception {
    NSSummary original = new NSSummary();
    original.setDirName("large");
    original.setNumOfFiles(10000);
    original.setSizeOfFiles(1024L * 1024L * 100L); // 100MB
    original.setParentId(999L);

    Set<Long> childDirs = new HashSet<>();
    for (long i = 1; i <= 1000; i++) {
      childDirs.add(i);
    }
    original.setChildDir(childDirs);

    int[] buckets = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = i * 100;
    }
    original.setFileSizeBucket(buckets);

    CodecBuffer buffer = codec.toCodecBuffer(original, CodecBuffer.Allocator.DIRECT);
    try {
      NSSummary decoded = codec.fromCodecBuffer(buffer);
      assertNSSummaryEquals(original, decoded);
    } finally {
      buffer.close();
    }
  }

  private NSSummary createTestNSSummary() {
    NSSummary summary = new NSSummary();
    summary.setDirName("test/directory");
    summary.setNumOfFiles(100);
    summary.setSizeOfFiles(1024L * 512L); // 512KB
    summary.setParentId(42L);

    Set<Long> childDirs = new HashSet<>();
    childDirs.add(1L);
    childDirs.add(2L);
    childDirs.add(3L);
    summary.setChildDir(childDirs);

    int[] buckets = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = i * 10;
    }
    summary.setFileSizeBucket(buckets);

    return summary;
  }

  private void assertNSSummaryEquals(NSSummary expected, NSSummary actual) {
    assertEquals(expected.getDirName(), actual.getDirName());
    assertEquals(expected.getNumOfFiles(), actual.getNumOfFiles());
    assertEquals(expected.getSizeOfFiles(), actual.getSizeOfFiles());
    assertEquals(expected.getParentId(), actual.getParentId());
    assertEquals(expected.getChildDir(), actual.getChildDir());
    assertArrayEquals(expected.getFileSizeBucket(), actual.getFileSizeBucket());
  }
}
