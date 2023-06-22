/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.spi.impl.OldContainerKeyPrefixCodecForTesting;
import org.apache.hadoop.ozone.recon.spi.impl.OldKeyPrefixContainerCodecForTesting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit Tests for Codecs used in Recon.
 */
public class TestReconCodecs {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestReconCodecs.class);

  static long nextPositiveLong() {
    final long next = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE) + 1;
    Assertions.assertTrue(next > 0);
    return next;
  }

  @Test
  public void testContainerKeyPrefixCodec() throws Exception {
    // consistent with old codecs
    runTestContainerKeyPrefixCodec(
        nextPositiveLong(), "TestKeyPrefix", 0, true, true);
    final String key = "0123456789";
    for (int i = 0; i < 10; i++) {
      runTestContainerKeyPrefixCodec(
          nextPositiveLong(), key.substring(i), nextPositiveLong(), true, true);
    }

    // Both old codecs cannot handle keyVersion == -1.
    runTestContainerKeyPrefixCodec(
        nextPositiveLong(), "TestKeyPrefix", -1, false, false);

    // Both old codecs can handle empty/null keyPrefix.
    runTestContainerKeyPrefixCodec(
        nextPositiveLong(), "", 0, true, null);
    runTestContainerKeyPrefixCodec(
        nextPositiveLong(), null, 0, true, null);

    // Old ContainerKeyPrefixCodec can handle containerId == -1
    // but old KeyPrefixContainerCodec cannot.
    runTestContainerKeyPrefixCodec(
        -1, "TestKeyPrefix", 0, true, false);
  }

  static void runTestContainerKeyPrefixCodec(
      long containerId, String keyPrefix, long keyVersion,
      boolean oldCKPCodecValid, Boolean oldKPCCodecValid) throws Exception {
    ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(
        containerId, keyPrefix, keyVersion);
    runTestContainerKeyPrefixCodec(containerKeyPrefix, oldCKPCodecValid);

    final KeyPrefixContainer keyPrefixContainer
        = containerKeyPrefix.toKeyPrefixContainer();
    if (keyPrefixContainer != null) {
      runTestKeyPrefixContainerCodec(keyPrefixContainer, oldKPCCodecValid);
    } else {
      Assertions.assertNull(oldKPCCodecValid);
    }
  }

  @Test
  public void testKeyPrefixContainer() throws Exception {
    // consistent with old codecs
    runTestKeyPrefixContainer(
        "TestKeyPrefix", 0, nextPositiveLong(), true, true);
    final String key = "0123456789";
    for (int i = 0; i < 10; i++) {
      runTestKeyPrefixContainer(
          key.substring(i), nextPositiveLong(), nextPositiveLong(), true, true);
    }

    // Both old codecs cannot handle keyVersion == -1.
    runTestKeyPrefixContainer(
        "TestKeyPrefix", -1, nextPositiveLong(), false, false);

    // Both old codecs can handle empty non-null keyPrefix.
    runTestKeyPrefixContainer(
        "", 0, nextPositiveLong(), true, true);

    // Old ContainerKeyPrefixCodec can handle containerId == -1
    // but old KeyPrefixContainerCodec cannot.
    runTestKeyPrefixContainer(
        "TestKeyPrefix", 0, -1, true, false);
  }

  static void runTestKeyPrefixContainer(
      String keyPrefix, long keyVersion, long containerId,
      boolean oldCKPCodecValid, boolean oldKPCCodecValid) throws Exception {
    final KeyPrefixContainer keyPrefixContainer = KeyPrefixContainer.get(
        keyPrefix, keyVersion, containerId);
    runTestKeyPrefixContainerCodec(keyPrefixContainer, oldKPCCodecValid);

    final ContainerKeyPrefix containerKeyPrefix
        = keyPrefixContainer.toContainerKeyPrefix();
    runTestContainerKeyPrefixCodec(containerKeyPrefix, oldCKPCodecValid);
  }

  static void runTestContainerKeyPrefixCodec(
      ContainerKeyPrefix containerKeyPrefix,
      boolean oldCodecValid) throws Exception {
    final Codec<ContainerKeyPrefix> newCodec = ContainerKeyPrefix.getCodec();
    final Codec<ContainerKeyPrefix> oldCodec
        = OldContainerKeyPrefixCodecForTesting.get();
    runTestContainerKeyPrefixCodec(containerKeyPrefix, newCodec);
    if (oldCodecValid) {
      runTestContainerKeyPrefixCodec(containerKeyPrefix, oldCodec);
    } else {
      try {
        runTestContainerKeyPrefixCodec(containerKeyPrefix, oldCodec);
        throw new IllegalStateException(
            "Unexpected: oldCodec did not fail for " + containerKeyPrefix);
      } catch (AssertionFailedError e) {
        LOG.info("Good!", e);
      }
    }
    CodecTestUtil.runTest(newCodec, containerKeyPrefix, null,
        oldCodecValid ? oldCodec : null);
  }

  static void runTestKeyPrefixContainerCodec(
      KeyPrefixContainer keyPrefixContainer,
      boolean oldCodecValid) throws Exception {
    final Codec<KeyPrefixContainer> newCodec = KeyPrefixContainer.getCodec();
    final Codec<KeyPrefixContainer> oldCodec
        = OldKeyPrefixContainerCodecForTesting.get();
    runTestKeyPrefixContainerCodec(keyPrefixContainer, newCodec);
    if (oldCodecValid) {
      runTestKeyPrefixContainerCodec(keyPrefixContainer, oldCodec);
    } else {
      try {
        runTestKeyPrefixContainerCodec(keyPrefixContainer, oldCodec);
        throw new IllegalStateException(
            "Unexpected: oldCodec did not fail for " + keyPrefixContainer);
      } catch (AssertionFailedError e) {
        LOG.info("Good!", e);
      }
    }
    CodecTestUtil.runTest(newCodec, keyPrefixContainer, null,
        oldCodecValid ? oldCodec : null);
  }

  static void runTestContainerKeyPrefixCodec(
      ContainerKeyPrefix containerKeyPrefix,
      Codec<ContainerKeyPrefix> codec) throws Exception {
    byte[] persistedFormat = codec.toPersistedFormat(containerKeyPrefix);
    Assertions.assertNotNull(persistedFormat);
    ContainerKeyPrefix fromPersistedFormat =
        codec.fromPersistedFormat(persistedFormat);
    Assertions.assertEquals(containerKeyPrefix, fromPersistedFormat);
  }

  static void runTestKeyPrefixContainerCodec(
      KeyPrefixContainer containerKeyPrefix,
      Codec<KeyPrefixContainer> codec) throws Exception {
    byte[] persistedFormat = codec.toPersistedFormat(containerKeyPrefix);
    Assertions.assertNotNull(persistedFormat);
    KeyPrefixContainer fromPersistedFormat =
        codec.fromPersistedFormat(persistedFormat);
    LOG.info("fromPersistedFormat = " + fromPersistedFormat);
    Assertions.assertEquals(containerKeyPrefix, fromPersistedFormat);
  }

  @Test
  public void testIntegerCodec() throws IOException {
    Integer i = 1000;
    Codec<Integer> codec = IntegerCodec.get();
    byte[] persistedFormat = codec.toPersistedFormat(i);
    Assertions.assertTrue(persistedFormat != null);
    Integer fromPersistedFormat =
        codec.fromPersistedFormat(persistedFormat);
    Assertions.assertEquals(i, fromPersistedFormat);
  }
}
