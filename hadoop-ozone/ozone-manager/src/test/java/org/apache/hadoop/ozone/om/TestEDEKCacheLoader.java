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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.ozone.om.OzoneManager.EDEKCacheLoader;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EDEKCacheLoader}, the OM startup warm-up of encrypted data
 * encryption keys (HDDS-12486). These exercise the loader logic directly
 * against a mock {@link KeyProviderCryptoExtension}, so they need neither a
 * running KMS nor a mini cluster.
 */
public class TestEDEKCacheLoader {

  private static final String[] KEY_NAMES = {"key1", "key2"};

  @Test
  public void warmsUpEncryptedKeysOnStartup() throws Exception {
    KeyProviderCryptoExtension kp = mock(KeyProviderCryptoExtension.class);
    doNothing().when(kp).warmUpEncryptedKeys(KEY_NAMES);

    new EDEKCacheLoader(KEY_NAMES, kp, 0, 1, 3).run();

    verify(kp, times(1)).warmUpEncryptedKeys(KEY_NAMES);
  }

  @Test
  public void retriesUntilWarmUpSucceeds() throws Exception {
    KeyProviderCryptoExtension kp = mock(KeyProviderCryptoExtension.class);
    doThrow(new IOException("KMS not reachable yet"))
        .doNothing()
        .when(kp).warmUpEncryptedKeys(KEY_NAMES);

    new EDEKCacheLoader(KEY_NAMES, kp, 0, 1, 3).run();

    // One failed attempt followed by one successful attempt.
    verify(kp, times(2)).warmUpEncryptedKeys(KEY_NAMES);
  }

  @Test
  public void stopsAfterMaxRetriesWithoutThrowing() throws Exception {
    KeyProviderCryptoExtension kp = mock(KeyProviderCryptoExtension.class);
    doThrow(new IOException("KMS unavailable"))
        .when(kp).warmUpEncryptedKeys(KEY_NAMES);

    EDEKCacheLoader loader = new EDEKCacheLoader(KEY_NAMES, kp, 0, 1, 2);

    // A persistent KMS failure must not propagate out of the loader; it gives
    // up after maxRetries attempts.
    assertDoesNotThrow(loader::run);
    verify(kp, times(2)).warmUpEncryptedKeys(KEY_NAMES);
  }
}
