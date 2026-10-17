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

package org.apache.hadoop.hdds.security.symmetric;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
import org.junit.jupiter.api.Test;

/** Tests refreshing the signing key through the composite client. */
class TestDefaultSecretKeyClient {
  @Test
  void refetchSecretKeyUpdatesCurrentKey() throws Exception {
    SecretKeyProtocol scm = mock(SecretKeyProtocol.class);
    ManagedSecretKey oldKey = SecretKeyTestUtil.generateHmac(Instant.now(), Duration.ofDays(9));
    ManagedSecretKey newKey = SecretKeyTestUtil.generateHmac(Instant.now(), Duration.ofDays(9));
    when(scm.getCurrentSecretKey()).thenReturn(oldKey, newKey);
    SecretKeyClient client = DefaultSecretKeyClient.create(new OzoneConfiguration(), scm, "test-");
    try {
      client.start(new OzoneConfiguration());
      assertSame(oldKey, client.getCurrentSecretKey());
      client.refetchSecretKey();
      assertSame(newKey, client.getCurrentSecretKey());
      verify(scm, times(2)).getCurrentSecretKey();
    } finally {
      client.stop();
    }
  }

  @Test
  void refetchSecretKeyPropagatesFailure() throws Exception {
    SecretKeyProtocol scm = mock(SecretKeyProtocol.class);
    ManagedSecretKey oldKey = SecretKeyTestUtil.generateHmac(Instant.now(), Duration.ofDays(9));
    IOException failure = new IOException("SCM fetch failed");
    when(scm.getCurrentSecretKey()).thenReturn(oldKey).thenThrow(failure);
    SecretKeyClient client = DefaultSecretKeyClient.create(new OzoneConfiguration(), scm, "test-");
    try {
      client.start(new OzoneConfiguration());
      UncheckedIOException exception = assertThrows(UncheckedIOException.class, client::refetchSecretKey);
      assertSame(failure, exception.getCause());
      assertSame(oldKey, client.getCurrentSecretKey());
    } finally {
      client.stop();
    }
  }
}
