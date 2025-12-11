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

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for OzoneOutputStream covering metadata and unwrap behavior.
 */
public class TestOzoneOutputStream {

  /**
   * Fake KeyOutputStream implementation for testing.
   * Uses the package-private KeyOutputStream() constructor.
   */
  private static class FakeKeyOutputStream extends KeyOutputStream
      implements KeyMetadataAware {

    private final Map<String, String> metadata;

    FakeKeyOutputStream(Map<String, String> metadata) {
      super(); // VisibleForTesting constructor
      this.metadata = metadata;
    }

    @Override
    public Map<String, String> getMetadata() {
      return metadata;
    }

    @Override
    public void flush() {
      // avoid KeyOutputStream.flush() using null semaphore
    }

    @Override
    public void close() {
      // avoid real close logic touching internal state
    }
  }

  /**
   * Minimal fake CipherOutputStreamOzone wrapper for testing unwrap().
   */
  private static class FakeCipherOutputStreamOzone
      extends CipherOutputStreamOzone {

    FakeCipherOutputStreamOzone(OutputStream out) {
      super(out);
    }

    @Override
    public OutputStream getWrappedStream() {
      return super.getWrappedStream();
    }
  }

  /**
   * test Plain KeyOutputStream and getMetadata() (no encryption).
   */
  @Test
  public void testPlainKeyOutputStream() throws IOException {
    Map<String, String> metadata = Collections.singletonMap("k1", "v1");

    FakeKeyOutputStream key = new FakeKeyOutputStream(metadata);
    try (OzoneOutputStream ozone = new OzoneOutputStream(key, null)) {
      assertNotNull(ozone.getKeyOutputStream());
      assertEquals("v1", ozone.getMetadata().get("k1"));
    }
  }

  /**
   * test getKeyOutputStream and getMetadata() wrapped inside CryptoOutputStream.
   */
  @Test
  public void testCryptoWrapped() throws IOException {
    Map<String, String> metadata = Collections.singletonMap("k1", "v1");

    FakeKeyOutputStream key = new FakeKeyOutputStream(metadata);

    // Mockito mock for CryptoOutputStream: we only care that
    // 1) it is an instance of CryptoOutputStream
    // 2) getWrappedStream() returns the underlying KeyOutputStream
    CryptoOutputStream crypto = mock(CryptoOutputStream.class);
    when(crypto.getWrappedStream()).thenReturn(key);

    try (OzoneOutputStream ozone = new OzoneOutputStream(crypto, null)) {
      assertNotNull(ozone.getKeyOutputStream());
      assertEquals("v1", ozone.getMetadata().get("k1"));
    }
  }

  /**
   * test getKeyOutputStream and getMetadata() wrapped inside CipherOutputStreamOzone.
   */
  @Test
  public void testCipherWrapped() throws IOException {
    Map<String, String> metadata = Collections.singletonMap("k1", "v1");

    FakeKeyOutputStream key = new FakeKeyOutputStream(metadata);
    FakeCipherOutputStreamOzone cipher =
        new FakeCipherOutputStreamOzone(key);

    try (OzoneOutputStream ozone = new OzoneOutputStream(cipher, null)) {
      assertNotNull(ozone.getKeyOutputStream());
      assertEquals("v1", ozone.getMetadata().get("k1"));
    }
  }

  /**
   * test for Non-KeyMetadataAware stream verify that exception is thrown here.
   */
  @Test
  public void testNonKeyMetadataAwareThrows() throws IOException {
    OutputStream nonMeta = new OutputStream() {
      @Override
      public void write(int b) {

      }
    };

    try (OzoneOutputStream ozone = new OzoneOutputStream(nonMeta, null)) {
      assertThrows(IllegalStateException.class, ozone::getMetadata);
    }
  }
}
