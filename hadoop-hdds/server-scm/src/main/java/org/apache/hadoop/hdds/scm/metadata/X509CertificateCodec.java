/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.X509Certificate;

import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.io.LengthOutputStream;
import org.apache.ratis.util.function.CheckedFunction;

import jakarta.annotation.Nonnull;

/**
 * Codec to serialize/deserialize {@link X509Certificate}.
 */
public final class X509CertificateCodec implements Codec<X509Certificate> {
  private static final int INITIAL_CAPACITY = 4 << 10; // 4 KB

  private static final Codec<X509Certificate> INSTANCE =
      new X509CertificateCodec();

  public static Codec<X509Certificate> get() {
    return INSTANCE;
  }

  private X509CertificateCodec() {
    // singleton
  }

  @Override
  public Class<X509Certificate> getTypeClass() {
    return X509Certificate.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  CheckedFunction<OutputStream, Integer, IOException> writeTo(
      X509Certificate object) {
    return out -> CertificateCodec.writePEMEncoded(object,
        new LengthOutputStream(out)).getLength();
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull X509Certificate object,
      CodecBuffer.Allocator allocator) throws IOException {
    return allocator.apply(-INITIAL_CAPACITY).put(writeTo(object));
  }

  @Override
  public X509Certificate fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws IOException {
    try (InputStream in = buffer.getInputStream()) {
      return CertificateCodec.readX509Certificate(in);
    }
  }

  @Override
  public byte[] toPersistedFormat(X509Certificate object) throws IOException {
    try (CodecBuffer buffer = toHeapCodecBuffer(object)) {
      return buffer.getArray();
    } catch (SCMSecurityException exp) {
      throw new IOException(exp);
    }
  }

  @Override
  public X509Certificate fromPersistedFormat(byte[] rawData)
      throws IOException {
    return CertificateCodec.readX509Certificate(
        new ByteArrayInputStream(rawData));
  }

  @Override
  public X509Certificate copyObject(X509Certificate object) {
    return object;
  }
}
