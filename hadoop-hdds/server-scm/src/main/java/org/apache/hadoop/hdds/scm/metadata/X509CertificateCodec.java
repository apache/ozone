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

package org.apache.hadoop.hdds.scm.metadata;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.io.LengthOutputStream;
import org.apache.ratis.util.function.CheckedFunction;

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

  private CheckedFunction<OutputStream, Integer, IOException> writeTo(X509Certificate object) {
    return new CheckedFunction<OutputStream, Integer, IOException>() {
      @Override
      public Integer apply(OutputStream out) throws IOException {
        return CertificateCodec.writePEMEncoded(object, new LengthOutputStream(out)).getLength();
      }

      @Override
      public String toString() {
        return "cert: " + object;
      }
    };
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull X509Certificate object,
      CodecBuffer.Allocator allocator) throws CodecException {
    return allocator.apply(-INITIAL_CAPACITY).put(writeTo(object));
  }

  @Override
  public X509Certificate fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {
    final InputStream in = buffer.getInputStream();
    try {
      return CertificateCodec.readX509Certificate(in);
    } catch (CertificateException e) {
      throw new CodecException("Failed to readX509Certificate from " + buffer, e);
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

  @Override
  public byte[] toPersistedFormat(X509Certificate object) throws CodecException {
    try (CodecBuffer buffer = toHeapCodecBuffer(object)) {
      return buffer.getArray();
    }
  }

  @Override
  public X509Certificate fromPersistedFormat(byte[] rawData)
      throws CodecException {
    // ByteArrayInputStream.close(), which is a noop, can be safely ignored.
    final ByteArrayInputStream in = new ByteArrayInputStream(rawData);
    try {
      return CertificateCodec.readX509Certificate(in);
    } catch (CertificateException e) {
      throw new CodecException("Failed to readX509Certificate from rawData, length=" + rawData.length, e);
    }
  }

  @Override
  public X509Certificate copyObject(X509Certificate object) {
    return object;
  }
}
