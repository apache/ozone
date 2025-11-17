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

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec to serialize/deserialize {@link X509Certificate}.
 */
public final class OldX509CertificateCodecForTesting
    implements Codec<X509Certificate> {

  private static final Codec<X509Certificate> INSTANCE =
      new OldX509CertificateCodecForTesting();

  public static Codec<X509Certificate> get() {
    return INSTANCE;
  }

  private OldX509CertificateCodecForTesting() {
    // singleton
  }

  @Override
  public Class<X509Certificate> getTypeClass() {
    return X509Certificate.class;
  }

  @Override
  public byte[] toPersistedFormatImpl(X509Certificate object) throws SCMSecurityException {
    return CertificateCodec.getPEMEncodedString(object)
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public X509Certificate fromPersistedFormatImpl(byte[] rawData) throws CertificateException {
    return CertificateCodec.getX509Certificate(rawData);
  }

  @Override
  public X509Certificate copyObject(X509Certificate object) {
    return object;
  }
}
