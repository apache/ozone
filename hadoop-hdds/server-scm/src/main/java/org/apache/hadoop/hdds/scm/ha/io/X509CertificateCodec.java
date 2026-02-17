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

package org.apache.hadoop.hdds.scm.ha.io;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.cert.X509Certificate;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Codec for type X509Certificate.
 */
public class X509CertificateCodec implements Codec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    try {
      String certString =
          CertificateCodec.getPEMEncodedString((X509Certificate) object);
      // getBytes returns a new array
      return UnsafeByteOperations.unsafeWrap(certString.getBytes(UTF_8));
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "X509Certificate cannot be decoded: " + ex.getMessage());
    }
  }

  @Override
  public Object deserialize(Class< ? > type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      String pemEncodedCert = new String(value.toByteArray(), UTF_8);
      return CertificateCodec.getX509Certificate(pemEncodedCert);
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "X509Certificate cannot be decoded: " + ex.getMessage());
    }
  }
}

