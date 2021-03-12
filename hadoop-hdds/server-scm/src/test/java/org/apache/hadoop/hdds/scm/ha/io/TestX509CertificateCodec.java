/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;
import java.security.cert.X509Certificate;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Class to test X509CertificateCodec serialize and deserialize.
 */
public class TestX509CertificateCodec {

  @Test
  public void codec() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate x509Certificate =
        KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
        "SHA256withRSA");

    X509CertificateCodec x509CertificateCodec = new X509CertificateCodec();
    ByteString byteString = x509CertificateCodec.serialize(x509Certificate);

    X509Certificate actual = (X509Certificate)
        x509CertificateCodec.deserialize(X509Certificate.class, byteString);

    Assert.assertEquals(x509Certificate, actual);

  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testCodecError() throws Exception {

    X509CertificateCodec x509CertificateCodec = new X509CertificateCodec();
    ByteString byteString = ByteString.copyFrom("dummy".getBytes(UTF_8));

    x509CertificateCodec.deserialize(X509Certificate.class, byteString);
  }
}
