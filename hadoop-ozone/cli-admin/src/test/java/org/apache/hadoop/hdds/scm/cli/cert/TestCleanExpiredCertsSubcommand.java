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

package org.apache.hadoop.hdds.scm.cli.cert;

import static org.apache.hadoop.hdds.security.x509.CertificateTestUtils.createSelfSignedCert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestCleanExpiredCertsSubcommand {

  private SCMSecurityProtocol scmSecurityProtocolMock;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private static final String OUTPUT_FORMAT = "%-17s %-30s %-30s %-110s %-110s";

  @BeforeEach
  public void setup() throws IOException {
    scmSecurityProtocolMock = mock(SCMSecurityProtocol.class);
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCleaningOneCertificate() throws Exception {
    CleanExpiredCertsSubcommand cmd = new CleanExpiredCertsSubcommand();
    KeyPair keyPair = CertificateTestUtils.aKeyPair(new OzoneConfiguration());
    X509Certificate cert = createSelfSignedCert(keyPair, "aCert");
    ArrayList<String> certPemList = new ArrayList<>();
    certPemList.add(CertificateCodec.getPEMEncodedString(cert));
    when(scmSecurityProtocolMock.removeExpiredCertificates())
        .thenReturn(certPemList);
    cmd.execute(scmSecurityProtocolMock);
    String cliOutPut = outContent.toString(DEFAULT_ENCODING);
    String certInfo = String.format(OUTPUT_FORMAT, cert.getSerialNumber(),
        cert.getNotBefore(), cert.getNotAfter(), cert.getSubjectDN(),
        cert.getIssuerDN());
    assertThat(cliOutPut).contains(certInfo);
  }
}
