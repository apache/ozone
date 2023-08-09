package org.apache.hadoop.hdds.scm.cli.cert;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;

import static org.apache.hadoop.hdds.security.x509.CertificateTestUtils.createSelfSignedCert;

import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCleanExpiredCertsSubcommand {

  private SCMSecurityProtocol scmSecurityProtocolMock;
  private CleanExpiredCertsSubcommand cmd;
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
    cmd = new CleanExpiredCertsSubcommand();
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
    assertThat(cliOutPut, new StringContains(true, certInfo));
  }
}
