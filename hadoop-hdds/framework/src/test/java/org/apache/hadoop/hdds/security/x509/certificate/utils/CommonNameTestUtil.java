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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.mockito.MockedStatic;

/**
 * Helpers for tests of the certificate commonName, which must fit the RFC 5280 ub-common-name (64). Tests in other
 * modules use them instead of Bouncy Castle.
 */
public final class CommonNameTestUtil {
  /** A Kubernetes pod FQDN, as in HDDS-17879: any {@code <prefix>@} commonName with it is longer than 64. */
  public static final String LONG_HOST_NAME =
      "aaaa-ozone-000-datanode-1.aaa-ozone-000-datanode-headless.aaaa-data.svc.cluster.local";

  private CommonNameTestUtil() {
  }

  /** @return the commonName in the subject of {@code csr}. */
  public static String getCommonName(PKCS10CertificationRequest csr) {
    return getCommonName(csr.getSubject());
  }

  /** @return the commonName in the subject of {@code certificate}. */
  public static String getCommonName(X509Certificate certificate) {
    return getCommonName(X500Name.getInstance(certificate.getSubjectX500Principal().getEncoded()));
  }

  /** @return the commonName in the subject of the CSR that {@code client} builds. */
  public static String getCsrCommonName(DefaultCertificateClient client) throws IOException {
    return getCommonName(client.configureCSRBuilder().build().generateCSR());
  }

  /**
   * Calls {@code action} with {@link InetAddress#getLocalHost()} returning a loopback address whose host name and
   * canonical host name are {@code hostName}.
   */
  public static <T> T withLocalHostName(String hostName, Callable<T> action) throws Exception {
    InetAddress localHost = spy(InetAddress.getByAddress(hostName, new byte[] {127, 0, 0, 1}));
    doReturn(hostName).when(localHost).getCanonicalHostName();
    try (MockedStatic<InetAddress> inetAddress = mockStatic(InetAddress.class, CALLS_REAL_METHODS)) {
      inetAddress.when(InetAddress::getLocalHost).thenReturn(localHost);
      return action.call();
    }
  }

  private static String getCommonName(X500Name name) {
    return name.getRDNs(BCStyle.CN)[0].getFirst().getValue().toString();
  }
}
