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

package org.apache.hadoop.ozone.om;

import static java.util.Collections.emptyList;
import static org.apache.hadoop.hdds.security.x509.CertificateTestUtils.aKeyPair;
import static org.apache.hadoop.hdds.security.x509.CertificateTestUtils.createSelfSignedCert;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getPEMEncodedString;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests to check functionality of how we provide the ServiceInfoEx object
 * from OM to clients.
 */
public class TestServiceInfoProvider {

  private OzoneConfiguration conf;
  private OzoneManagerProtocol om;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    om = mock(OzoneManagerProtocol.class);
    when(om.getServiceList()).thenReturn(emptyList());
  }

  /**
   * Tests for unsecure environment.
   */
  @Nested
  public class UnsecureEnvironment {

    private ServiceInfoProvider provider;

    @BeforeEach
    public void setup() throws Exception {
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, false);
      provider = new ServiceInfoProvider(new SecurityConfig(conf), om, null);
    }

    @Test
    public void test() throws Exception {
      ServiceInfoEx info = provider.provide();

      assertThat(info.getServiceInfoList()).isSameAs(emptyList());
      assertThat(info.getCaCertificate()).isNull();
      assertThat(info.getCaCertPemList()).isEmpty();
    }
  }

  /**
   * Tests for secure environment.
   */
  @Nested
  public class TestSecureEnvironment {

    private CertificateClient certClient;
    private String pem1;
    private X509Certificate cert2;
    private String pem2;
    private ServiceInfoProvider provider;

    @BeforeEach
    public void setup() throws Exception {
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      certClient = mock(CertificateClient.class);
      X509Certificate cert1 = createSelfSignedCert(aKeyPair(conf), "1st", Duration.ofDays(1));
      pem1 = getPEMEncodedString(cert1);
      cert2 = createSelfSignedCert(aKeyPair(conf), "2nd", Duration.ofDays(2));
      pem2 = getPEMEncodedString(cert2);
      when(certClient.getAllRootCaCerts())
          .thenReturn(new HashSet<>(Arrays.asList(cert1, cert2)));
      provider =
          new ServiceInfoProvider(new SecurityConfig(conf), om, certClient);
    }

    @Test
    public void withoutRootCARenew() throws Exception {
      ServiceInfoEx info = provider.provide();

      assertThat(info.getServiceInfoList()).isSameAs(emptyList());
      assertThat(info.getCaCertificate()).isEqualTo(pem2);
      assertThat(info.getCaCertPemList()).contains(pem1, pem2);

      info = provider.provide();

      assertThat(info.getServiceInfoList()).isSameAs(emptyList());
      assertThat(info.getCaCertificate()).isEqualTo(pem2);
      assertThat(info.getCaCertPemList()).contains(pem1, pem2);
    }

    @Test
    public void withRootCARenew() throws Exception {
      ServiceInfoEx info = provider.provide();

      assertThat(info.getServiceInfoList()).isSameAs(emptyList());
      assertThat(info.getCaCertificate()).isEqualTo(pem2);
      assertThat(info.getCaCertPemList()).contains(pem1, pem2);

      X509Certificate cert3 =
          createSelfSignedCert(aKeyPair(conf), "cn", Duration.ofDays(3));
      String pem3 = getPEMEncodedString(cert3);
      List<X509Certificate> certs = Arrays.asList(cert2, cert3);
      ArgumentCaptor<Function<List<X509Certificate>, CompletableFuture<Void>>>
          captor = ArgumentCaptor.forClass(Function.class);
      verify(certClient).registerRootCARotationListener(captor.capture());
      captor.getValue().apply(certs).join();

      info = provider.provide();

      assertThat(info.getServiceInfoList()).isSameAs(emptyList());
      assertThat(info.getCaCertificate()).isEqualTo(pem3);
      assertThat(info.getCaCertPemList()).contains(pem2, pem3);
    }
  }
}
