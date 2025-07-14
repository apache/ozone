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

package org.apache.hadoop.hdds.scm.ha;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.security.x509.CertificateTestUtils.createSelfSignedCert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509KeyManager;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509TrustManager;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * This test checks that mTLS authentication is turned on for
 * {@link InterSCMGrpcProtocolService}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HDDS-8901">HDDS-8901</a>
 */
class TestInterSCMGrpcProtocolService {

  private static final String CP_FILE_NAME = "cpFile";
  private static final String CP_CONTENTS = "Hello world!";

  private X509Certificate serviceCert;
  private X509Certificate clientCert;

  private ReloadingX509KeyManager serverKeyManager;
  private ReloadingX509TrustManager serverTrustManager;
  private ReloadingX509KeyManager clientKeyManager;
  private ReloadingX509TrustManager clientTrustManager;

  @TempDir
  private Path temp;

  @Test
  void testMTLSOnInterScmGrpcProtocolServiceAccess() throws Exception {
    int port = PortAllocator.getFreePort();
    OzoneConfiguration conf = setupConfiguration(port);
    SCMCertificateClient
        scmCertClient = setupCertificateClientForMTLS(conf);
    InterSCMGrpcProtocolService service =
        new InterSCMGrpcProtocolService(conf, scmWith(scmCertClient));
    service.start();

    InterSCMGrpcClient client =
        new InterSCMGrpcClient("localhost", port, conf, scmCertClient);
    Path tempFile = temp.resolve(CP_FILE_NAME);
    CompletableFuture<Path> res = client.download(tempFile);
    Path downloaded = res.get();

    verifyServiceUsedItsCertAndValidatedClientCert();
    verifyClientUsedItsCertAndValidatedServerCert();
    verifyDownloadedCheckPoint(downloaded);

    client.close();
    service.stop();
  }

  private void verifyServiceUsedItsCertAndValidatedClientCert()
      throws CertificateException {
    ArgumentCaptor<X509Certificate[]> capturedCerts =
        ArgumentCaptor.forClass(X509Certificate[].class);
    verify(serverKeyManager, times(1)).getCertificateChain(any());
    verify(serverTrustManager, never()).checkServerTrusted(any(), any());
    verify(serverTrustManager, times(1))
        .checkClientTrusted(capturedCerts.capture(), any());
    assertThat(capturedCerts.getValue().length).isEqualTo(1);
    assertThat(capturedCerts.getValue()[0]).isEqualTo(clientCert);
  }

  private void verifyClientUsedItsCertAndValidatedServerCert()
      throws CertificateException {
    ArgumentCaptor<X509Certificate[]> capturedCerts =
        ArgumentCaptor.forClass(X509Certificate[].class);
    verify(clientKeyManager, times(1)).getCertificateChain(any());
    verify(clientTrustManager, times(1))
        .checkServerTrusted(capturedCerts.capture(), any());
    verify(clientTrustManager, never()).checkClientTrusted(any(), any());
    assertThat(capturedCerts.getValue().length).isEqualTo(1);
    assertThat(capturedCerts.getValue()[0]).isEqualTo(serviceCert);
  }

  private void verifyDownloadedCheckPoint(Path downloaded) throws IOException {
    try (
        TarArchiveInputStream in =
            new TarArchiveInputStream(Files.newInputStream(downloaded));
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(in, UTF_8))
    ) {
      assertThat(in.getNextTarEntry().getName()).isEqualTo(CP_FILE_NAME);
      assertThat(reader.readLine()).isEqualTo(CP_CONTENTS);
    }
  }

  private StorageContainerManager scmWith(
      SCMCertificateClient scmCertClient) throws IOException {
    StorageContainerManager scmMock = mock(StorageContainerManager.class);
    when(scmMock.getScmCertificateClient()).thenReturn(scmCertClient);
    SCMMetadataStore metadataStore = metadataStore();
    when(scmMock.getScmMetadataStore()).thenReturn(metadataStore);
    SCMHAManager haManager = scmHAManager();
    when(scmMock.getScmHAManager()).thenReturn(haManager);
    when(scmMock.getClusterId()).thenReturn("clusterId");
    return scmMock;
  }

  private SCMHAManager scmHAManager() {
    SCMHAManager hamanager = mock(SCMHAManager.class);
    doReturn(mock(SCMHADBTransactionBuffer.class))
        .when(hamanager).asSCMHADBTransactionBuffer();
    return hamanager;
  }

  private SCMMetadataStore metadataStore() throws IOException {
    SCMMetadataStore metaStoreMock = mock(SCMMetadataStore.class);
    DBStore dbStore = dbStore();
    when(metaStoreMock.getStore()).thenReturn(dbStore);
    return metaStoreMock;
  }

  private DBStore dbStore() throws IOException {
    DBStore dbStoreMock = mock(DBStore.class);
    doReturn(trInfoTable()).when(dbStoreMock).getTable(any(), (Codec<?>) any(), any());
    doReturn(checkPoint()).when(dbStoreMock).getCheckpoint(anyBoolean());
    return dbStoreMock;
  }

  private DBCheckpoint checkPoint() throws IOException {
    Path checkPointLocation = Files.createDirectory(temp.resolve("cpDir"));
    Path cpFile = Paths.get(checkPointLocation.toString(), CP_FILE_NAME);
    Files.write(cpFile, CP_CONTENTS.getBytes(UTF_8));
    DBCheckpoint checkpoint = mock(DBCheckpoint.class);
    when(checkpoint.getCheckpointLocation()).thenReturn(checkPointLocation);
    return checkpoint;
  }

  private TypedTable<String, TransactionInfo> trInfoTable()
      throws IOException {
    final TypedTable<String, TransactionInfo> tableMock = mock(TypedTable.class);
    doReturn(mock(TransactionInfo.class)).when(tableMock).get(any());
    return tableMock;
  }

  private SCMCertificateClient setupCertificateClientForMTLS(
      OzoneConfiguration conf
  ) throws Exception {
    KeyPair serviceKeys = CertificateTestUtils.aKeyPair(conf);
    KeyPair clientKeys = CertificateTestUtils.aKeyPair(conf);

    serviceCert = createSelfSignedCert(serviceKeys, "service");
    clientCert = createSelfSignedCert(clientKeys, "client");

    ReloadingX509TrustManager toSpyServerTrustManager = new ReloadingX509TrustManager(KeyStore.getDefaultType(),
        ImmutableList.of(clientCert));
    serverTrustManager = spy(toSpyServerTrustManager);
    ReloadingX509TrustManager toSpyClientTrustManager = new ReloadingX509TrustManager(KeyStore.getDefaultType(),
        ImmutableList.of(serviceCert));
    clientTrustManager = spy(toSpyClientTrustManager);
    ReloadingX509KeyManager toSpyServerKeyManager = new ReloadingX509KeyManager(KeyStore.getDefaultType(), "server",
        serviceKeys.getPrivate(), ImmutableList.of(serviceCert));
    ReloadingX509KeyManager toSpyClientKeyManager = new ReloadingX509KeyManager(KeyStore.getDefaultType(), "server",
        clientKeys.getPrivate(), ImmutableList.of(clientCert));
    clientKeyManager = spy(toSpyClientKeyManager);
    serverKeyManager = spy(toSpyServerKeyManager);

    SCMCertificateClient scmCertClient = mock(SCMCertificateClient.class);
    doReturn(serverKeyManager, clientKeyManager).when(scmCertClient).getKeyManager();
    doReturn(serverTrustManager, clientTrustManager).when(scmCertClient).getTrustManager();
    return scmCertClient;
  }

  private OzoneConfiguration setupConfiguration(int port) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, port);
    conf.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_ENABLED, true);
    return conf;
  }

}
