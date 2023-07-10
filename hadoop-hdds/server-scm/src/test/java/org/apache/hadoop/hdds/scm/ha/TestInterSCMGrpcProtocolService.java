/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509ExtensionUtils;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcDigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This test checks that mTLS authentication is turned on for
 * {@link InterSCMGrpcProtocolService}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HDDS-8901">HDDS-8901</a>
 */
public class TestInterSCMGrpcProtocolService {

  private static final String CP_FILE_NAME = "cpFile";
  private static final String CP_CONTENTS = "Hello world!";

  private X509Certificate serviceCert;
  private X509Certificate clientCert;

  private X509KeyManager serverKeyManager;
  private X509TrustManager serverTrustManager;
  private X509KeyManager clientKeyManager;
  private X509TrustManager clientTrustManager;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testMTLSOnInterScmGrpcProtocolServiceAccess() throws Exception {
    int port = new Random().nextInt(1000) + 45000;
    OzoneConfiguration conf = setupConfiguration(port);
    SCMCertificateClient
        scmCertClient = setupCertificateClientForMTLS(conf);
    InterSCMGrpcProtocolService service =
        new InterSCMGrpcProtocolService(conf, scmWith(scmCertClient));
    service.start();

    InterSCMGrpcClient client =
        new InterSCMGrpcClient("localhost", port, conf, scmCertClient);
    CompletableFuture<Path> res = client.download(temp.newFile().toPath());
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
    assertThat(capturedCerts.getValue().length, is(1));
    assertThat(capturedCerts.getValue()[0], is(clientCert));
  }

  private void verifyClientUsedItsCertAndValidatedServerCert()
      throws CertificateException {
    ArgumentCaptor<X509Certificate[]> capturedCerts =
        ArgumentCaptor.forClass(X509Certificate[].class);
    verify(clientKeyManager, times(1)).getCertificateChain(any());
    verify(clientTrustManager, times(1))
        .checkServerTrusted(capturedCerts.capture(), any());
    verify(clientTrustManager, never()).checkClientTrusted(any(), any());
    assertThat(capturedCerts.getValue().length, is(1));
    assertThat(capturedCerts.getValue()[0], is(serviceCert));
  }

  private void verifyDownloadedCheckPoint(Path downloaded) throws IOException {
    try (
        TarArchiveInputStream in =
            new TarArchiveInputStream(Files.newInputStream(downloaded));
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(in, UTF_8))
    ) {
      assertThat(in.getNextTarEntry().getName(), is(CP_FILE_NAME));
      assertThat(reader.readLine(), is(CP_CONTENTS));
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
    doReturn(trInfoTable()).when(dbStoreMock).getTable(any(), any(), any());
    doReturn(checkPoint()).when(dbStoreMock).getCheckpoint(anyBoolean());
    return dbStoreMock;
  }

  private DBCheckpoint checkPoint() throws IOException {
    Path checkPointLocation = temp.newFolder().toPath();
    Path cpFile = Paths.get(checkPointLocation.toString(), CP_FILE_NAME);
    Files.write(cpFile, CP_CONTENTS.getBytes(UTF_8));
    DBCheckpoint checkpoint = mock(DBCheckpoint.class);
    when(checkpoint.getCheckpointLocation()).thenReturn(checkPointLocation);
    return checkpoint;
  }

  private Table<String, TransactionInfo> trInfoTable()
      throws IOException {
    Table<String, TransactionInfo> tableMock = mock(Table.class);
    doReturn(mock(TransactionInfo.class)).when(tableMock).get(any());
    return tableMock;
  }

  private SCMCertificateClient setupCertificateClientForMTLS(
      OzoneConfiguration conf
  ) throws Exception {
    KeyPair serviceKeys = aKeyPair(conf);
    KeyPair clientKeys = aKeyPair(conf);

    serviceCert = createSelfSignedCert(serviceKeys, "service");
    clientCert = createSelfSignedCert(clientKeys, "client");

    serverKeyManager = aKeyManagerWith(serviceKeys, serviceCert);
    serverTrustManager = aTrustManagerThatTrusts(clientCert);
    KeyStoresFactory serverKeyStores =
        aKeyStoresFactoryWith(serverKeyManager, serverTrustManager);

    clientKeyManager = aKeyManagerWith(clientKeys, clientCert);
    clientTrustManager = aTrustManagerThatTrusts(serviceCert);
    KeyStoresFactory clientKeyStores =
        aKeyStoresFactoryWith(clientKeyManager, clientTrustManager);

    SCMCertificateClient scmCertClient = mock(SCMCertificateClient.class);
    doReturn(serverKeyStores).when(scmCertClient).getServerKeyStoresFactory();
    doReturn(clientKeyStores).when(scmCertClient).getClientKeyStoresFactory();
    return scmCertClient;
  }

  private KeyStoresFactory aKeyStoresFactoryWith(
      X509KeyManager keyManager,
      X509TrustManager trustManager
  ) {
    KeyStoresFactory serverKeyStores = mock(KeyStoresFactory.class);
    doReturn(new KeyManager[]{keyManager})
        .when(serverKeyStores).getKeyManagers();
    doReturn(new TrustManager[]{trustManager})
        .when(serverKeyStores).getTrustManagers();
    return serverKeyStores;
  }

  private X509TrustManager aTrustManagerThatTrusts(X509Certificate certificate)
      throws CertificateException {
    X509TrustManager trustManager = mock(X509TrustManager.class);
    doNothing().when(trustManager).checkServerTrusted(any(), any());
    doNothing().when(trustManager).checkClientTrusted(any(), any());
    doReturn(new X509Certificate[] {certificate})
        .when(trustManager).getAcceptedIssuers();
    return trustManager;
  }

  private X509KeyManager aKeyManagerWith(KeyPair keyPair,
      X509Certificate certificate) {
    X509KeyManager keyManager = mock(X509KeyManager.class);
    doReturn("server")
        .when(keyManager).chooseServerAlias(any(), any(), any());
    doReturn("client")
        .when(keyManager).chooseClientAlias(any(), any(), any());
    doReturn(new String[] {"server"})
        .when(keyManager).getServerAliases(any(), any());
    doReturn(new String[] {"client"})
        .when(keyManager).getClientAliases(any(), any());
    doReturn(new X509Certificate[] {certificate})
        .when(keyManager).getCertificateChain(any());
    doReturn(keyPair.getPrivate())
        .when(keyManager).getPrivateKey(any());
    return keyManager;
  }

  private KeyPair aKeyPair(ConfigurationSource conf)
      throws NoSuchProviderException, NoSuchAlgorithmException {
    return new HDDSKeyGenerator(new SecurityConfig(conf)).generateKey();
  }

  private OzoneConfiguration setupConfiguration(int port) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, port);
    conf.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_ENABLED, true);
    return conf;
  }


  private static final String HASH_ALGO = "SHA256WithRSA";

  private X509Certificate createSelfSignedCert(KeyPair keys, String commonName)
      throws Exception {
    final Instant now = Instant.now();
    final Date notBefore = Date.from(now);
    final Date notAfter = Date.from(now.plus(Duration.ofDays(1)));
    final ContentSigner contentSigner =
        new JcaContentSignerBuilder(HASH_ALGO).build(keys.getPrivate());
    final X500Name x500Name = new X500Name("CN=" + commonName);

    SubjectKeyIdentifier keyId = subjectKeyIdOf(keys);
    AuthorityKeyIdentifier authorityKeyId = authorityKeyIdOf(keys);
    BasicConstraints constraints = new BasicConstraints(true);

    final X509v3CertificateBuilder certificateBuilder =
        new JcaX509v3CertificateBuilder(
            x500Name,
            BigInteger.valueOf(keys.getPublic().hashCode()),
            notBefore,
            notAfter,
            x500Name,
            keys.getPublic()
        );
    certificateBuilder
        .addExtension(Extension.subjectKeyIdentifier, false, keyId)
        .addExtension(Extension.authorityKeyIdentifier, false, authorityKeyId)
        .addExtension(Extension.basicConstraints, true, constraints);

    return new JcaX509CertificateConverter()
        .setProvider(new BouncyCastleProvider())
        .getCertificate(certificateBuilder.build(contentSigner));
  }

  private SubjectKeyIdentifier subjectKeyIdOf(KeyPair keys) throws Exception {
    return extensionUtil().createSubjectKeyIdentifier(pubKeyInfo(keys));
  }

  private AuthorityKeyIdentifier authorityKeyIdOf(KeyPair keys)
      throws Exception {
    return extensionUtil().createAuthorityKeyIdentifier(pubKeyInfo(keys));
  }

  private SubjectPublicKeyInfo pubKeyInfo(KeyPair keys) {
    return SubjectPublicKeyInfo.getInstance(keys.getPublic().getEncoded());
  }

  private X509ExtensionUtils extensionUtil()
      throws OperatorCreationException {
    DigestCalculator digest =
        new BcDigestCalculatorProvider()
            .get(new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));

    return new X509ExtensionUtils(digest);
  }

}
