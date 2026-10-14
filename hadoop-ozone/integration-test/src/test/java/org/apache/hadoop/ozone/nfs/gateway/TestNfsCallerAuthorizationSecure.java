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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.NfsStatus;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayGrpc;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CommitRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CommitResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CreateRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CreateResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReaddirRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReaddirResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.WriteRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.WriteResponse;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.KerberosTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Verify NFS caller mapping with service credentials issued by a real MiniKdc. */
@Tag("nfs")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestNfsCallerAuthorizationSecure extends KerberosTests {

  private String originalRules;
  private String originalRuleMechanism;
  private UserGroupInformation originalLoginUser;

  @BeforeAll
  void startKdc() throws Exception {
    initKerberos();
    UserGroupInformation.setConfiguration(getConf());
    originalLoginUser = UserGroupInformation.getLoginUser();
    originalRules = KerberosName.getRules();
    originalRuleMechanism = KerberosName.getRuleMechanism();
    KerberosName.setRules("RULE:[2:$1](scm)s/@.*// RULE:[1:$1@$0](.*@" + getRealm() + ")s/@.*//");
    KerberosName.setRuleMechanism(KerberosName.MECHANISM_HADOOP);
  }

  @AfterAll
  void stopKdc() {
    UserGroupInformation.setLoginUser(originalLoginUser);
    KerberosName.setRules(originalRules);
    KerberosName.setRuleMechanism(originalRuleMechanism);
    stopMiniKdc();
  }

  @Test
  void mapsKdcPrincipalAndEnforcesProxyUserPolicy() throws Exception {
    UserGroupInformation gatewayUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        getOzonePrincipal(), getOzoneKeytab().getAbsolutePath());
    assertEquals(KERBEROS, gatewayUser.getAuthenticationMethod());
    getConf().set("hadoop.proxyuser." + gatewayUser.getShortUserName() + ".users", "test");
    getConf().set("hadoop.proxyuser." + gatewayUser.getShortUserName() + ".hosts", "127.0.0.1");
    ProxyUsers.refreshSuperUserGroupsConfiguration(getConf());
    ImpersonationProvider provider = ProxyUsers.getDefaultImpersonationProvider();
    NfsCallerAuthorizer authorizer = new NfsCallerAuthorizer(gatewayUser, provider);
    InetAddress peer = InetAddress.getByName("127.0.0.1");

    for (Caller.Protection protection : new Caller.Protection[] {
        Caller.Protection.INTEGRITY, Caller.Protection.PRIVACY}) {
      UserGroupInformation caller = authorizer.authorizeCaller(caller("test@" + getRealm(), protection), peer);
      assertEquals("test", caller.getShortUserName());
      assertSame(gatewayUser, caller.getRealUser());
    }
    assertCode(Status.Code.PERMISSION_DENIED,
        () -> authorizer.authorizeCaller(caller("other@" + getRealm(), Caller.Protection.PRIVACY), peer));
    assertCode(Status.Code.PERMISSION_DENIED, () -> authorizer.authorizeCaller(
        caller("test@" + getRealm(), Caller.Protection.PRIVACY), InetAddress.getByName("127.0.0.2")));
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void startsSecureGatewayAndAcquiresExportLeaseOverMutualTls(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir)
      throws Exception {
    UserGroupInformation gatewayUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        getOzonePrincipal(), getOzoneKeytab().getAbsolutePath());
    UserGroupInformation.setLoginUser(gatewayUser);
    getConf().setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    getConf().set(OzoneConfigKeys.OZONE_ADMINISTRATORS, "scm");
    getConf().set("hadoop.proxyuser.scm.users", "scm");
    getConf().set("hadoop.proxyuser.scm.hosts", "127.0.0.1," + InetAddress.getLocalHost().getHostAddress());
    getConf().setBoolean(OMConfigKeys.OZONE_OM_NFS_OBJECT_INDEX_BOOTSTRAP_ENABLED, true);
    getConf().setTimeDuration(OMConfigKeys.OZONE_OM_NFS_OBJECT_INDEX_BOOTSTRAP_INTERVAL,
        20, TimeUnit.MILLISECONDS);
    OzoneManager.setTestSecureOmFlag(true);

    File kmsDir = tempDir.resolve("kms").toFile();
    assertTrue(kmsDir.mkdirs());
    String keyProviderUri = JavaKeyStoreProvider.SCHEME_NAME + "://file"
        + new File(kmsDir, "kms.jks").toURI().getPath();
    getConf().set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH, keyProviderUri);
    try (KeyProvider keyProvider = KeyProviderFactory.get(new URI(keyProviderUri), getConf())) {
      KeyProvider.Options keyOptions = KeyProvider.options(getConf());
      keyOptions.setBitLength(128);
      keyProvider.createKey("nfs-test-key", keyOptions);
      keyProvider.flush();
    }
    MiniOzoneCluster cluster = null;
    OzoneClient client = null;
    try {
      cluster = MiniOzoneCluster.newHABuilder(getConf()).setSCMServiceId("NfsSecureIT")
          .setNumOfStorageContainerManagers(3).setNumOfOzoneManagers(1).setNumDatanodes(5).build();
      cluster.waitForClusterToBeReady();
      client = cluster.newClient();
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED,
          new DefaultReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE)));
      OzoneBucket stagingBucket = DataTestUtil.createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      grantReadOnlyUser(client, bucket);
      byte[] expected = "secure NFS gateway read".getBytes(StandardCharsets.UTF_8);
      try (OzoneOutputStream output = bucket.createKey("nfs-file", expected.length)) {
        output.write(expected);
      }
      byte[] ecExpected = "secure NFS gateway EC read".getBytes(StandardCharsets.UTF_8);
      DataTestUtil.createKey(bucket, "nfs-ec-file", new ECReplicationConfig("RS-3-2-1024k"), ecExpected);
      bucket.setEncryptionKey("nfs-test-key");
      byte[] encryptedExpected = "secure NFS gateway encrypted read".getBytes(StandardCharsets.UTF_8);
      try (OzoneOutputStream output = bucket.createKey("nfs-encrypted-file", encryptedExpected.length)) {
        output.write(encryptedExpected);
      }

      OzoneManager om = cluster.getOzoneManager();
      OMMetadataManager metadata = om.getMetadataManager();
      GenericTestUtils.waitFor(() -> {
        try {
          return metadata.isNfsObjectIndexReady();
        } catch (IOException ex) {
          return false;
        }
      }, 20, 30_000);
      if (!metadata.isNfsWriterLeaseEnabled() || !metadata.isNfsDurableWriteRangesEnabled()) {
        om.finalizeUpgrade();
      }

      UUID exportId = UUID.randomUUID();
      long volumeId = metadata.getVolumeId(bucket.getVolumeName());
      long bucketId = metadata.getBucketId(bucket.getVolumeName(), bucket.getName());
      String prefix = "ozone.nfs.export.test.";
      getConf().set(prefix + "uuid", exportId.toString());
      getConf().set(prefix + "cluster.id", cluster.getClusterId());
      getConf().set(prefix + "volume", bucket.getVolumeName());
      getConf().set(prefix + "bucket", bucket.getName());
      getConf().set(prefix + "volume.id", Long.toUnsignedString(volumeId));
      getConf().set(prefix + "bucket.id", Long.toUnsignedString(bucketId));

      SecurityConfig securityConfig = new SecurityConfig(getConf());
      KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
      X509Certificate certificate = SelfSignedCertificate.newBuilder().setSubject("localhost")
          .setClusterID(cluster.getClusterId()).setScmID("nfs-gateway-it")
          .setBeginDate(ZonedDateTime.now().minusDays(1)).setEndDate(ZonedDateTime.now().plusDays(1))
          .setConfiguration(securityConfig).setKey(keyPair).addDnsName("localhost").build();
      File certificateFile = tempDir.resolve("nfs-gateway.crt").toFile();
      File privateKeyFile = tempDir.resolve("nfs-gateway.key").toFile();
      Files.write(certificateFile.toPath(), CertificateCodec.getPEMEncodedString(certificate)
          .getBytes(StandardCharsets.US_ASCII));
      Files.write(privateKeyFile.toPath(), securityConfig.keyCodec().encodePrivateKey(keyPair.getPrivate()));
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      getConf().set(NfsGatewayConfigKeys.ADDRESS, "127.0.0.1");
      getConf().setInt(NfsGatewayConfigKeys.PORT, port);
      getConf().set(NfsGatewayConfigKeys.SERVER_CERTIFICATE, certificateFile.getAbsolutePath());
      getConf().set(NfsGatewayConfigKeys.SERVER_PRIVATE_KEY, privateKeyFile.getAbsolutePath());
      getConf().set(NfsGatewayConfigKeys.TRUSTED_CA, certificateFile.getAbsolutePath());
      getConf().set(NfsGatewayConfigKeys.GANESHA_CLIENT_CERTIFICATES,
          NfsTlsPeerInterceptor.fingerprint(certificate));
      getConf().set(NfsGatewayConfigKeys.KERBEROS_PRINCIPAL, getOzonePrincipal());
      getConf().set(NfsGatewayConfigKeys.KERBEROS_KEYTAB, getOzoneKeytab().getAbsolutePath());
      getConf().set(NfsGatewayConfigKeys.GATEWAY_ID, "secure-nfs-gateway-it");
      getConf().set(NfsGatewayConfigKeys.STAGING_VOLUME, stagingBucket.getVolumeName());
      getConf().set(NfsGatewayConfigKeys.STAGING_BUCKET, stagingBucket.getName());
      getConf().setTimeDuration(NfsGatewayConfigKeys.EXPORT_LEASE_DURATION, 30, TimeUnit.SECONDS);
      getConf().setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 0);
      assertEquals(30L, getConf().getTimeDuration(NfsGatewayConfigKeys.EXPORT_LEASE_DURATION,
          30, TimeUnit.SECONDS));
      assertTrue(getConf().getConfigKeys().stream().anyMatch(key -> key.equals(prefix + "uuid")));

      OmKeyInfo keyInfo = om.lookupFile(new OmKeyArgs.Builder().setVolumeName(bucket.getVolumeName())
          .setBucketName(bucket.getName()).setKeyName("nfs-file").build());
      NfsFileHandle handle = new NfsFileHandle(exportId, volumeId, bucketId, keyInfo.getObjectID(),
          NfsFileHandle.Kind.FILE);
      ByteString exportUuid = ByteString.copyFrom(ByteBuffer.allocate(16).putLong(exportId.getMostSignificantBits())
          .putLong(exportId.getLeastSignificantBits()).array());
      RequestContext context = RequestContext.newBuilder().setVersion(1).setExportUuid(exportUuid)
          .setCaller(Caller.newBuilder().setKerberosPrincipal(getOzonePrincipal())
              .setProtection(Caller.Protection.PRIVACY)).build();

      ByteString recoveryHandle;
      try (NfsGatewayServer server = NfsGatewayDaemon.start(getConf())) {
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
            .overrideAuthority("localhost")
            .sslContext(GrpcSslContexts.forClient().keyManager(certificateFile, privateKeyFile)
                .trustManager(certificateFile).build())
            .build();
        try {
          NfsGatewayGrpc.NfsGatewayBlockingStub stub = NfsGatewayGrpc.newBlockingStub(channel)
              .withDeadlineAfter(10, TimeUnit.SECONDS);
          CapabilityResponse capabilities = stub.capabilities(CapabilityRequest.newBuilder().setVersion(1)
              .setExportUuid(exportUuid).build());
          assertEquals(NfsStatus.OK.getCode(), capabilities.getResult().getNfsStatus());
          assertTrue(capabilities.getOperationsList().contains(CapabilityResponse.Operation.CREATE));
          GetattrResponse attributes = stub.getattr(GetattrRequest.newBuilder().setContext(context)
              .setHandle(ByteString.copyFrom(handle.toBytes())).build());
          assertEquals(NfsStatus.OK.getCode(), attributes.getResult().getNfsStatus());
          assertEquals(keyInfo.getObjectID(), attributes.getAttributes().getObjectId());
          ReadResponse read = stub.read(ReadRequest.newBuilder().setContext(context)
              .setHandle(ByteString.copyFrom(handle.toBytes())).setOffset(0).setCount(expected.length).build());
          assertEquals(NfsStatus.OK.getCode(), read.getResult().getNfsStatus());
          assertEquals(ByteString.copyFrom(expected), read.getData());
          assertTrue(read.getEof());
          OmKeyInfo ecKeyInfo = om.lookupFile(new OmKeyArgs.Builder().setVolumeName(bucket.getVolumeName())
              .setBucketName(bucket.getName()).setKeyName("nfs-ec-file").build());
          NfsFileHandle ecHandle = new NfsFileHandle(exportId, volumeId, bucketId, ecKeyInfo.getObjectID(),
              NfsFileHandle.Kind.FILE);
          ReadResponse ecRead = stub.read(ReadRequest.newBuilder().setContext(context)
              .setHandle(ByteString.copyFrom(ecHandle.toBytes())).setOffset(0).setCount(ecExpected.length).build());
          assertEquals(NfsStatus.OK.getCode(), ecRead.getResult().getNfsStatus());
          assertEquals(ByteString.copyFrom(ecExpected), ecRead.getData());
          assertTrue(ecRead.getEof());
          OmKeyInfo encryptedKeyInfo = om.lookupFile(new OmKeyArgs.Builder().setVolumeName(bucket.getVolumeName())
              .setBucketName(bucket.getName()).setKeyName("nfs-encrypted-file").build());
          assertNotNull(encryptedKeyInfo.getFileEncryptionInfo());
          NfsFileHandle encryptedHandle = new NfsFileHandle(exportId, volumeId, bucketId,
              encryptedKeyInfo.getObjectID(), NfsFileHandle.Kind.FILE);
          ReadResponse encryptedRead = stub.read(ReadRequest.newBuilder().setContext(context)
              .setHandle(ByteString.copyFrom(encryptedHandle.toBytes())).setOffset(0)
              .setCount(encryptedExpected.length).build());
          assertEquals(NfsStatus.OK.getCode(), encryptedRead.getResult().getNfsStatus());
          assertEquals(ByteString.copyFrom(encryptedExpected), encryptedRead.getData());
          assertTrue(encryptedRead.getEof());

          RequestContext readerContext = context.toBuilder().setCaller(Caller.newBuilder()
              .setKerberosPrincipal("reader@" + getRealm()).setProtection(Caller.Protection.PRIVACY)).build();
          ByteString bucketHandle = ByteString.copyFrom(new NfsFileHandle(exportId, volumeId, bucketId, bucketId,
              NfsFileHandle.Kind.BUCKET).toBytes());
          ReaddirResponse listing = stub.readdir(ReaddirRequest.newBuilder().setContext(readerContext)
              .setDirectoryHandle(bucketHandle).setMaxEntries(16).build());
          assertEquals(NfsStatus.OK.getCode(), listing.getResult().getNfsStatus());
          assertTrue(listing.getEntriesList().stream().anyMatch(entry -> "nfs-file".equals(entry.getName())));
          assertSecureGatewayWrite(stub, context, readerContext, exportId, volumeId, bucketId, metadata, bucket,
              client.getObjectStore());
        } finally {
          channel.shutdownNow();
        }
      }
      getConf().set(NfsGatewayConfigKeys.GATEWAY_ID, "secure-nfs-gateway-crash-it");
      getConf().setTimeDuration(NfsGatewayConfigKeys.EXPORT_LEASE_DURATION, 10, TimeUnit.SECONDS);
      getConf().setInt(NfsGatewayConfigKeys.PORT, availablePort());
      File crashConfig = tempDir.resolve("nfs-gateway-crash.xml").toFile();
      File crashLog = tempDir.resolve("nfs-gateway-crash.log").toFile();
      Process crashedGateway = startGatewayProcess(getConf(), crashConfig, crashLog);
      ByteString bucketHandle = ByteString.copyFrom(new NfsFileHandle(exportId, volumeId, bucketId, bucketId,
          NfsFileHandle.Kind.BUCKET).toBytes());
      try {
        recoveryHandle = stageGatewayRecoveryWrite(getConf().getInt(NfsGatewayConfigKeys.PORT, 0),
            certificateFile, privateKeyFile, certificateFile, context, bucketHandle);
      } finally {
        crashedGateway.destroyForcibly();
        assertTrue(crashedGateway.waitFor(10, TimeUnit.SECONDS), "Gateway process did not terminate after kill");
      }
      waitForExportLeaseExpiry(metadata, exportId);
      getConf().set(NfsGatewayConfigKeys.GATEWAY_ID, "secure-nfs-gateway-replacement-it");
      getConf().setInt(NfsGatewayConfigKeys.PORT, availablePort());
      assertRecoveredGatewayWrite(getConf(), certificateFile, privateKeyFile, context, recoveryHandle, bucket,
          metadata);
    } finally {
      try {
        if (client != null) {
          client.close();
        }
      } finally {
        if (cluster != null) {
          cluster.shutdown();
        }
      }
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static void assertSecureGatewayWrite(NfsGatewayGrpc.NfsGatewayBlockingStub stub, RequestContext context,
      RequestContext readerContext, UUID exportId, long volumeId, long bucketId, OMMetadataManager metadata,
      OzoneBucket bucket, ObjectStore objectStore) throws Exception {
    ByteString bucketHandle = ByteString.copyFrom(new NfsFileHandle(exportId, volumeId, bucketId, bucketId,
        NfsFileHandle.Kind.BUCKET).toBytes());
    CreateResponse create = stub.create(CreateRequest.newBuilder().setContext(context)
        .setParentHandle(bucketHandle).setName("secure-nfs-write").build());
    assertEquals(NfsStatus.OK.getCode(), create.getResult().getNfsStatus());
    ByteString createdHandle = create.getAttributes().getHandle();
    NfsFileHandle createdFile = NfsFileHandle.fromBytes(createdHandle.toByteArray());
    assertEquals(create.getAttributes().getObjectId(), createdFile.getObjectId());
    OzoneObj createdObject = new OzoneObjInfo.Builder().setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName()).setKeyName("secure-nfs-write")
        .setResType(OzoneObj.ResourceType.KEY).setStoreType(OzoneObj.StoreType.OZONE).build();
    objectStore.addAcl(createdObject, OzoneAcl.of(ACLIdentityType.USER, "reader", OzoneAcl.AclScope.ACCESS,
        IAccessAuthorizer.ACLType.READ));
    OmNfsLeaseInfo fileLease = metadata.getNfsFileLeaseTable().get(metadata.getNfsFileLeaseKey(
        volumeId, bucketId, createdFile.getObjectId()));
    assertNotNull(fileLease, "NFS CREATE must retain its OM writer lease after publishing the empty file");
    byte[] writeData = "secure NFS WRITE and COMMIT".getBytes(StandardCharsets.UTF_8);
    int split = writeData.length / 2;
    WriteResponse secondHalf = stub.write(WriteRequest.newBuilder().setContext(context)
        .setHandle(createdHandle).setTransferId(create.getTransferId()).setOffset(split)
        .setData(ByteString.copyFrom(writeData, split, writeData.length - split)).build());
    assertEquals(NfsStatus.OK.getCode(), secondHalf.getResult().getNfsStatus());
    WriteResponse firstHalf = stub.write(WriteRequest.newBuilder().setContext(context)
        .setHandle(createdHandle).setTransferId(create.getTransferId()).setOffset(0)
        .setData(ByteString.copyFrom(writeData, 0, split)).build());
    assertEquals(NfsStatus.OK.getCode(), firstHalf.getResult().getNfsStatus());
    assertEquals(OMException.ResultCodes.NFS_LEASE_CONFLICT, assertThrows(OMException.class,
        () -> bucket.renameKey("secure-nfs-write", "secure-nfs-write-renamed")).getResult());
    assertEquals(OMException.ResultCodes.NFS_LEASE_CONFLICT, assertThrows(OMException.class,
        () -> bucket.deleteKey("secure-nfs-write")).getResult());
    ReadResponse readBeforeCommit = stub.read(ReadRequest.newBuilder().setContext(readerContext)
        .setHandle(createdHandle).setOffset(0).setCount(writeData.length).build());
    assertEquals(NfsStatus.OK.getCode(), readBeforeCommit.getResult().getNfsStatus());
    assertEquals(ByteString.EMPTY, readBeforeCommit.getData());
    assertTrue(readBeforeCommit.getEof());
    CommitResponse commit = stub.commit(CommitRequest.newBuilder().setContext(context)
        .setHandle(createdHandle).setFinalize(true).build());
    assertEquals(NfsStatus.OK.getCode(), commit.getResult().getNfsStatus());
    try (org.apache.hadoop.ozone.client.io.OzoneInputStream input = bucket.readKey("secure-nfs-write")) {
      byte[] published = new byte[writeData.length];
      assertEquals(writeData.length, input.read(published));
      assertArrayEquals(writeData, published);
      assertEquals(-1, input.read());
    }
    assertEquals(bucket.getKey("nfs-file").getReplicationConfig().getRequiredNodes(),
        bucket.getKey("secure-nfs-write").getReplicationConfig().getRequiredNodes());
  }

  private static ByteString stageGatewayRecoveryWrite(NfsGatewayGrpc.NfsGatewayBlockingStub stub,
      RequestContext context, ByteString bucketHandle) {
    byte[] data = "gateway replacement recovers staged data".getBytes(StandardCharsets.UTF_8);
    CreateResponse create = stub.create(CreateRequest.newBuilder().setContext(context).setParentHandle(bucketHandle)
        .setName("secure-nfs-recovery").build());
    assertEquals(NfsStatus.OK.getCode(), create.getResult().getNfsStatus());
    WriteResponse write = stub.write(WriteRequest.newBuilder().setContext(context)
        .setHandle(create.getAttributes().getHandle()).setTransferId(create.getTransferId()).setOffset(0)
        .setData(ByteString.copyFrom(data)).build());
    assertEquals(NfsStatus.OK.getCode(), write.getResult().getNfsStatus());
    return create.getAttributes().getHandle();
  }

  private ByteString stageGatewayRecoveryWrite(int port, File certificateFile, File privateKeyFile,
      File trustedCa, RequestContext context, ByteString bucketHandle) throws Exception {
    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port).overrideAuthority("localhost")
        .sslContext(GrpcSslContexts.forClient().keyManager(certificateFile, privateKeyFile)
            .trustManager(trustedCa).build()).build();
    try {
      NfsGatewayGrpc.NfsGatewayBlockingStub stub = NfsGatewayGrpc.newBlockingStub(channel)
          .withDeadlineAfter(10, TimeUnit.SECONDS);
      return stageGatewayRecoveryWrite(stub, context, bucketHandle);
    } finally {
      channel.shutdownNow();
    }
  }

  private static Process startGatewayProcess(org.apache.hadoop.hdds.conf.OzoneConfiguration conf,
      File configurationFile, File logFile) throws Exception {
    try (java.io.OutputStream output = Files.newOutputStream(configurationFile.toPath())) {
      conf.writeXml(output);
    }
    String java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath();
    String krb5Config = System.getProperty("java.security.krb5.conf");
    ProcessBuilder builder = new ProcessBuilder(java, "-Djava.security.krb5.conf=" + krb5Config, "-cp",
        System.getProperty("java.class.path"), NfsGatewayTestProcess.class.getName(),
        configurationFile.getAbsolutePath());
    builder.redirectErrorStream(true).redirectOutput(logFile);
    Process process = builder.start();
    GenericTestUtils.waitFor(() -> !process.isAlive() || readFile(logFile).contains("NFS_GATEWAY_READY"),
        100, 30_000);
    assertTrue(process.isAlive(), "NFS gateway process failed to start: " + readFile(logFile));
    assertTrue(readFile(logFile).contains("NFS_GATEWAY_READY"), "NFS gateway did not report readiness");
    return process;
  }

  private static String readFile(File file) {
    try {
      return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    } catch (IOException ex) {
      return "";
    }
  }

  private static int availablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void waitForExportLeaseExpiry(OMMetadataManager metadata, UUID exportId) throws Exception {
    String exportUuid = exportId.toString();
    GenericTestUtils.waitFor(() -> {
      try {
        OmNfsLeaseInfo lease = metadata.getNfsExportLeaseTable().get(metadata.getNfsExportLeaseKey(exportUuid));
        return lease == null || lease.getProtobuf().getExpiresAtMillis() <= System.currentTimeMillis();
      } catch (IOException ex) {
        return false;
      }
    }, 100, 10_000);
  }

  private void grantReadOnlyUser(OzoneClient client, OzoneBucket bucket) throws IOException {
    getConf().set("hadoop.proxyuser.scm.users", "scm,reader");
    OzoneVolume volume = client.getObjectStore().getVolume(bucket.getVolumeName());
    for (OzoneAcl.AclScope scope : new OzoneAcl.AclScope[] {
        OzoneAcl.AclScope.ACCESS, OzoneAcl.AclScope.DEFAULT}) {
      volume.addAcl(OzoneAcl.of(ACLIdentityType.USER, "reader", scope,
          IAccessAuthorizer.ACLType.READ, IAccessAuthorizer.ACLType.LIST));
      bucket.addAcl(OzoneAcl.of(ACLIdentityType.USER, "reader", scope,
          IAccessAuthorizer.ACLType.READ, IAccessAuthorizer.ACLType.LIST));
    }
  }

  private static void assertRecoveredGatewayWrite(org.apache.hadoop.hdds.conf.OzoneConfiguration conf,
      File certificateFile, File privateKeyFile, RequestContext context, ByteString handle, OzoneBucket bucket,
      OMMetadataManager metadata) throws Exception {
    try (NfsGatewayServer replacement = NfsGatewayDaemon.start(conf)) {
      ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", replacement.getPort())
          .overrideAuthority("localhost")
          .sslContext(GrpcSslContexts.forClient().keyManager(certificateFile, privateKeyFile)
              .trustManager(certificateFile).build())
          .build();
      try {
        NfsGatewayGrpc.NfsGatewayBlockingStub stub = NfsGatewayGrpc.newBlockingStub(channel)
            .withDeadlineAfter(10, TimeUnit.SECONDS);
        CommitResponse commit = stub.commit(CommitRequest.newBuilder().setContext(context)
            .setHandle(handle).setFinalize(true).build());
        NfsFileHandle fileHandle = NfsFileHandle.fromBytes(handle.toByteArray());
        OmNfsLeaseInfo lease = metadata.getNfsFileLeaseTable().get(metadata.getNfsFileLeaseKey(
            fileHandle.getVolumeId(), fileHandle.getBucketId(), fileHandle.getObjectId()));
        assertEquals(NfsStatus.OK.getCode(), commit.getResult().getNfsStatus(),
            "NFS commit failed with lease state " + (lease == null ? "absent" : lease.getProtobuf()));
      } finally {
        channel.shutdownNow();
      }
    }
    byte[] expected = "gateway replacement recovers staged data".getBytes(StandardCharsets.UTF_8);
    try (org.apache.hadoop.ozone.client.io.OzoneInputStream input = bucket.readKey("secure-nfs-recovery")) {
      byte[] published = new byte[expected.length];
      assertEquals(expected.length, input.read(published));
      assertArrayEquals(expected, published);
      assertEquals(-1, input.read());
    }
  }

  private static Caller caller(String principal, Caller.Protection protection) {
    return Caller.newBuilder().setKerberosPrincipal(principal).setProtection(protection).build();
  }

  private static void assertCode(Status.Code expected, org.junit.jupiter.api.function.Executable action) {
    assertEquals(expected, assertThrows(StatusRuntimeException.class, action).getStatus().getCode());
  }
}
