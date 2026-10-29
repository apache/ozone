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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.ozone.client.rpc.RpcClient.getOmVersion;
import static org.apache.hadoop.ozone.client.rpc.RpcClient.validateOmVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.storage.TestClientThreadShutdown;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.event.Level;

/**
 * Run RPC Client tests.
 */
public class TestRpcClient {
  @Test
  void ratisWriteDoesNotPreventJvmShutdown(@TempDir Path tempDir) throws Exception {
    TestClientThreadShutdown.assertExitsWithoutClosingClient(
        tempDir.resolve("ratis-write.log"), RatisWriteClient.class, "RATIS_WRITE");
  }

  /** Child JVM for the HDDS-10564 regression test, using the actual RpcClient write executor. */
  public static final class RatisWriteClient {
    public static void main(String[] args) throws Exception {
      RpcClient client = createRpcClient();
      @SuppressWarnings("unchecked")
      Supplier<ExecutorService> executor =
          (Supplier<ExecutorService>) FieldUtils.readField(client, "writeExecutor", true);
      TestClientThreadShutdown.writeRatisBlock(executor.get());
    }
  }

  private enum ValidateOmVersionTestCases {
    NULL_EXPECTED_NO_OM(
        null, // Expected version
        null, // First OM Version
        null, // Second OM Version
        true), // Should validation pass
    NULL_EXPECTED_ONE_OM(
        null,
        OzoneManagerVersion.SOFTWARE_VERSION,
        null,
        true),
    NULL_EXPECTED_TWO_OM(
        null,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        true),
    NULL_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        null,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        true
    ),
    NULL_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        null,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true
    ),
    NULL_EXPECTED_TWO_FUTURE_OM(
        null,
        OzoneManagerVersion.UNKNOWN_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true
    ),

    DEFAULT_EXPECTED_NO_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        null,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        true),
    DEFAULT_EXPECTED_ONE_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        null,
        true),
    DEFAULT_EXPECTED_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        null,
        true),
    DEFAULT_EXPECTED_TWO_DEFAULT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        true),
    DEFAULT_EXPECTED_TWO_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        true),
    DEFAULT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true),

    CURRENT_EXPECTED_NO_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        null,
        null,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        null,
        true),
    CURRENT_EXPECTED_ONE_FUTURE_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        null,
        true),
    CURRENT_EXPECTED_TWO_DEFAULT_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        false),
    CURRENT_EXPECTED_TWO_CURRENT_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        true),
    CURRENT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.UNKNOWN_VERSION,
        true),

    // An intermediate expected version is supported only by OMs at or above that version.
    INTERMEDIATE_EXPECTED_ONE_OLDER_OM(
        OzoneManagerVersion.HBASE_SUPPORT,
        OzoneManagerVersion.ATOMIC_REWRITE_KEY,
        null,
        false),
    INTERMEDIATE_EXPECTED_ONE_EXACT_OM(
        OzoneManagerVersion.HBASE_SUPPORT,
        OzoneManagerVersion.HBASE_SUPPORT,
        null,
        true),
    INTERMEDIATE_EXPECTED_ONE_NEWER_OM(
        OzoneManagerVersion.HBASE_SUPPORT,
        OzoneManagerVersion.LIGHTWEIGHT_LIST_STATUS,
        null,
        true),
    INTERMEDIATE_EXPECTED_ONE_NEWER_ONE_OLDER_OM(
        OzoneManagerVersion.HBASE_SUPPORT,
        OzoneManagerVersion.LIGHTWEIGHT_LIST_STATUS,
        OzoneManagerVersion.ATOMIC_REWRITE_KEY,
        false),
    INTERMEDIATE_EXPECTED_TWO_NEWER_OM(
        OzoneManagerVersion.HBASE_SUPPORT,
        OzoneManagerVersion.LIGHTWEIGHT_LIST_STATUS,
        OzoneManagerVersion.S3_OBJECT_TAGGING_API,
        true);

    private final OzoneManagerVersion expectedVersion;
    private final OzoneManagerVersion om1Version;
    private final OzoneManagerVersion om2Version;
    private final boolean validation;

    ValidateOmVersionTestCases(
        OzoneManagerVersion expectedVersion,
        OzoneManagerVersion om1Version,
        OzoneManagerVersion om2Version,
        boolean validation) {
      this.expectedVersion = expectedVersion;
      this.om1Version = om1Version;
      this.om2Version = om2Version;
      this.validation = validation;
    }
  }

  @ParameterizedTest
  @EnumSource(ValidateOmVersionTestCases.class)
  public void testValidateOmVersion(ValidateOmVersionTestCases testCase) {
    List<ServiceInfo> serviceInfoList = new LinkedList<>();
    ServiceInfo.Builder b1 = new ServiceInfo.Builder();
    ServiceInfo.Builder b2 = new ServiceInfo.Builder();
    b1.setNodeType(HddsProtos.NodeType.OM).setHostname("localhost");
    b2.setNodeType(HddsProtos.NodeType.OM).setHostname("localhost");
    if (testCase.om1Version != null) {
      b1.setOmVersion(testCase.om1Version);
      serviceInfoList.add(b1.build());
    }
    if (testCase.om2Version != null) {
      b2.setOmVersion(testCase.om2Version);
      serviceInfoList.add(b2.build());
    }
    assertEquals(testCase.validation,
        validateOmVersion(testCase.expectedVersion, serviceInfoList),
        "Running test " + testCase);
  }

  @Test
  public void testUnknownVersionShouldNotBeAnExpectedVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> validateOmVersion(OzoneManagerVersion.UNKNOWN_VERSION, null));
  }

  @Test
  public void testValidateOmVersionFailsWhenNoOmPresent() {
    // At least one OM must be present. An empty list should fail validation.
    assertFalse(validateOmVersion(OzoneManagerVersion.SOFTWARE_VERSION, Collections.emptyList()));
  }

  @Test
  public void testGetOmVersionWithNoOmDefaultsToSoftwareVersion() {
    // Default software version should be returned if OM list is empty.
    assertEquals(OzoneManagerVersion.SOFTWARE_VERSION, getOmVersion(serviceInfoEx()));
  }

  @Test
  public void testGetOmVersionReturnsSingleOmVersion() {
    assertEquals(OzoneManagerVersion.HBASE_SUPPORT, getOmVersion(serviceInfoEx(om(OzoneManagerVersion.HBASE_SUPPORT))));
  }

  @Test
  public void testGetOmVersionReturnsMinimumAcrossOms() {
    // The lowest version among all OMs is returned, regardless of ordering.
    assertEquals(OzoneManagerVersion.HBASE_SUPPORT, // version 7
        getOmVersion(serviceInfoEx(
            om(OzoneManagerVersion.LIGHTWEIGHT_LIST_STATUS), // version 8
            om(OzoneManagerVersion.HBASE_SUPPORT)))); // version 7
    assertEquals(OzoneManagerVersion.DEFAULT_VERSION, // version 0
        getOmVersion(serviceInfoEx(
            om(OzoneManagerVersion.DEFAULT_VERSION), // version 0
            om(OzoneManagerVersion.SOFTWARE_VERSION)))); // largest concrete version.
  }

  @Test
  public void testGetOmVersionWithFutureOM() {
    // A future (unknown) OM version must never be returned as the version to use, even if it is the only one present.
    // The client's latest known OM version should be used in this case, which should still be less than the future
    // versions.
    assertEquals(OzoneManagerVersion.SOFTWARE_VERSION,
        getOmVersion(serviceInfoEx(
            om(OzoneManagerVersion.UNKNOWN_VERSION),
            om(OzoneManagerVersion.SOFTWARE_VERSION))));
    assertEquals(OzoneManagerVersion.SOFTWARE_VERSION,
        getOmVersion(serviceInfoEx(
            om(OzoneManagerVersion.UNKNOWN_VERSION),
            om(OzoneManagerVersion.UNKNOWN_VERSION))));
  }

  @Test
  public void testCloseTwiceDoesNotWarn() throws IOException {
    RpcClient rpcClient = createRpcClient();
    GenericTestUtils.setLogLevel(RpcClient.class, Level.DEBUG);
    LogCapturer logs = LogCapturer.captureLogs(RpcClient.class);
    logs.clearOutput();

    try {
      assertDoesNotThrow(() -> {
        rpcClient.close();
        rpcClient.close();
      });

      assertThat(logs.getOutput())
          .doesNotContain("WARN")
          .doesNotContain("This metrics class is not used.");
    } finally {
      logs.stopCapturing();
    }
  }

  @ParameterizedTest
  @CsvSource({"invalid/volume, bucket1, INVALID_VOLUME_NAME", "volume1, invalid/bucket, INVALID_BUCKET_NAME"})
  public void testListMultipartUploadsValidatesNames(String volume, String bucket, ResultCodes expected)
      throws Exception {
    RpcClient client = createRpcClient();
    try {
      OMException error = assertThrows(OMException.class,
          () -> client.listMultipartUploads(volume, bucket, null, null, null, 10));
      assertThat(error.getResult()).isEqualTo(expected);
    } finally {
      client.close();
    }
  }

  /**
   * S3 Gateway serves all users with one RpcClient. A request from bob on another thread must not change the owner
   * of the key that alice creates.
   */
  @Test
  public void testKeyOwnerComesFromS3AuthOfCallingThread() throws Exception {
    List<String> owners = new ArrayList<>();
    RpcClient client = createRpcClient(new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) throws IOException {
        if (request.getCmdType() == Type.CreateKey) {
          owners.add(request.getCreateKeyRequest().getKeyArgs().getOwnerName());
        }
        return super.submitRequest(request);
      }
    });
    try {
      client.createVolume("vol1");
      client.createBucket("vol1", "bucket1");
      client.setThreadLocalS3Auth(new S3Auth("stringToSign", "signature", "alice", "alice"));
      Thread otherRequest = new Thread(
          () -> client.setThreadLocalS3Auth(new S3Auth("stringToSign", "signature", "bob", "bob")));
      otherRequest.start();
      otherRequest.join();

      ReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
      client.createKey("vol1", "bucket1", "key1", 0, replication, Collections.emptyMap()).close();
      assertThat(owners).containsExactly("alice");
    } finally {
      client.clearThreadLocalS3Auth();
      client.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class,
      names = {"GET_FILE_STATUS_REJECTS_OBS", "ZDU", "S3_VOLUME_CONTEXT_RESOLUTION"})
  void testAbortS3MultipartUpload(OzoneManagerVersion version) throws Exception {
    RpcClient client = spy(createRpcClient());
    try {
      OzoneManagerClientProtocol om = mock(OzoneManagerClientProtocol.class);
      FieldUtils.writeField(client, "omVersion", version, true);
      FieldUtils.writeField(client, "ozoneManagerClient", om, true);
      doReturn(new S3VolumeContext(OmVolumeArgs.newBuilder().setVolume("tenant-volume")
          .setOwnerName("owner").setAdminName("admin").build(), "owner")).when(client).getS3VolumeContext();
      boolean assumeContext = version == OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION;

      client.abortS3MultipartUpload("bucket", "key", "upload-id");

      ArgumentCaptor<OmKeyArgs> args = ArgumentCaptor.forClass(OmKeyArgs.class);
      if (assumeContext) {
        verify(om).abortMultipartUpload(args.capture(), eq(true));
      } else {
        verify(om).abortMultipartUpload(args.capture());
      }
      verify(client, times(assumeContext ? 0 : 1)).getS3VolumeContext();
      assertEquals(assumeContext ? "s3v" : "tenant-volume", args.getValue().getVolumeName());
      assertEquals("upload-id", args.getValue().getMultipartUploadID());
    } finally {
      client.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class, mode = EnumSource.Mode.EXCLUDE, names = "UNKNOWN_VERSION")
  public void testCreateKeyIfNotExistsRequiresSupportedOmVersion(OzoneManagerVersion version) throws IOException {
    checkCreateKeyIfNotExistsOmVersion(version, false);
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class, mode = EnumSource.Mode.EXCLUDE, names = "UNKNOWN_VERSION")
  public void testCreateStreamKeyIfNotExistsRequiresSupportedOmVersion(OzoneManagerVersion version) throws IOException {
    checkCreateKeyIfNotExistsOmVersion(version, true);
  }

  private static void checkCreateKeyIfNotExistsOmVersion(OzoneManagerVersion version, boolean streaming)
      throws IOException {
    AtomicInteger createKeyRequests = new AtomicInteger();
    IOException requestSent = new IOException("CreateKey request sent");
    OmTransport transport = new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) throws IOException {
        if (request.getCmdType() == Type.ServiceList) {
          OMResponse response = super.submitRequest(request);
          return response.toBuilder().setServiceListResponse(response.getServiceListResponse().toBuilder()
              .clearServiceInfo().addServiceInfo(om(version).getProtobuf())).build();
        }
        if (request.getCmdType() == Type.CreateKey) {
          createKeyRequests.incrementAndGet();
          assertThat(request.getCreateKeyRequest().getKeyArgs().hasExpectedDataGeneration()).isTrue();
          assertEquals(OzoneConsts.EXPECTED_GEN_CREATE_IF_ABSENT,
              request.getCreateKeyRequest().getKeyArgs().getExpectedDataGeneration());
          throw requestSent;
        }
        return super.submitRequest(request);
      }
    };
    RpcClient client = createRpcClient(transport);
    try {
      IOException error = assertThrows(IOException.class, () -> {
        if (streaming) {
          client.createStreamKeyIfNotExists("volume1", "bucket1", "key1", 0,
              RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
              Collections.emptyMap(), Collections.emptyMap());
        } else {
          client.createKeyIfNotExists("volume1", "bucket1", "key1", 0,
              RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
              Collections.emptyMap(), Collections.emptyMap());
        }
      });
      if (version.serialize() < OzoneManagerVersion.ATOMIC_CREATE_IF_NOT_EXISTS.serialize()) {
        assertEquals("OzoneManager does not support atomic key creation.", error.getMessage());
        assertEquals(0, createKeyRequests.get());
      } else {
        assertSame(requestSent, error);
        assertEquals(1, createKeyRequests.get());
      }
    } finally {
      client.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class, names = {
      "ATOMIC_CREATE_IF_NOT_EXISTS", "S3_BUCKET_TAGGING_API", "GET_FILE_STATUS_REJECTS_OBS", "UNKNOWN_VERSION"
  })
  public void testBucketTaggingVersionChecks(OzoneManagerVersion version) throws IOException {
    List<Type> requests = new LinkedList<>();
    OmTransport transport = new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) {
        OMResponse.Builder response = OMResponse.newBuilder()
            .setCmdType(request.getCmdType()).setSuccess(true).setStatus(Status.OK);
        if (request.getCmdType() == Type.ServiceList) {
          response.setServiceListResponse(ServiceListResponse.newBuilder().addServiceInfo(om(version).getProtobuf()));
        } else {
          requests.add(request.getCmdType());
        }
        return response.build();
      }
    };
    RpcClient client = createRpcClient(transport);
    try {
      List<Executable> operations = Arrays.asList(
          () -> assertThat(client.getBucketTagging("volume1", "bucket1")).isEmpty(),
          () -> client.putBucketTagging("volume1", "bucket1", Collections.singletonMap("tag", "value")),
          () -> client.deleteBucketTagging("volume1", "bucket1"));
      for (Executable operation : operations) {
        if (version == OzoneManagerVersion.ATOMIC_CREATE_IF_NOT_EXISTS) {
          IOException error = assertThrows(IOException.class, operation);
          assertThat(error).hasMessage("OzoneManager does not support S3 bucket tagging API");
        } else {
          assertDoesNotThrow(operation);
        }
      }
      if (version == OzoneManagerVersion.ATOMIC_CREATE_IF_NOT_EXISTS) {
        assertThat(requests).isEmpty();
      } else {
        assertThat(requests).containsExactly(Type.GetBucketTagging, Type.PutBucketTagging, Type.DeleteBucketTagging);
      }
    } finally {
      client.close();
    }
  }

  private enum KeyWriteOperation {
    CREATE, CREATE_IF_ABSENT, REWRITE_IF_MATCH,
    STREAM, STREAM_IF_ABSENT, STREAM_IF_MATCH,
    MULTIPART, MULTIPART_STREAM
  }

  static Stream<Arguments> derivedKeyPiggybackCases() {
    return Arrays.stream(KeyWriteOperation.values()).flatMap(operation -> Stream.of(
        Arguments.of(operation, OzoneManagerVersion.LOOKUP_FILE_REJECTS_OBS.serialize(), true, false),
        Arguments.of(operation, OzoneManagerVersion.LOOKUP_FILE_REJECTS_OBS.serialize(), false, false),
        Arguments.of(operation, OzoneManagerVersion.S3_DERIVED_KEY.serialize(), true, true),
        Arguments.of(operation, OzoneManagerVersion.S3_DERIVED_KEY.serialize(), false, false),
        Arguments.of(operation, OzoneManagerVersion.ZDU.serialize(), true, true),
        Arguments.of(operation, OzoneManagerVersion.ZDU.serialize(), false, false),
        Arguments.of(operation, OzoneManagerVersion.SOFTWARE_VERSION.serialize() + 1, true, true),
        Arguments.of(operation, OzoneManagerVersion.SOFTWARE_VERSION.serialize() + 1, false, false)));
  }

  @ParameterizedTest
  @MethodSource("derivedKeyPiggybackCases")
  void testDerivedKeyPiggybackVersionGate(KeyWriteOperation operation, int omVersion,
      boolean requested, boolean expected) throws IOException {
    AtomicReference<CreateKeyRequest> captured = new AtomicReference<>();
    OmTransport transport = new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) throws IOException {
        if (request.getCmdType() == Type.ServiceList) {
          return super.submitRequest(request).toBuilder()
              .setServiceListResponse(ServiceListResponse.newBuilder()
                  .addServiceInfo(OzoneManagerProtocolProtos.ServiceInfo.newBuilder()
                      .setNodeType(HddsProtos.NodeType.OM).setHostname("new-om")
                      .setOMVersion(OzoneManagerVersion.SOFTWARE_VERSION.serialize()))
                  .addServiceInfo(OzoneManagerProtocolProtos.ServiceInfo.newBuilder()
                      .setNodeType(HddsProtos.NodeType.OM).setHostname("other-om").setOMVersion(omVersion)))
              .build();
        }
        if (request.getCmdType() == Type.CreateKey) {
          captured.set(request.getCreateKeyRequest());
          throw new IOException("Captured CreateKey request");
        }
        return super.submitRequest(request);
      }
    };
    RpcClient client = createRpcClient(transport);
    try {
      ReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
      IOException error = assertThrows(IOException.class, () -> {
        switch (operation) {
        case CREATE:
          client.createKey("volume", "bucket", "key", 0, replication, Collections.emptyMap(), Collections.emptyMap(),
              requested);
          break;
        case CREATE_IF_ABSENT:
          client.createKeyIfNotExists("volume", "bucket", "key", 0, replication, Collections.emptyMap(),
              Collections.emptyMap(), requested);
          break;
        case REWRITE_IF_MATCH:
          client.rewriteKeyIfMatch("volume", "bucket", "key", 0, "etag", replication, Collections.emptyMap(),
              Collections.emptyMap(), requested);
          break;
        case STREAM:
          client.createStreamKey("volume", "bucket", "key", 0, replication, Collections.emptyMap(),
              Collections.emptyMap(), requested);
          break;
        case STREAM_IF_ABSENT:
          client.createStreamKeyIfNotExists("volume", "bucket", "key", 0, replication, Collections.emptyMap(),
              Collections.emptyMap(), requested);
          break;
        case STREAM_IF_MATCH:
          client.rewriteStreamKeyIfMatch("volume", "bucket", "key", 0, "etag", replication, Collections.emptyMap(),
              Collections.emptyMap(), requested);
          break;
        case MULTIPART:
          client.createMultipartKey("volume", "bucket", "key", 0, 1, "upload", requested);
          break;
        case MULTIPART_STREAM:
          client.createMultipartStreamKey("volume", "bucket", "key", 0, 1, "upload", requested);
          break;
        default:
          throw new IllegalArgumentException("Unexpected operation: " + operation);
        }
      });
      assertThat(error).hasMessage("Captured CreateKey request");
      assertThat(captured.get()).isNotNull();
      assertThat(captured.get().getDerivedKeyPiggyBacking()).isEqualTo(expected);
    } finally {
      client.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class, mode = EnumSource.Mode.EXCLUDE, names = "UNKNOWN_VERSION")
  public void testIfNoneMatchETagReachesOMOrRejectsOldVersion(OzoneManagerVersion version) throws IOException {
    List<KeyArgs> requests = new ArrayList<>();
    OmTransport transport = new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) throws IOException {
        if (request.getCmdType() == Type.ServiceList) {
          return OMResponse.newBuilder().setCmdType(Type.ServiceList)
              .setStatus(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK)
              .setSuccess(true).setServiceListResponse(ServiceListResponse.newBuilder().addServiceInfo(
                  new ServiceInfo.Builder().setNodeType(HddsProtos.NodeType.OM).setHostname("localhost")
                      .setOmVersion(version).build().getProtobuf())).build();
        }
        if (request.getCmdType() == Type.CreateKey) {
          requests.add(request.getCreateKeyRequest().getKeyArgs());
          throw new IOException("captured");
        }
        if (request.getCmdType() == Type.CompleteMultiPartUpload) {
          requests.add(request.getCompleteMultiPartUploadRequest().getKeyArgs());
          throw new IOException("captured");
        }
        return super.submitRequest(request);
      }
    };
    RpcClient client = createRpcClient(transport);
    try {
      RatisReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
      IOException putError = assertThrows(IOException.class, () -> client.createKeyIfNoneMatch("vol", "bucket", "key",
          1L, "excluded-etag", replication, Collections.emptyMap(), Collections.emptyMap(), false));
      IOException streamError = assertThrows(IOException.class, () -> client.createStreamKeyIfNoneMatch("vol", "bucket",
          "key", 1L, "excluded-etag", replication, Collections.emptyMap(), Collections.emptyMap(), false));
      IOException completeError = assertThrows(IOException.class, () -> client.completeMultipartUpload("vol", "bucket",
          "key", "upload-id", Collections.singletonMap(1, "part-etag"), null, null, "excluded-etag"));
      if (OzoneManagerVersion.S3_IF_NONE_MATCH_ETAG.isSupportedBy(version)) {
        assertEquals("captured", putError.getMessage());
        assertEquals("captured", streamError.getMessage());
        assertEquals("captured", completeError.getMessage());
        assertThat(requests).hasSize(3).allSatisfy(args -> assertEquals("excluded-etag", args.getExcludedETag()));
      } else {
        assertThat(requests).isEmpty();
        assertThat(putError.getMessage()).contains("does not support If-None-Match");
        assertThat(streamError.getMessage()).contains("does not support If-None-Match");
        assertThat(completeError.getMessage()).contains("does not support If-None-Match");
      }
    } finally {
      client.close();
    }
  }

  private static ServiceInfo om(OzoneManagerVersion version) {
    return new ServiceInfo.Builder()
        .setNodeType(HddsProtos.NodeType.OM)
        .setHostname("localhost")
        .setOmVersion(version)
        .build();
  }

  private static ServiceInfoEx serviceInfoEx(ServiceInfo... serviceInfos) {
    return new ServiceInfoEx(Arrays.asList(serviceInfos), null, null);
  }

  private static RpcClient createRpcClient() throws IOException {
    return createRpcClient(new MockOmTransport());
  }

  private static RpcClient createRpcClient(OmTransport transport) throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    return new RpcClient(config, null) {
      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return transport;
      }

      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    };
  }

  /**
   * A task wrapped by withSubmitterContext sees the submitting thread's values of every given thread-local, or none
   * where it has none, and the running thread's own values are restored afterwards, also when the task throws.
   */
  @Test
  public void testWithSubmitterContextPropagatesAndRestores() {
    ThreadLocal<S3Auth> auth = new ThreadLocal<>();
    ThreadLocal<String> hint = new ThreadLocal<>();
    List<ThreadLocal<?>> context = Arrays.asList(auth, hint);
    S3Auth submitterAuth = new S3Auth("stringToSign", "signature", "submitter", "submitterUser");
    S3Auth workerAuth = new S3Auth("stringToSign", "signature", "worker", "workerUser");
    List<List<Object>> seen = new LinkedList<>();
    Runnable observe = () -> seen.add(Arrays.asList(auth.get(), hint.get()));

    auth.set(submitterAuth);
    hint.set("submitterHint");
    Runnable withSubmitter = RpcClient.withSubmitterContext(observe, context);
    hint.remove();
    Runnable withAuthOnly = RpcClient.withSubmitterContext(observe, context);
    auth.remove();
    Runnable withNothing = RpcClient.withSubmitterContext(observe, context);

    // the running thread (a pool worker) holds different values
    auth.set(workerAuth);
    hint.set("workerHint");
    for (Runnable task : Arrays.asList(withSubmitter, withAuthOnly, withNothing)) {
      task.run();
      assertSame(workerAuth, auth.get());
      assertEquals("workerHint", hint.get());
    }
    assertEquals(Arrays.asList(Arrays.asList(submitterAuth, "submitterHint"), Arrays.asList(submitterAuth, null),
        Arrays.asList(null, null)), seen);

    // a running thread without values is left without them
    auth.remove();
    hint.remove();
    withSubmitter.run();
    assertNull(auth.get());
    assertNull(hint.get());

    auth.set(workerAuth);
    hint.set("workerHint");
    Runnable failing = RpcClient.withSubmitterContext(() -> {
      throw new IllegalStateException("task failed");
    }, context);
    assertThrows(IllegalStateException.class, failing::run);
    assertSame(workerAuth, auth.get());
    assertEquals("workerHint", hint.get());
  }
}
