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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.slf4j.event.Level;

/**
 * Run RPC Client tests.
 */
public class TestRpcClient {
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

  @Test
  public void testNfsReadRefreshUsesIdentityAfterRenameAndRejectsRecreation() throws Exception {
    AtomicReference<OmKeyInfo> live = new AtomicReference<>(nfsKey(3, "original/file"));
    AtomicReference<Function<OmKeyInfo, OmKeyInfo>> refresh = new AtomicReference<>();
    AtomicInteger lookups = new AtomicInteger();
    OmTransport transport = new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) throws IOException {
        if (request.getCmdType() == Type.LookupNfsObject) {
          lookups.incrementAndGet();
          LookupNfsObjectRequest lookup = request.getLookupNfsObjectRequest();
          assertEquals("vol", lookup.getVolumeName());
          assertEquals("bucket", lookup.getBucketName());
          assertEquals(1, lookup.getVolumeID());
          assertEquals(2, lookup.getBucketID());
          assertEquals(3, lookup.getObjectID());
          assertEquals(LookupNfsObjectRequest.Kind.FILE, lookup.getKind());
          assertThat(lookup.getHeadOp()).isFalse();
          assertThat(lookup.getLatestVersionLocation()).isTrue();
          if (live.get() == null) {
            throw new OMException("deleted", OMException.ResultCodes.FILE_NOT_FOUND);
          }
          return OMResponse.newBuilder().setCmdType(Type.LookupNfsObject).setStatus(Status.OK)
              .setLookupNfsObjectResponse(LookupNfsObjectResponse.newBuilder()
                  .setStatus(new OzoneFileStatus(live.get(), 1024, false).getProtobuf(ClientVersion.CURRENT)))
              .build();
        }
        assertThat(request.getCmdType()).isNotIn(Type.LookupFile, Type.LookupKey, Type.GetKeyInfo);
        return super.submitRequest(request);
      }
    };
    RpcClient client = createRpcClient(transport);
    try (MockedStatic<KeyInputStream> streams = mockStatic(KeyInputStream.class)) {
      streams.when(() -> KeyInputStream.getFromOmKeyInfo(any(), any(), any(), any(), any()))
          .thenAnswer(invocation -> {
            refresh.set(invocation.getArgument(2));
            return new LengthInputStream(new ByteArrayInputStream(new byte[]{1, 2, 3}), 3);
          });
      OmKeyInfo original = live.get();
      try (LengthInputStream input = client.readNfsFile("vol", "bucket", 1, 2, 3)) {
        assertEquals(3, input.getLength());
        assertEquals(1, input.read());
      }
      live.set(nfsKey(3, "moved/renamed"));
      assertEquals("moved/renamed", refresh.get().apply(original).getKeyName());
      live.set(null);
      assertNull(refresh.get().apply(original), "A deleted object must not fall back to its former path");
      live.set(nfsKey(4, "original/file"));
      assertNull(refresh.get().apply(original), "A replacement must not refresh the deleted object's pipeline");
      assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, assertThrows(OMException.class,
          () -> client.readNfsFile("vol", "bucket", 1, 2, 3)).getResult());
      assertEquals(5, lookups.get());
    } finally {
      client.close();
    }
  }

  private static OmKeyInfo nfsKey(long objectId, String name) {
    return new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName(name)
        .setDataSize(3).setObjectID(objectId)
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE)).build();
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
}
