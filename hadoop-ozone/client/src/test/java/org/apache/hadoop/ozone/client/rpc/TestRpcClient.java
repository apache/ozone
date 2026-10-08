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

import static org.apache.hadoop.ozone.client.rpc.RpcClient.validateOmVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
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
        OzoneManagerVersion.CURRENT,
        null,
        true),
    NULL_EXPECTED_TWO_OM(
        null,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        true),
    NULL_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        null,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        true
    ),
    NULL_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        null,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        true
    ),
    NULL_EXPECTED_TWO_FUTURE_OM(
        null,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
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
        OzoneManagerVersion.CURRENT,
        null,
        true),
    DEFAULT_EXPECTED_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        null,
        true),
    DEFAULT_EXPECTED_TWO_DEFAULT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        true),
    DEFAULT_EXPECTED_TWO_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        true),
    DEFAULT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        true),

    CURRENT_EXPECTED_NO_OM(
        OzoneManagerVersion.CURRENT,
        null,
        null,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        null,
        true),
    CURRENT_EXPECTED_ONE_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        null,
        true),
    CURRENT_EXPECTED_TWO_DEFAULT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        false),
    CURRENT_EXPECTED_TWO_CURRENT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        true),
    CURRENT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
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
  public void testFutureVersionShouldNotBeAnExpectedVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> validateOmVersion(OzoneManagerVersion.FUTURE_VERSION, null));
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

  private enum KeyWriteOperation {
    CREATE, CREATE_IF_ABSENT, REWRITE_IF_MATCH,
    STREAM, STREAM_IF_ABSENT, STREAM_IF_MATCH,
    MULTIPART, MULTIPART_STREAM
  }

  static Stream<Arguments> derivedKeyPiggybackCases() {
    return Arrays.stream(KeyWriteOperation.values()).flatMap(operation -> Stream.of(
        Arguments.of(operation, OzoneManagerVersion.GET_FILE_STATUS_REJECTS_OBS.toProtoValue(), true, false),
        Arguments.of(operation, OzoneManagerVersion.GET_FILE_STATUS_REJECTS_OBS.toProtoValue(), false, false),
        Arguments.of(operation, OzoneManagerVersion.S3_DERIVED_KEY.toProtoValue(), true, true),
        Arguments.of(operation, OzoneManagerVersion.S3_DERIVED_KEY.toProtoValue(), false, false),
        Arguments.of(operation, OzoneManagerVersion.S3_DERIVED_KEY.toProtoValue() + 1, true, true),
        Arguments.of(operation, OzoneManagerVersion.S3_DERIVED_KEY.toProtoValue() + 1, false, false)));
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
                      .setOMVersion(OzoneManagerVersion.CURRENT.toProtoValue()))
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
