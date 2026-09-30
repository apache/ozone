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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
  @EnumSource(value = OzoneManagerVersion.class, names = {"GET_FILE_STATUS_REJECTS_OBS", "S3_IF_NONE_MATCH_ETAG"})
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
      if (version == OzoneManagerVersion.S3_IF_NONE_MATCH_ETAG) {
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
