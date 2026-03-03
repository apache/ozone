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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
        OzoneManagerVersion.SOFTWARE_VERSION,
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
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        true),
    DEFAULT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
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
        OzoneManagerVersion.FUTURE_VERSION,
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
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.SOFTWARE_VERSION,
        OzoneManagerVersion.SOFTWARE_VERSION,
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
}
