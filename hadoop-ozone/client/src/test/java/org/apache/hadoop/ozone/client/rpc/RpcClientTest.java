/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;


import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_CLIENT_PROTOCOL_VERSION;
import static org.apache.hadoop.ozone.client.rpc.RpcClient.validateOmVersion;

/**
 * Run RPC Client tests.
 */
public class RpcClientTest {
  private enum ValidateOmVersionTestCases {
    NULL_EXPECTED_NO_OM(
        null, // Expected version
        null, // First OM Version
        null, // Second OM Version
        true), // Should validation pass
    NULL_EXPECTED_ONE_OM(
        null,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        null,
        true),
    NULL_EXPECTED_TWO_OM(
        null,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        true),
    NULL_EXPECTED_TWO_OM_SECOND_MISMATCH(
        null,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "invalid",
        true),
    NULL_EXPECTED_TWO_OM_BOTH_MISMATCH(
        null,
        "invalid",
        "invalid",
        true),
    NULL_EXPECTED_TWO_OM_FIRST_MISMATCH(
        null,
        "invalid",
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        true),
    EMPTY_EXPECTED_NO_OM(
        "",
        null,
        null,
        true),
    EMPTY_EXPECTED_ONE_OM(
        "",
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        null,
        true),
    EMPTY_EXPECTED_TWO_OM(
        "",
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        true),
    EMPTY_EXPECTED_TWO_OM_MISMATCH(
        "",
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "invalid",
        true),
    EMPTY_EXPECTED_TWO_OM_BOTH_MISMATCH(
        "",
        "invalid",
        "invalid",
        true),
    VALID_EXPECTED_NO_OM(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        null,
        null,
        false),
    VALID_EXPECTED_ONE_OM(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        null,
        true),
    VALID_EXPECTED_TWO_OM(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        true),
    VALID_EXPECTED_TWO_OM_SECOND_LOWER(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        false),
    VALID_EXPECTED_TWO_OM_SECOND_HIGHER(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "100.0.1",
        true),
    VALID_EXPECTED_TWO_OM_FIRST_OM_LOWER(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        false),
    VALID_EXPECTED_TWO_OM_FIRST_OM_HIGHER(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "10.0.1",
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        true),
    VALID_EXPECTED_TWO_OM_BOTH_LOWER(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        "1.9.1",
        false),
    VALID_EXPECTED_ONE_OM_HIGHER_VERSION(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "100.0.1",
        null,
        true),
    VALID_EXPECTED_TWO_OM_HIGHER_VERSION(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "100.0.1",
        "100.0.1",
        true),
    VALID_EXPECTED_ONE_OM_LOWER_VERSION(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        null,
        false),
    VALID_EXPECTED_TWO_OM_LOWER_VERSION(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        "1.0.1",
        false),
    VALID_EXPECTED_TWO_OM_ONE_LOWER_VERSION(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        "2.0.1",
        false),
    VALID_EXPECTED_TWO_OM_SECOND_EMPTY(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "1.0.1",
        "",
        false),
    VALID_EXPECTED_TWO_OM_FIRST_EMPTY(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "",
        "1.0.1",
        false),
    VALID_EXPECTED_TWO_OM_BOTH_EMPTY(
        OZONE_OM_CLIENT_PROTOCOL_VERSION,
        "",
        "",
        false),;

    private static final List<ValidateOmVersionTestCases>
        TEST_CASES = new LinkedList<>();

    static {
      for (ValidateOmVersionTestCases t : values()) {
        TEST_CASES.add(t);
      }
    }
    private final String expectedVersion;
    private final String om1Version;
    private final String om2Version;
    private final boolean validation;
    ValidateOmVersionTestCases(String expectedVersion,
                               String om1Version,
                               String om2Version,
                               boolean validation) {
      this.expectedVersion = expectedVersion;
      this.om1Version = om1Version;
      this.om2Version = om2Version;
      this.validation = validation;
    }

  }
  @Test
  public void testValidateOmVersion() {
    for (ValidateOmVersionTestCases t: ValidateOmVersionTestCases.TEST_CASES) {
      List<ServiceInfo> serviceInfoList = new LinkedList<>();
      ServiceInfo.Builder b1 = new ServiceInfo.Builder();
      ServiceInfo.Builder b2 = new ServiceInfo.Builder();
      b1.setNodeType(HddsProtos.NodeType.OM).setHostname("localhost");
      b2.setNodeType(HddsProtos.NodeType.OM).setHostname("localhost");
      if (t.om1Version != null) {
        b1.setOmClientProtocolVersion(t.om1Version);
        serviceInfoList.add(b1.build());
      }
      if (t.om2Version != null) {
        b2.setOmClientProtocolVersion(t.om2Version);
        serviceInfoList.add(b2.build());
      }
      Assert.assertEquals("Running test " + t, t.validation,
          validateOmVersion(t.expectedVersion, serviceInfoList));
    }
  }
}