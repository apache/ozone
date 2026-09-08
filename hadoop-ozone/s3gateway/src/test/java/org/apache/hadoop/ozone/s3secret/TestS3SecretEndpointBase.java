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

package org.apache.hadoop.ozone.s3secret;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransportFactory;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;

/**
 * Tests OM transport selection for the S3 secret endpoints.
 */
class TestS3SecretEndpointBase {

  @Test
  void testSecretOpsUseRpcTransportInSecureMode() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    // Simulate the S3 Gateway pinning the gRPC transport for its data client.
    conf.set(OZONE_OM_TRANSPORT_CLASS, GrpcOmTransportFactory.class.getName());

    LogCapturer logs = LogCapturer.captureLogs(S3SecretEndpointBase.class);
    S3SecretManagementEndpoint endpoint =
        new S3SecretManagementEndpoint(conf);

    // The gRPC S3 endpoint has no client authentication, so in secure mode
    // secret ops must fall back to the Kerberos-authenticated RPC transport
    // where OM can authorize the real caller.
    assertEquals(OZONE_OM_TRANSPORT_CLASS_DEFAULT,
        endpoint.getConf().get(OZONE_OM_TRANSPORT_CLASS));
    // The override is not silent: it warns that only this endpoint changed.
    assertThat(logs.getOutput()).contains("Overriding OM transport");
  }

  @Test
  void testTransportUnchangedWhenSecurityDisabled() {
    OzoneConfiguration conf = new OzoneConfiguration();
    String grpcTransport = GrpcOmTransportFactory.class.getName();
    conf.set(OZONE_OM_TRANSPORT_CLASS, grpcTransport);

    S3SecretManagementEndpoint endpoint =
        new S3SecretManagementEndpoint(conf);

    // Non-secure mode is outside the security model; leave the transport as-is.
    assertEquals(grpcTransport,
        endpoint.getConf().get(OZONE_OM_TRANSPORT_CLASS));
  }
}
