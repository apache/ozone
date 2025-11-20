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

package org.apache.hadoop.ozone.s3sts;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Test for S3 STS endpoint.
 */
public class TestSTS {
  private S3STSEndpoint endpoint;

  @Mock
  private ContainerRequestContext context;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3_ADMINISTRATORS, "test-user");
    OzoneConfigurationHolder.setConfiguration(config);
    OzoneClient clientStub = new OzoneClientStub();
    endpoint = new S3STSEndpoint();
    endpoint.setClient(clientStub);
    endpoint.setContext(context);
    SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setAwsAccessId("test-user")
        .setSignature("some-signature")
        .setStringToSign("dummy-string")
        .build();
    endpoint.setSignatureInfo(signatureInfo);
  }

  @Test
  public void testStsAssumeRole() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";

    Response response = endpoint.get(
        "AssumeRole", roleArn, roleSessionName, 3600, "2011-06-15");

    assertEquals(200, response.getStatus());

    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("AssumeRoleResponse"));
    assertTrue(responseXml.contains("AccessKeyId"));
    assertTrue(responseXml.contains("SecretAccessKey"));
    assertTrue(responseXml.contains("SessionToken"));
    assertTrue(responseXml.contains("AssumedRoleUser"));
    assertTrue(responseXml.contains(roleArn));
  }

  @Test
  public void testStsInvalidDuration() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";

    Response response = endpoint.get(
        "AssumeRole", roleArn, roleSessionName, -1, "2011-06-15");

    assertEquals(400, response.getStatus());
    String errorMessage = (String) response.getEntity();
    assertTrue(errorMessage.contains("Invalid Value: DurationSeconds"));
  }

  @Test
  public void testStsUnsupportedAction() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";

    Response response = endpoint.get(
        "UnsupportedAction", roleArn, roleSessionName, 3600, "2011-06-15");

    assertEquals(400, response.getStatus());
    String errorMessage = (String) response.getEntity();
    assertTrue(errorMessage.contains("Unsupported Action"));
  }
}
