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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.time.Instant;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * Test for S3 STS endpoint.
 */
public class TestS3STSEndpoint {
  private S3STSEndpoint endpoint;
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String ROLE_SESSION_NAME = "test-session";
  private static final String ROLE_USER_ARN = "arn:aws:sts::123456789012:assumed-role/test-role/" + ROLE_SESSION_NAME;

  @Mock
  private ContainerRequestContext context;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3_ADMINISTRATORS, "test-user");
    OzoneConfigurationHolder.setConfiguration(config);
    OzoneClient clientStub = spy(new OzoneClientStub());

    // Stub assumeRole to return deterministic credentials.
    ObjectStore objectStore = mock(ObjectStore.class);
    when(objectStore.assumeRole(anyString(), anyString(), anyInt(), any()))
        .thenReturn(new AssumeRoleResponseInfo(
            "ASIA1234567890123456",
            "mySecretAccessKey",
            "session-token",
            Instant.now().plusSeconds(3600).getEpochSecond(),
            "AROA1234567890123456:test-session"));
    when(clientStub.getObjectStore()).thenReturn(objectStore);

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
    Response response = endpoint.get(
        "AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null);

    assertEquals(200, response.getStatus());

    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);

    // Parse response XML and verify values
    final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    final Document doc = documentBuilder.parse(new InputSource(new StringReader(responseXml)));

    final Element root = doc.getDocumentElement();
    assertEquals("AssumeRoleResponse", root.getLocalName());

    final String accessKeyId = doc.getElementsByTagName("AccessKeyId").item(0).getTextContent();
    assertEquals("ASIA1234567890123456", accessKeyId);

    final String secretAccessKey = doc.getElementsByTagName("SecretAccessKey").item(0).getTextContent();
    assertEquals("mySecretAccessKey", secretAccessKey);

    final String sessionToken = doc.getElementsByTagName("SessionToken").item(0).getTextContent();
    assertEquals("session-token", sessionToken);

    final String arn = doc.getElementsByTagName("Arn").item(0).getTextContent();
    assertEquals(ROLE_USER_ARN, arn);
  }

  @Test
  public void testStsInvalidDuration() throws Exception {
    Response response = endpoint.get(
        "AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, -1, "2011-06-15", null);

    assertEquals(400, response.getStatus());
    String errorMessage = (String) response.getEntity();
    assertTrue(errorMessage.contains("Invalid Value: DurationSeconds"));
  }

  @Test
  public void testStsUnsupportedAction() throws Exception {
    Response response = endpoint.get(
        "UnsupportedAction", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null);

    assertEquals(400, response.getStatus());
    String errorMessage = (String) response.getEntity();
    assertTrue(errorMessage.contains("Unsupported Action"));
  }

  @Test
  public void testStsInvalidVersion() throws Exception {
    Response response = endpoint.get(
        "AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2000-01-01", null);

    assertEquals(400, response.getStatus());
    String errorMessage = (String) response.getEntity();
    assertTrue(errorMessage.contains("Invalid or missing Version parameter. Supported version is 2011-06-15."));
  }

  @Test
  public void testStsPolicyTooLarge() throws Exception {
    final String tooLargePolicy = RandomStringUtils.insecure().nextAlphanumeric(2049);

    final Response response = endpoint.get(
        "AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", tooLargePolicy);

    assertEquals(400, response.getStatus());
    final String errorMessage = (String) response.getEntity();
    assertTrue(errorMessage.contains("Policy length exceeded maximum allowed length of 2048"));
  }

  @Test
  public void testStsInvalidRoleArn() throws Exception {
    final String invalidRoleArn = "arn:awsNotValid::123456789012:role/test-role";
    final Response response = endpoint.get(
        "AssumeRole", invalidRoleArn, ROLE_SESSION_NAME, 3600, "2011-06-15", null);

    assertEquals(400, response.getStatus());
    final String errorMessage = (String) response.getEntity();
    assertTrue(
        errorMessage.contains("Invalid RoleArn: must be in the format arn:aws:iam::<account-id>:role/<role-name>"));
  }
}
