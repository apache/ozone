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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class for STS (Security Token Service) endpoint.
 */
public class TestSTSEndpoint {

  private OzoneClient clientStub;
  private STSEndpoint stsEndpoint;
  private HttpHeaders httpHeaders;

  @BeforeEach
  public void setup() throws Exception {
    // Create client stub and object store stub
    clientStub = new OzoneClientStub();
    
    // Create mock HttpHeaders
    httpHeaders = Mockito.mock(HttpHeaders.class);

    // Create STS endpoint and set client to OzoneClientStub
    stsEndpoint = EndpointBuilder.newSTSEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  @Test
  public void testGetSessionToken() throws Exception {
    Response response = stsEndpoint.handleSTSGet(
        "GetSessionToken", null, null, null, "2011-06-15", httpHeaders);
    
    assertEquals(200, response.getStatus());
    
    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("GetSessionTokenResponse"));
    assertTrue(responseXml.contains("AccessKeyId"));
    assertTrue(responseXml.contains("SecretAccessKey"));
    assertTrue(responseXml.contains("SessionToken"));
    assertTrue(responseXml.contains("Expiration"));
  }

  @Test
  public void testAssumeRole() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    
    Response response = stsEndpoint.handleSTSGet(
        "AssumeRole", roleArn, roleSessionName, 3600, "2011-06-15", httpHeaders);
    
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
  public void testAssumeRoleWithSAML() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/saml-role";
    
    Response response = stsEndpoint.handleSTSGet(
        "AssumeRoleWithSAML", roleArn, null, 3600, "2011-06-15", httpHeaders);
    
    assertEquals(200, response.getStatus());
    
    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("AssumeRoleResponse"));
    assertTrue(responseXml.contains("AccessKeyId"));
    assertTrue(responseXml.contains("SecretAccessKey"));
    assertTrue(responseXml.contains("SessionToken"));
    assertTrue(responseXml.contains(roleArn));
  }

  @Test
  public void testAssumeRoleWithWebIdentity() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/web-identity-role";
    
    Response response = stsEndpoint.handleSTSGet(
        "AssumeRoleWithWebIdentity", roleArn, null, 3600, "2011-06-15", httpHeaders);
    
    assertEquals(200, response.getStatus());
    
    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("AssumeRoleResponse"));
    assertTrue(responseXml.contains("AccessKeyId"));
    assertTrue(responseXml.contains("SecretAccessKey"));
    assertTrue(responseXml.contains("SessionToken"));
    assertTrue(responseXml.contains(roleArn));
  }

  @Test
  public void testCustomDuration() throws Exception {
    int customDuration = 7200; // 2 hours
    
    Response response = stsEndpoint.handleSTSGet(
        "GetSessionToken", null, null, customDuration, "2011-06-15", httpHeaders);
    
    assertEquals(200, response.getStatus());
    
    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("GetSessionTokenResponse"));
    assertTrue(responseXml.contains("Expiration"));
  }

  @Test
  public void testInvalidAction() throws Exception {
    // Should throw an exception for unsupported action
    assertThrows(OS3Exception.class, () -> {
      stsEndpoint.handleSTSGet(
          "InvalidAction", null, null, null, "2011-06-15", httpHeaders);
    });
  }

  @Test
  public void testAssumeRoleWithoutRoleArn() throws Exception {
    // Should throw an exception for missing RoleArn
    assertThrows(OS3Exception.class, () -> {
      stsEndpoint.handleSTSGet(
          "AssumeRole", null, "test-session", null, "2011-06-15", httpHeaders);
    });
  }

  @Test
  public void testAssumeRoleWithoutRoleSessionName() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    
    // Should throw an exception for missing RoleSessionName
    assertThrows(OS3Exception.class, () -> {
      stsEndpoint.handleSTSGet(
          "AssumeRole", roleArn, null, null, "2011-06-15", httpHeaders);
    });
  }

  @Test
  public void testInvalidDuration() throws Exception {
    int invalidDuration = 100; // Too short (minimum is 900 seconds)
    
    // Should throw an exception for invalid duration
    assertThrows(OS3Exception.class, () -> {
      stsEndpoint.handleSTSGet(
          "GetSessionToken", null, null, invalidDuration, "2011-06-15", httpHeaders);
    });
  }

  @Test
  public void testPOSTRequest() throws Exception {
    Response response = stsEndpoint.handleSTSPost(
        "GetSessionToken", null, null, null, "2011-06-15", httpHeaders);
    
    assertEquals(200, response.getStatus());
    
    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("GetSessionTokenResponse"));
  }

  @Test
  public void testDefaultAction() throws Exception {
    // Test with null action - should default to GetSessionToken
    Response response = stsEndpoint.handleSTSGet(
        null, null, null, null, "2011-06-15", httpHeaders);
    
    assertEquals(200, response.getStatus());
    
    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("GetSessionTokenResponse"));
  }

  @Test
  public void testInvalidVersion() throws Exception {
    // Should throw an exception for unsupported version
    assertThrows(OS3Exception.class, () -> {
      stsEndpoint.handleSTSGet(
          "GetSessionToken", null, null, null, "2020-01-01", httpHeaders);
    });
  }
}