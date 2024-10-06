/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.s3secret;

import java.io.IOException;
import java.security.Principal;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.notNull;
import static org.mockito.Mockito.when;

/**
 * Test for S3 secret generate endpoint.
 */
@ExtendWith(MockitoExtension.class)
class TestSecretGenerate {
  private static final String ADMIN_USER_NAME = "test_admin";
  private static final String USER_NAME = "test";
  private static final String OTHER_USER_NAME = "test2";
  private static final String USER_SECRET = "test_secret";

  private S3SecretManagementEndpoint endpoint;

  @Mock
  private ClientProtocol proxy;
  @Mock
  private ContainerRequestContext context;
  @Mock
  private UriInfo uriInfo;
  @Mock
  private SecurityContext securityContext;
  @Mock
  private Principal principal;

  private static S3SecretValue getS3SecretValue(InvocationOnMock invocation) {
    Object[] args = invocation.getArguments();
    return S3SecretValue.of((String) args[0], USER_SECRET);
  }

  @BeforeEach
  void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_ADMINISTRATORS, ADMIN_USER_NAME);
    OzoneClient client = new OzoneClientStub(new ObjectStoreStub(conf, proxy));

    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(context.getUriInfo()).thenReturn(uriInfo);

    endpoint = new S3SecretManagementEndpoint();
    endpoint.setClient(client);
    endpoint.setContext(context);
  }

  @Test
  void testUnauthorizedSecretGeneration() throws IOException {
    setupSecurityContext(false);
    hasNoSecretYet();

    Response response = endpoint.generate();
    assertEquals(FORBIDDEN.getStatusCode(), response.getStatus());
  }

  @Test
  void testAuthorizedSecretGeneration() throws IOException {
    setupSecurityContext(true);
    hasNoSecretYet();

    S3SecretResponse response =
        (S3SecretResponse) endpoint.generate().getEntity();

    assertEquals(USER_SECRET, response.getAwsSecret());
    assertEquals(ADMIN_USER_NAME, response.getAwsAccessKey());
  }

  @Test
  void testUnauthorizedUserSecretAlreadyExists() throws IOException {
    setupSecurityContext(false);
    hasSecretAlready();

    Response response = endpoint.generate();

    assertEquals(FORBIDDEN.getStatusCode(), response.getStatus());
  }

  @Test
  void testAuthorizedUserSecretAlreadyExists() throws IOException {
    setupSecurityContext(true);
    hasSecretAlready();

    Response response = endpoint.generate();

    assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals(OMException.ResultCodes.S3_SECRET_ALREADY_EXISTS.toString(),
        response.getStatusInfo().getReasonPhrase());
  }

  @Test
  @Unhealthy("HDDS-11041")
  void testSecretGenerateWithUsername() throws IOException {
    hasNoSecretYet();

    S3SecretResponse response =
            (S3SecretResponse) endpoint.generate(OTHER_USER_NAME).getEntity();
    assertEquals(USER_SECRET, response.getAwsSecret());
    assertEquals(OTHER_USER_NAME, response.getAwsAccessKey());
  }

  /**
   * Provides mocking for {@link ContainerRequestContext}.
   * @param isAdmin Stores whether the user to mock is an admin or not
   */
  private void setupSecurityContext(boolean isAdmin) {
    if (isAdmin) {
      when(principal.getName()).thenReturn(ADMIN_USER_NAME);
    } else {
      when(principal.getName()).thenReturn(USER_NAME);
    }
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(context.getSecurityContext()).thenReturn(securityContext);
  }

  private void hasNoSecretYet() throws IOException {
    when(proxy.getS3Secret(notNull()))
        .then(TestSecretGenerate::getS3SecretValue);
  }

  private void hasSecretAlready() throws IOException {
    when(proxy.getS3Secret(notNull()))
        .thenThrow(new OMException("Secret already exists", OMException.ResultCodes.S3_SECRET_ALREADY_EXISTS));
  }
}
