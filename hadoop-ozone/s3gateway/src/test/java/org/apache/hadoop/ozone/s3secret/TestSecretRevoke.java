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
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.S3_SECRET_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for S3 secret revoke endpoint.
 */
@ExtendWith(MockitoExtension.class)
public class TestSecretRevoke {
  private static final String ADMIN_USER_NAME = "test_admin";
  private static final String USER_NAME = "test";
  private static final String OTHER_USER_NAME = "test2";

  private S3SecretManagementEndpoint endpoint;
  private ObjectStoreStub objectStore;

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

  @BeforeEach
  void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_ADMINISTRATORS, ADMIN_USER_NAME);
    objectStore = new ObjectStoreStub(conf, proxy);
    OzoneClient client = new OzoneClientStub(objectStore);

    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(context.getUriInfo()).thenReturn(uriInfo);

    endpoint = new S3SecretManagementEndpoint();
    endpoint.setClient(client);
    endpoint.setContext(context);
  }

  /**
   * Provides mocking for users and security context.
   * @param isAdmin Stores whether the user is admin or not
   */
  private void mockSecurityContext(boolean isAdmin) {
    if (isAdmin) {
      when(principal.getName()).thenReturn(ADMIN_USER_NAME);
    } else {
      when(principal.getName()).thenReturn(USER_NAME);
    }
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(context.getSecurityContext()).thenReturn(securityContext);
  }

  @Test
  void testUnauthorizedUserSecretRevoke() throws IOException {
    mockSecurityContext(false);
    Response response = endpoint.revoke();

    assertEquals(FORBIDDEN.getStatusCode(), response.getStatus());
  }

  @Test
  void testAuthorizedUserSecretRevoke() throws IOException {
    mockSecurityContext(true);
    endpoint.revoke();
    verify(objectStore, times(1)).revokeS3Secret(eq(USER_NAME));
  }

  @Test
  @Unhealthy("HDDS-11041")
  void testSecretRevokeWithUsername() throws IOException {
    endpoint.revoke(OTHER_USER_NAME);
    verify(objectStore, times(1))
        .revokeS3Secret(eq(OTHER_USER_NAME));
  }

  @Test
  void testUnauthorizedUserSecretSequentialRevokes() throws IOException {
    mockSecurityContext(false);
    Response firstResponse = endpoint.revoke();
    assertEquals(FORBIDDEN.getStatusCode(), firstResponse.getStatus());

    Response secondResponse = endpoint.revoke();
    assertEquals(FORBIDDEN.getStatusCode(), secondResponse.getStatus());
  }

  @Test
  void testAuthorizedUserSecretSequentialRevokes() throws IOException {
    mockSecurityContext(true);
    Response firstResponse = endpoint.revoke();
    assertEquals(OK.getStatusCode(), firstResponse.getStatus());
    doThrow(new OMException(S3_SECRET_NOT_FOUND))
        .when(objectStore).revokeS3Secret(any());
    Response secondResponse = endpoint.revoke();
    assertEquals(NOT_FOUND.getStatusCode(), secondResponse.getStatus());
  }

  @Test
  void testSecretRevokesHandlesException() throws IOException {
    mockSecurityContext(true);
    doThrow(new OMException(ACCESS_DENIED))
        .when(objectStore).revokeS3Secret(any());
    Response response = endpoint.revoke();
    assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }
}
