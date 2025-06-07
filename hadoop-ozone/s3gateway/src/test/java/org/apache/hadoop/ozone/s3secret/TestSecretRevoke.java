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

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.S3_SECRET_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test for S3 secret revoke endpoint.
 */
@ExtendWith(MockitoExtension.class)
public class TestSecretRevoke {
  private static final String USER_NAME = "test";
  private static final String OTHER_USER_NAME = "test2";

  private S3SecretManagementEndpoint endpoint;

  @Mock
  private ObjectStoreStub objectStore;
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
    OzoneClient client = new OzoneClientStub(objectStore);

    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(context.getUriInfo()).thenReturn(uriInfo);

    endpoint = new S3SecretManagementEndpoint(conf);
    endpoint.setClient(client);
    endpoint.setContext(context);
  }

  private void mockSecurityContext() {
    when(principal.getName()).thenReturn(USER_NAME);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(context.getSecurityContext()).thenReturn(securityContext);
  }

  @Test
  void testSecretRevoke() throws IOException {
    mockSecurityContext();
    endpoint.revoke();
    verify(objectStore, times(1)).revokeS3Secret(eq(USER_NAME));
  }

  @Test
  void testSecretRevokeWithUsername() throws IOException {
    endpoint.revoke(OTHER_USER_NAME);
    verify(objectStore, times(1))
        .revokeS3Secret(eq(OTHER_USER_NAME));
  }

  @Test
  void testSecretSequentialRevokes() throws IOException {
    mockSecurityContext();
    Response firstResponse = endpoint.revoke();
    assertEquals(OK.getStatusCode(), firstResponse.getStatus());
    doThrow(new OMException(S3_SECRET_NOT_FOUND))
        .when(objectStore).revokeS3Secret(any());
    Response secondResponse = endpoint.revoke();
    assertEquals(NOT_FOUND.getStatusCode(), secondResponse.getStatus());
  }

  @Test
  void testSecretRevokesHandlesException() throws IOException {
    mockSecurityContext();
    doThrow(new OMException(ACCESS_DENIED))
        .when(objectStore).revokeS3Secret(any());
    Response response = endpoint.revoke();
    assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }
}
