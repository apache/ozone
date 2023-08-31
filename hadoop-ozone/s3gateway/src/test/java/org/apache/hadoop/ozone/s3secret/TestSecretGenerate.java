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
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test for S3 secret generate endpoint.
 */
@ExtendWith(MockitoExtension.class)
public class TestSecretGenerate {
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
    return new S3SecretValue((String) args[0], USER_SECRET);
  }

  @BeforeEach
  void setUp() throws IOException {
    when(proxy.getS3Secret(any())).then(TestSecretGenerate::getS3SecretValue);
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClient client = new OzoneClientStub(new ObjectStoreStub(conf, proxy));

    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(context.getUriInfo()).thenReturn(uriInfo);

    endpoint = new S3SecretManagementEndpoint();
    endpoint.setClient(client);
    endpoint.setContext(context);
  }

  @Test
  void testSecretGenerate() throws IOException {
    when(principal.getName()).thenReturn(USER_NAME);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(context.getSecurityContext()).thenReturn(securityContext);

    S3SecretResponse response =
            (S3SecretResponse) endpoint.generate().getEntity();
    assertEquals(USER_SECRET, response.getAwsSecret());
    assertEquals(USER_NAME, response.getAwsAccessKey());
  }

  @Test
  void testSecretGenerateWithUsername() throws IOException {
    S3SecretResponse response =
            (S3SecretResponse) endpoint.generate(OTHER_USER_NAME).getEntity();
    assertEquals(USER_SECRET, response.getAwsSecret());
    assertEquals(OTHER_USER_NAME, response.getAwsAccessKey());
  }
}
