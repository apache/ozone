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
import javax.ws.rs.core.SecurityContext;
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
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Test for S3 secret generate endpoint.
 */
@ExtendWith(MockitoExtension.class)
public class TestSecretGenerate {
  private static final String USER_NAME = "test";
  private static final String USER_SECRET = "test_secret";

  private S3SecretGenerateEndpoint endpoint;

  @Mock
  private ClientProtocol proxy;
  @Mock
  private ContainerRequestContext context;
  @Mock
  private SecurityContext securityContext;
  @Mock
  private Principal principal;

  @BeforeEach
  void setUp() throws IOException {
    S3SecretValue value = new S3SecretValue(USER_NAME, USER_SECRET);
    when(proxy.getS3Secret(eq(USER_NAME))).thenReturn(value);
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClient client = new OzoneClientStub(new ObjectStoreStub(conf, proxy));

    when(principal.getName()).thenReturn(USER_NAME);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(context.getSecurityContext()).thenReturn(securityContext);

    endpoint = new S3SecretGenerateEndpoint();
    endpoint.setClient(client);
    endpoint.setContext(context);
  }

  @Test
  void testSecretGenerate() throws IOException {
    S3SecretResponse response = (S3SecretResponse) endpoint.get().getEntity();
    assertEquals(USER_SECRET, response.getAwsSecret());
    assertEquals(USER_NAME, response.getAwsAccessKey());
  }
}
