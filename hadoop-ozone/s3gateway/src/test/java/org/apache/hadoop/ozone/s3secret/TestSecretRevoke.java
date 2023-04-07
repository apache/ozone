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
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for S3 secret revoke endpoint.
 */
@ExtendWith(MockitoExtension.class)
public class TestSecretRevoke {
  private static final String USER_NAME = "test";

  private S3SecretRevokeEndpoint endpoint;

  @Mock
  private ObjectStoreStub objectStore;
  @Mock
  private ContainerRequestContext context;
  @Mock
  private SecurityContext securityContext;
  @Mock
  private Principal principal;

  @BeforeEach
  void setUp() {
    OzoneClient client = new OzoneClientStub(objectStore);

    when(principal.getName()).thenReturn(USER_NAME);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(context.getSecurityContext()).thenReturn(securityContext);

    endpoint = new S3SecretRevokeEndpoint();
    endpoint.setClient(client);
    endpoint.setContext(context);
  }

  @Test
  void testSecretRevoke() throws IOException {
    endpoint.get();
    verify(objectStore, times(1)).revokeS3Secret(eq(USER_NAME));
  }
}
