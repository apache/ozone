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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import java.net.InetAddress;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.Logger;

/**
 * Test ozone metadata reader.
 */
public class TestOMMetadataReader {

  @AfterEach
  public void clearStsThreadLocal() {
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  public void testGetClientAddress() {
    try (
        MockedStatic<Server> ipcServerStaticMock = mockStatic(Server.class);
        MockedStatic<Context> grpcRequestContextStaticMock = mockStatic(Context.class);
    ) {
      // given
      String expectedClientAddressInCaseOfHadoopRpcCall =
          "hadoop.ipc.client.com";
      ipcServerStaticMock.when(Server::getRemoteAddress)
          .thenReturn(null, null, expectedClientAddressInCaseOfHadoopRpcCall);

      String expectedClientAddressInCaseOfGrpcCall = "172.45.23.4";
      Context.Key<String> clientIpAddressKey = mock(Context.Key.class);
      when(clientIpAddressKey.get())
          .thenReturn(expectedClientAddressInCaseOfGrpcCall, null);

      grpcRequestContextStaticMock.when(() -> Context.key("CLIENT_IP_ADDRESS"))
          .thenReturn(clientIpAddressKey);

      // when (GRPC call with defined client address)
      String clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfGrpcCall, clientAddress);

      // and when (GRPC call without client address)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals("", clientAddress);

      // and when (Hadoop RPC client call)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfHadoopRpcCall, clientAddress);
    }
  }

  @Test
  public void testCheckAclsAttachesSessionPolicyFromThreadLocal() throws Exception {
    final String sessionPolicy = "session-policy-from-thread-local";
    setupStsTokenIdentifier(sessionPolicy);

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext contextWithoutSessionPolicy = createTestRequestContext(null);
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutSessionPolicy, true));

    verifySessionPolicyPassedToAuthorizer(accessAuthorizer, obj, sessionPolicy);
  }

  @Test
  public void testNoSessionPolicyWhenThreadLocalIsNull() throws Exception {
    // No STS token identifier in thread local
    OzoneManager.setStsTokenIdentifier(null);

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext contextWithoutSessionPolicy = createTestRequestContext(null);
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutSessionPolicy, true));

    verifySessionPolicyPassedToAuthorizer(accessAuthorizer, obj, null);
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer) {
    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getAclsEnabled()).thenReturn(true);
    when(ozoneManager.getPerfMetrics()).thenReturn(mock(OMPerformanceMetrics.class));

    return new OmMetadataReader(
        mock(KeyManager.class), mock(PrefixManager.class), ozoneManager, mock(Logger.class), mock(AuditLogger.class),
        mock(OmMetadataReaderMetrics.class), accessAuthorizer);
  }

  /**
   * Creates and sets a mock STSTokenIdentifier with the given session policy in the thread-local.
   * @param sessionPolicy the session policy to return, or null
   */
  private void setupStsTokenIdentifier(String sessionPolicy) {
    final STSTokenIdentifier stsTokenIdentifier = mock(STSTokenIdentifier.class);
    when(stsTokenIdentifier.getSessionPolicy()).thenReturn(sessionPolicy);
    OzoneManager.setStsTokenIdentifier(stsTokenIdentifier);
  }

  /**
   * Creates a mock IAccessAuthorizer that returns the specified result for checkAccess.
   * @return the mocked IAccessAuthorizer
   */
  private IAccessAuthorizer createMockIAccessAuthorizerReturningTrue() throws OMException {
    final IAccessAuthorizer accessAuthorizer = mock(IAccessAuthorizer.class);
    when(accessAuthorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class)))
        .thenReturn(true);
    return accessAuthorizer;
  }

  /**
   * Creates a test RequestContext with the given session policy.
   * @param sessionPolicy the session policy to set, or null
   * @return the constructed RequestContext
   */
  private RequestContext createTestRequestContext(String sessionPolicy) {
    RequestContext.Builder builder = RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser("testUser"))
        .setIp(InetAddress.getLoopbackAddress())
        .setHost("localhost")
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(IAccessAuthorizer.ACLType.READ)
        .setOwnerName("owner");

    if (sessionPolicy != null) {
      builder.setSessionPolicy(sessionPolicy);
    }

    return builder.build();
  }

  /**
   * Creates a test OzoneObj representing a key.
   * @return the constructed OzoneObj
   */
  private OzoneObj createTestOzoneObj() {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .build();
  }

  /**
   * Verifies that the accessAuthorizer received a call to checkAccess with the expected session policy.
   * @param accessAuthorizer the mock authorizer to verify
   * @param expectedObj the expected OzoneObj
   * @param expectedSessionPolicy the expected session policy (may be null)
   */
  private void verifySessionPolicyPassedToAuthorizer(IAccessAuthorizer accessAuthorizer, OzoneObj expectedObj,
      String expectedSessionPolicy) throws OMException {
    final ArgumentCaptor<RequestContext> captor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer).checkAccess(eq(expectedObj), captor.capture());
    assertEquals(expectedSessionPolicy, captor.getValue().getSessionPolicy());
  }
}
