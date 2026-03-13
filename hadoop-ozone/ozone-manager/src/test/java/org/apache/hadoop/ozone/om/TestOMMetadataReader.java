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

import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocolPB.grpc.GrpcClientConstants;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
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

  private static final String ACCESS_KEY_ID = "ASIA7O1AJD8VV4KCEAX5";
  private static final String VOLUME_NAME = "s3v";
  private static final String BUCKET_NAME = "bucket123";
  private static final String KEY_PREFIX = "prefix";
  private static final long MAX_KEYS = 100L;

  @AfterEach
  public void clearStsThreadLocal() {
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  public void testGetClientAddress() throws Exception {
    try (MockedStatic<Server> ipcServerStaticMock = mockStatic(Server.class)) {
      // given
      String expectedClientAddressInCaseOfHadoopRpcCall =
          "hadoop.ipc.client.com";
      ipcServerStaticMock.when(Server::getRemoteAddress)
          .thenReturn(null, null, expectedClientAddressInCaseOfHadoopRpcCall);

      String expectedClientAddressInCaseOfGrpcCall = "172.45.23.4";
      // when (GRPC call with defined client address)
      String clientAddress = Context.current()
          .withValue(GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY,
              expectedClientAddressInCaseOfGrpcCall)
          .call(OmMetadataReader::getClientAddress);
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
    setupStsTokenIdentifier();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext contextWithoutSessionPolicy = createTestRequestContext();
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

    final RequestContext contextWithoutSessionPolicy = createTestRequestContext();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutSessionPolicy, true));

    verifySessionPolicyPassedToAuthorizer(accessAuthorizer, obj, null);
  }

  @Test
  public void testListStatusUsesListAclForStsS3Request() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);

    // For STS S3 requests, listStatus() performs these checks:
    // 1. Volume READ (for volume access)
    // 2) Key LIST (for the specific prefix being listed) - we need LIST permission for STS in order to tell whether the
    //    file should be listed only or downloadable (downloadable would be READ)
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyListCheckWithName(checks, KEY_PREFIX);
  }

  @Test
  public void testListStatusUsesReadAclForNonStsRequest() throws Exception {
    setupNonStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertTrue(checks.stream().allMatch(check -> check.getContext().getAclRights() == READ));

    assertContainsVolumeReadCheck(checks);
    // We want to ensure the current behavior for non-STS requests remains the same
    assertContainsKeyReadCheckWithName(checks);
    assertDoesNotContainKeyListCheck(checks);
  }

  @Test
  public void testListStatusUsesListPrefixForAclWhenKeyNameEmptyAndListPrefixSet() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("")
        .setListPrefix("userA/")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyListCheckWithName(checks, "userA/");
  }

  @Test
  public void testListStatusUsesWildcardForAclWhenKeyNameAndListPrefixEmpty() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyListCheckWithName(checks, "*");
  }

  @Test
  public void testListStatusKeyNameTakesPrecedenceOverListPrefix() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(KEY_PREFIX)
        .setListPrefix("other/")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyListCheckWithName(checks, KEY_PREFIX);
  }

  @Test
  public void testGetFileStatusUsesListAclForStsS3Request() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createGetFileStatusKeyManagerReturningStatus();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    omMetadataReader.getFileStatus(args);

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyListCheckWithName(checks, KEY_PREFIX);
    assertDoesNotContainKeyReadCheck(checks);
  }

  @Test
  public void testGetFileStatusUsesReadAclForNonStsS3Request() throws Exception {
    setupNonStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createGetFileStatusKeyManagerReturningStatus();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    omMetadataReader.getFileStatus(args);

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks);
    assertDoesNotContainKeyListCheck(checks);
  }

  @Test
  public void testListKeysUsesPrefixCheckForStsS3Request() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListKeysKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);

    // Case 1: List with prefix "userA/"
    omMetadataReader.listKeys(VOLUME_NAME, BUCKET_NAME, "", "userA/", (int) MAX_KEYS);

    List<AclCheck> checks = captureAclChecks(accessAuthorizer, 4);
    assertContainsBucketListCheck(checks);
    assertContainsKeyListCheckWithName(checks, "userA/");

    // Reset to make case 2 assertions independent of case 1 captures.
    reset(accessAuthorizer);
    reenableAllowAllAccessChecks(accessAuthorizer);

    // Case 2: List with empty prefix (should check "*")
    omMetadataReader.listKeys(VOLUME_NAME, BUCKET_NAME, "", "", (int) MAX_KEYS);

    checks = captureAclChecks(accessAuthorizer, 4);
    assertContainsBucketListCheck(checks);
    assertContainsKeyListCheckWithName(checks, "*");
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer) throws IOException {
    return createMetadataReader(accessAuthorizer, mock(KeyManager.class));
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer, KeyManager keyManager)
      throws IOException {
    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getConfiguration()).thenReturn(new OzoneConfiguration());
    when(ozoneManager.getAclsEnabled()).thenReturn(true);
    final OMPerformanceMetrics perfMetrics = mock(OMPerformanceMetrics.class);
    // OmMetadataReader uses these MutableRate metrics via MetricUtil.captureLatencyNs(...).
    when(perfMetrics.getListKeysResolveBucketLatencyNs()).thenReturn(mock(MutableRate.class));
    when(perfMetrics.getListKeysAclCheckLatencyNs()).thenReturn(mock(MutableRate.class));
    when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);
    when(ozoneManager.getVolumeOwner(any(), any(), any())).thenReturn("volume-owner");
    when(ozoneManager.getBucketOwner(any(), any(), any(), any())).thenReturn("bucket-owner");
    when(ozoneManager.getOmRpcServerAddr()).thenReturn(new InetSocketAddress("127.0.0.1", 9874));
    when(ozoneManager.resolveBucketLink(any(Pair.class)))
        .thenReturn(
            new ResolvedBucket(
                VOLUME_NAME, BUCKET_NAME, VOLUME_NAME, BUCKET_NAME, "bucket-owner", FILE_SYSTEM_OPTIMIZED));
    when(ozoneManager.resolveBucketLink(any(OmKeyArgs.class)))
        .thenReturn(
            new ResolvedBucket(
                VOLUME_NAME, BUCKET_NAME, VOLUME_NAME, BUCKET_NAME, "bucket-owner", FILE_SYSTEM_OPTIMIZED));

    return new OmMetadataReader(
        keyManager, mock(PrefixManager.class), ozoneManager, mock(Logger.class), mock(AuditLogger.class),
        mock(OmMetadataReaderMetrics.class), accessAuthorizer);
  }

  /**
   * Creates and sets a mock STSTokenIdentifier with a session policy in the thread-local.
   */
  private void setupStsTokenIdentifier() {
    final STSTokenIdentifier stsTokenIdentifier = mock(STSTokenIdentifier.class);
    when(stsTokenIdentifier.getSessionPolicy()).thenReturn("session-policy-from-thread-local");
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
   * Creates a test RequestContext.
   *
   * @return the constructed RequestContext
   */
  private RequestContext createTestRequestContext() {
    RequestContext.Builder builder = RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser("testUser"))
        .setIp(InetAddress.getLoopbackAddress())
        .setHost("localhost")
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(READ)
        .setOwnerName("owner");

    return builder.build();
  }

  /**
   * Creates a test OzoneObj representing a key.
   * @return the constructed OzoneObj
   */
  private OzoneObj createTestOzoneObj() {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("key")
        .build();
  }

  private void setupStsS3Request() {
    OzoneManager.setStsTokenIdentifier(mock(STSTokenIdentifier.class));
    OzoneManager.setS3Auth(S3Authentication.newBuilder().setAccessId(TestOMMetadataReader.ACCESS_KEY_ID).build());
  }

  private void setupNonStsS3Request() {
    OzoneManager.setStsTokenIdentifier(null);
    OzoneManager.setS3Auth(null);
  }

  private OmKeyArgs createOmKeyArgs() {
    return new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(TestOMMetadataReader.KEY_PREFIX)
        .build();
  }

  private KeyManager createListStatusKeyManagerReturningEmpty() throws IOException {
    final KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.listStatus(any(OmKeyArgs.class), eq(false), eq(""), eq(MAX_KEYS), any(), eq(false)))
        .thenReturn(Collections.emptyList());
    return keyManager;
  }

  private KeyManager createGetFileStatusKeyManagerReturningStatus() throws IOException {
    final KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.getFileStatus(any(OmKeyArgs.class), any()))
        .thenReturn(mock(OzoneFileStatus.class));
    return keyManager;
  }

  private KeyManager createListKeysKeyManagerReturningEmpty() throws IOException {
    final KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.listKeys(any(), any(), any(), any(), eq(100)))
        .thenReturn(new ListKeysResult(Collections.emptyList(), false));
    return keyManager;
  }

  private void reenableAllowAllAccessChecks(IAccessAuthorizer accessAuthorizer) throws OMException {
    when(accessAuthorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class)))
        .thenReturn(true);
  }

  /**
   * Verifies that the accessAuthorizer received a call to checkAccess with the expected session policy.
   * @param accessAuthorizer the mock authorizer to verify
   * @param expectedObj the expected OzoneObj
   * @param expectedSessionPolicy the expected session policy (could be null)
   */
  private void verifySessionPolicyPassedToAuthorizer(IAccessAuthorizer accessAuthorizer, OzoneObj expectedObj,
      String expectedSessionPolicy) throws OMException {
    final ArgumentCaptor<RequestContext> captor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer).checkAccess(eq(expectedObj), captor.capture());
    assertEquals(expectedSessionPolicy, captor.getValue().getSessionPolicy());
  }

  private List<AclCheck> captureAclChecks(IAccessAuthorizer accessAuthorizer, int expectedCheckCount)
      throws OMException {
    final ArgumentCaptor<OzoneObj> objCaptor = ArgumentCaptor.forClass(OzoneObj.class);
    final ArgumentCaptor<RequestContext> ctxCaptor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer, times(expectedCheckCount)).checkAccess(objCaptor.capture(), ctxCaptor.capture());
    return toAclChecks(objCaptor.getAllValues(), ctxCaptor.getAllValues());
  }

  private List<AclCheck> toAclChecks(List<OzoneObj> objs, List<RequestContext> contexts) {
    assertEquals(objs.size(), contexts.size(), "Captured ACL objects and contexts should align");
    final List<AclCheck> checks = new ArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      checks.add(new AclCheck(objs.get(i), contexts.get(i)));
    }
    return checks;
  }

  private void assertContainsVolumeReadCheck(List<AclCheck> checks) {
    assertTrue(checks.stream().anyMatch(this::isVolumeReadCheck), "Expected a VOLUME READ ACL check");
  }

  private boolean isVolumeReadCheck(AclCheck check) {
    return check.getObj().getResourceType() == VOLUME && check.getContext().getAclRights() == READ;
  }

  private void assertContainsBucketListCheck(List<AclCheck> checks) {
    assertTrue(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == OzoneObj.ResourceType.BUCKET &&
                check.getContext().getAclRights() == LIST),
        "Expected a BUCKET LIST ACL check");
  }

  private void assertContainsKeyListCheckWithName(List<AclCheck> checks, String keyName) {
    assertTrue(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == KEY && check.getContext().getAclRights() == LIST &&
                keyName.equals(check.getObj().getKeyName())),
        "Expected a KEY LIST ACL check for key '" + keyName + "'");
  }

  private void assertContainsKeyReadCheckWithName(List<AclCheck> checks) {
    assertTrue(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == KEY && check.getContext().getAclRights() == READ &&
                TestOMMetadataReader.KEY_PREFIX.equals(check.getObj().getKeyName())),
        "Expected a KEY READ ACL check for key '" + TestOMMetadataReader.KEY_PREFIX + "'");
  }

  private void assertDoesNotContainKeyReadCheck(List<AclCheck> checks) {
    assertFalse(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == KEY && check.getContext().getAclRights() == READ),
        "Did not expect a KEY READ ACL check");
  }

  private void assertDoesNotContainKeyListCheck(List<AclCheck> checks) {
    assertFalse(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == KEY && check.getContext().getAclRights() == LIST),
        "Did not expect a KEY LIST ACL check");
  }

  private static final class AclCheck {
    private final OzoneObj obj;
    private final RequestContext context;

    private AclCheck(OzoneObj obj, RequestContext context) {
      this.obj = obj;
      this.context = context;
    }

    private OzoneObj getObj() {
      return obj;
    }

    private RequestContext getContext() {
      return context;
    }
  }
}
