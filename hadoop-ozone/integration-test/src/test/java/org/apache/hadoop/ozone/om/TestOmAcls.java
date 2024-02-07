/**
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
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.audit.AuditLogTestUtils.verifyAuditLog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for Ozone Manager ACLs.
 */
@Timeout(300)
public class TestOmAcls {

  private static boolean volumeAclAllow = true;
  private static boolean bucketAclAllow = true;
  private static boolean keyAclAllow = true;
  private static boolean prefixAclAllow = true;
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static GenericTestUtils.LogCapturer logCapturer;

  static {
    AuditLogTestUtils.enableAuditLog();
  }

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   */
  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthorizerTest.class,
        IAccessAuthorizer.class);
    conf.setStrings(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.getLogger());
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    AuditLogTestUtils.deleteAuditLogFile();
  }

  @BeforeEach
  public void setup() throws IOException {
    logCapturer.clearOutput();
    AuditLogTestUtils.truncateAuditLogFile();

    TestOmAcls.volumeAclAllow = true;
    TestOmAcls.bucketAclAllow = true;
    TestOmAcls.keyAclAllow = true;
    TestOmAcls.prefixAclAllow = true;
  }

  @Test
  public void testCreateVolumePermissionDenied() throws Exception {
    TestOmAcls.volumeAclAllow = false;

    OMException exception = assertThrows(OMException.class,
            () -> TestDataUtil.createVolumeAndBucket(client));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());

    assertThat(logCapturer.getOutput())
            .contains("doesn't have CREATE permission to access volume");
    verifyAuditLog(OMAction.CREATE_VOLUME, AuditEventStatus.FAILURE);
  }

  @Test
  public void testReadVolumePermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
    TestOmAcls.volumeAclAllow = false;
    ObjectStore objectStore = client.getObjectStore();
    OMException exception = assertThrows(OMException.class, () ->
            objectStore.getVolume(bucket.getVolumeName()));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());

    assertThat(logCapturer.getOutput())
            .contains("doesn't have READ permission to access volume");
    verifyAuditLog(OMAction.READ_VOLUME, AuditEventStatus.FAILURE);
  }

  @Test
  public void testCreateBucketPermissionDenied() throws Exception {
    TestOmAcls.bucketAclAllow = false;

    OMException exception = assertThrows(OMException.class,
            () -> TestDataUtil.createVolumeAndBucket(client));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());

    assertThat(logCapturer.getOutput())
            .contains("doesn't have CREATE permission to access bucket");
    verifyAuditLog(OMAction.CREATE_BUCKET, AuditEventStatus.FAILURE);
  }

  @Test
  public void testReadBucketPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
    TestOmAcls.bucketAclAllow = false;
    ObjectStore objectStore = client.getObjectStore();
    OMException exception = assertThrows(OMException.class,
            () -> objectStore.getVolume(
                    bucket.getVolumeName()).getBucket(bucket.getName())
    );

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput())
            .contains("doesn't have READ permission to access bucket");
    verifyAuditLog(OMAction.READ_BUCKET, AuditEventStatus.FAILURE);
  }

  @Test
  public void testCreateKeyPermissionDenied() throws Exception {
    TestOmAcls.keyAclAllow = false;

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    OMException exception = assertThrows(OMException.class,
            () -> TestDataUtil.createKey(bucket, "testKey", "testcontent"));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have CREATE " +
            "permission to access key");
  }

  @Test
  public void testReadKeyPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
    TestDataUtil.createKey(bucket, "testKey", "testcontent");

    TestOmAcls.keyAclAllow = false;
    OMException exception = assertThrows(OMException.class,
            () -> TestDataUtil.getKey(bucket, "testKey"));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have READ " +
            "permission to access key");
    verifyAuditLog(OMAction.READ_KEY, AuditEventStatus.FAILURE);
  }

  @Test
  public void testSetACLPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    TestOmAcls.bucketAclAllow = false;

    OMException exception = assertThrows(OMException.class,
            () -> bucket.setAcl(new ArrayList<>()));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput())
        .contains("doesn't have WRITE_ACL permission to access bucket");
    verifyAuditLog(OMAction.SET_ACL, AuditEventStatus.FAILURE);
  }

  /**
   * Test implementation to negative case.
   */
  static class OzoneAccessAuthorizerTest implements IAccessAuthorizer {

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      switch (((OzoneObjInfo) ozoneObject).getResourceType()) {
      case VOLUME:
        return TestOmAcls.volumeAclAllow;
      case BUCKET:
        return TestOmAcls.bucketAclAllow;
      case KEY:
        return TestOmAcls.keyAclAllow;
      case PREFIX:
        return TestOmAcls.prefixAclAllow;
      default:
        return false;
      }
    }
  }
}
