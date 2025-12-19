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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.audit.AuditLogTestUtils.verifyAuditLog;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for Ozone Manager ACLs.
 */
public class TestOmAcls {

  private static OzoneAccessAuthorizerTest authorizer;
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static LogCapturer logCapturer;

  static {
    AuditLogTestUtils.enableAuditLog();
  }

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
    logCapturer = LogCapturer.captureLogs(OzoneManager.class);
    authorizer = assertInstanceOf(OzoneAccessAuthorizerTest.class, cluster.getOzoneManager().getAccessAuthorizer());
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

    authorizer.volumeAclAllow = true;
    authorizer.bucketAclAllow = true;
    authorizer.keyAclAllow = true;
    authorizer.prefixAclAllow = true;
  }

  @Test
  public void testCreateVolumePermissionDenied() throws Exception {
    authorizer.volumeAclAllow = false;

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
    authorizer.volumeAclAllow = false;
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
    authorizer.bucketAclAllow = false;

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
    authorizer.bucketAclAllow = false;
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
    authorizer.keyAclAllow = false;

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    OMException exception = assertThrows(OMException.class,
            () -> TestDataUtil.createKey(bucket, "testKey", "testcontent".getBytes(StandardCharsets.UTF_8)));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have CREATE " +
            "permission to access key");
  }

  @Test
  public void testReadKeyPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
    TestDataUtil.createKey(bucket, "testKey", "testcontent".getBytes(StandardCharsets.UTF_8));

    authorizer.keyAclAllow = false;
    OMException exception = assertThrows(OMException.class,
            () -> TestDataUtil.getKey(bucket, "testKey"));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have READ " +
            "permission to access key");
    verifyAuditLog(OMAction.READ_KEY, AuditEventStatus.FAILURE);
  }

  @Test
  public void testGetFileStatusPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
    TestDataUtil.createKey(bucket, "testKey", "testcontent".getBytes(StandardCharsets.UTF_8));

    authorizer.keyAclAllow = false;
    OMException exception = assertThrows(OMException.class,
            () -> bucket.getFileStatus("testKey"));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have READ " +
            "permission to access key");
    verifyAuditLog(OMAction.GET_FILE_STATUS, AuditEventStatus.FAILURE);
  }

  @Test
  public void testSetACLPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    authorizer.bucketAclAllow = false;

    OMException exception = assertThrows(OMException.class,
            () -> bucket.setAcl(new ArrayList<>()));
    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput())
        .contains("doesn't have WRITE_ACL permission to access bucket");
    verifyAuditLog(OMAction.SET_ACL, AuditEventStatus.FAILURE);
  }

  @Test
  public void testKeyACLOpsPermissionDenied() throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
    String keyName = "testKey";
    TestDataUtil.createKey(bucket, keyName, "testcontent".getBytes(StandardCharsets.UTF_8));

    authorizer.keyAclAllow = false;
    ObjectStore objectStore = client.getObjectStore();
    OzoneObj key = new OzoneObjInfo.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OMException exception = assertThrows(OMException.class,
        () -> objectStore.getAcl(key));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have READ_ACL " +
        "permission to access key");
    verifyAuditLog(OMAction.GET_ACL, AuditEventStatus.FAILURE);

    OzoneAcl acl = OzoneAcl.of(USER, "testuser1",
        OzoneAcl.AclScope.DEFAULT, IAccessAuthorizer.ACLType.ALL);

    exception = assertThrows(OMException.class,
        () -> objectStore.setAcl(key, Collections.singletonList(acl)));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have WRITE_ACL " +
        "permission to access key");
    verifyAuditLog(OMAction.SET_ACL, AuditEventStatus.FAILURE);

    exception = assertThrows(OMException.class,
        () -> objectStore.addAcl(key, acl));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have WRITE_ACL " +
        "permission to access key");
    verifyAuditLog(OMAction.ADD_ACL, AuditEventStatus.FAILURE);

    exception = assertThrows(OMException.class,
        () -> objectStore.removeAcl(key, acl));

    assertEquals(ResultCodes.PERMISSION_DENIED, exception.getResult());
    assertThat(logCapturer.getOutput()).contains("doesn't have WRITE_ACL " +
        "permission to access key");
    verifyAuditLog(OMAction.REMOVE_ACL, AuditEventStatus.FAILURE);
  }

  /**
   * Test implementation to negative case.
   */
  static class OzoneAccessAuthorizerTest implements IAccessAuthorizer {

    private boolean volumeAclAllow = true;
    private boolean bucketAclAllow = true;
    private boolean keyAclAllow = true;
    private boolean prefixAclAllow = true;

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      switch (((OzoneObjInfo) ozoneObject).getResourceType()) {
      case VOLUME:
        return volumeAclAllow;
      case BUCKET:
        return bucketAclAllow;
      case KEY:
        return keyAclAllow;
      case PREFIX:
        return prefixAclAllow;
      default:
        return false;
      }
    }
  }
}
