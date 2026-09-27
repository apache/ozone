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

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createOmKeyInfo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Unit tests for {@link TrashOzoneFileSystem} on LEGACY buckets.
 */
class TestTrashOzoneFileSystemLegacyBucket {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String USER = "testuser";
  private static final String CHECKPOINT = "260131104600";
  private static final RatisReplicationConfig REPLICATION =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);

  private OzoneManager ozoneManager;
  private UserGroupInformation testUgi;

  @BeforeEach
  public void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    testUgi = UserGroupInformation.createUserForTesting(USER, new String[0]);
    UserGroupInformation.setConfiguration(conf);

    ozoneManager = mock(OzoneManager.class);
    KeyManager keyManager = mock(KeyManager.class);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    OMMetrics metrics = mock(OMMetrics.class);

    when(ozoneManager.getConfiguration()).thenReturn(conf);
    when(ozoneManager.getMetrics()).thenReturn(metrics);
    when(ozoneManager.getKeyManager()).thenReturn(keyManager);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(ozoneManager.getOmRpcServerAddr())
        .thenReturn(new InetSocketAddress("localhost", 9862));
    when(ozoneManager.getBucketInfo(VOLUME, BUCKET)).thenReturn(
        OmBucketInfo.newBuilder()
            .setVolumeName(VOLUME)
            .setBucketName(BUCKET)
            .setBucketLayout(BucketLayout.LEGACY)
            .build());

    when(keyManager.getFileStatus(any(OmKeyArgs.class))).thenAnswer(invocation -> {
      OmKeyArgs keyArgs = invocation.getArgument(0);
      String keyName = keyArgs.getKeyName();
      OmKeyInfo keyInfo = createOmKeyInfo(VOLUME, BUCKET, keyName, REPLICATION).build();
      boolean isDirectory = keyName.endsWith("/")
          || keyName.endsWith("Current")
          || keyName.endsWith(CHECKPOINT);
      return new OzoneFileStatus(keyInfo, 4096, isDirectory);
    });

    when(metadataManager.listKeys(
        eq(VOLUME), eq(BUCKET), anyString(), anyString(), anyInt()))
        .thenAnswer(invocation -> {
          String startKey = invocation.getArgument(2);
          String keyPrefix = invocation.getArgument(3);
          if (StringUtils.isNotBlank(startKey)) {
            return new ListKeysResult(Collections.emptyList(), false);
          }
          if (keyPrefix.startsWith(".Trash/" + USER + "/Current")) {
            return new ListKeysResult(
                Collections.singletonList(
                    createTrashKeyInfo(".Trash/" + USER + "/Current/file1")),
                false);
          }
          if (keyPrefix.startsWith(".Trash/" + USER + "/" + CHECKPOINT)) {
            return new ListKeysResult(
                Collections.singletonList(
                    createTrashKeyInfo(".Trash/" + USER + "/" + CHECKPOINT + "/file1")),
                false);
          }
          return new ListKeysResult(Collections.emptyList(), false);
        });
  }

  @Test
  public void testLegacyDeleteReturnsFalseWhenSubmitRequestFails() throws Exception {
    try (MockedStatic<OzoneManagerRatisUtils> ratisUtils =
        mockStatic(OzoneManagerRatisUtils.class)) {
      stubRatisUtilsToFailOnSubmit(ratisUtils);
      TrashOzoneFileSystem trashFs = new TrashOzoneFileSystem(ozoneManager);

      boolean deleted = testUgi.doAs(
          (PrivilegedExceptionAction<Boolean>) () ->
              trashFs.delete(expiredCheckpointPath(), true));

      assertFalse(deleted,
          "LEGACY trash delete should return false when submitRequest fails");
    }
  }

  @Test
  public void testLegacyRenameReturnsFalseWhenSubmitRequestFails() throws Exception {
    try (MockedStatic<OzoneManagerRatisUtils> ratisUtils =
        mockStatic(OzoneManagerRatisUtils.class)) {
      stubRatisUtilsToFailOnSubmit(ratisUtils);
      TrashOzoneFileSystem trashFs = new TrashOzoneFileSystem(ozoneManager);

      boolean renamed = testUgi.doAs(
          (PrivilegedExceptionAction<Boolean>) () ->
              trashFs.rename(currentTrashPath(), checkpointTrashPath()));

      assertFalse(renamed,
          "LEGACY trash rename should return false when submitRequest fails");
    }
  }

  @Test
  public void testLegacyDeleteReturnsTrueWhenSubmitRequestSucceeds() throws Exception {
    try (MockedStatic<OzoneManagerRatisUtils> ratisUtils =
        mockStatic(OzoneManagerRatisUtils.class)) {
      stubRatisUtilsToSucceedOnSubmit(ratisUtils, OzoneManagerProtocolProtos.Type.DeleteKey);
      TrashOzoneFileSystem trashFs = new TrashOzoneFileSystem(ozoneManager);

      boolean deleted = testUgi.doAs(
          (PrivilegedExceptionAction<Boolean>) () ->
              trashFs.delete(expiredCheckpointPath(), true));

      assertTrue(deleted,
          "LEGACY trash delete should return true when submitRequest succeeds");
    }
  }

  @Test
  public void testLegacyRenameReturnsTrueWhenSubmitRequestSucceeds() throws Exception {
    try (MockedStatic<OzoneManagerRatisUtils> ratisUtils =
        mockStatic(OzoneManagerRatisUtils.class)) {
      stubRatisUtilsToSucceedOnSubmit(ratisUtils, OzoneManagerProtocolProtos.Type.RenameKey);
      TrashOzoneFileSystem trashFs = new TrashOzoneFileSystem(ozoneManager);

      boolean renamed = testUgi.doAs(
          (PrivilegedExceptionAction<Boolean>) () ->
              trashFs.rename(currentTrashPath(), checkpointTrashPath()));

      assertTrue(renamed,
          "LEGACY trash rename should return true when submitRequest succeeds");
    }
  }

  @Test
  public void testLegacyDeleteReturnsFalseWhenOmRequestIsNull() throws Exception {
    TrashOzoneFileSystem trashFs = new TrashOzoneFileSystem(ozoneManager);

    try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
      ugi.when(UserGroupInformation::getCurrentUser)
          .thenThrow(new IOException("simulated userinfo failure"));

      assertFalse(trashFs.delete(expiredCheckpointPath(), true),
          "LEGACY trash delete should return false when OM request cannot be built");
    }
  }

  @Test
  public void testLegacyRenameReturnsFalseWhenOmRequestIsNull() throws Exception {
    TrashOzoneFileSystem trashFs = new TrashOzoneFileSystem(ozoneManager);

    try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
      UserGroupInformation.setLoginUser(testUgi);
      AtomicInteger getCurrentUserCalls = new AtomicInteger(0);
      ugi.when(UserGroupInformation::getCurrentUser).thenAnswer(invocation -> {
        // rename() calls getCurrentUser for src/dst trash-root validation first
        if (getCurrentUserCalls.incrementAndGet() <= 2) {
          return testUgi;
        }
        throw new IOException("simulated userinfo failure");
      });

      assertFalse(trashFs.rename(currentTrashPath(), checkpointTrashPath()),
          "LEGACY trash rename should return false when OM request cannot be built");
    }
  }

  private static Path expiredCheckpointPath() {
    return new Path("/" + VOLUME + "/" + BUCKET + "/.Trash/" + USER + "/" + CHECKPOINT);
  }

  private static Path currentTrashPath() {
    return new Path("/" + VOLUME + "/" + BUCKET + "/.Trash/" + USER + "/Current");
  }

  private static Path checkpointTrashPath() {
    return new Path("/" + VOLUME + "/" + BUCKET + "/.Trash/" + USER + "/" + CHECKPOINT);
  }

  private static void stubRatisUtilsCreateClientRequest(
      MockedStatic<OzoneManagerRatisUtils> ratisUtils) {
    ratisUtils.when(() -> OzoneManagerRatisUtils.createClientRequest(
            any(OMRequest.class), any(OzoneManager.class)))
        .thenAnswer(invocation -> {
          OMRequest omRequest = invocation.getArgument(0);
          OMClientRequest clientRequest = mock(OMClientRequest.class);
          when(clientRequest.preExecute(invocation.getArgument(1))).thenReturn(omRequest);
          return clientRequest;
        });
  }

  private static void stubRatisUtilsToFailOnSubmit(
      MockedStatic<OzoneManagerRatisUtils> ratisUtils) {
    stubRatisUtilsCreateClientRequest(ratisUtils);
    ratisUtils.when(() -> OzoneManagerRatisUtils.submitRequest(
            any(OzoneManager.class), any(OMRequest.class), any(), anyLong()))
        .thenThrow(new ServiceException("simulated trash write failure"));
  }

  private static void stubRatisUtilsToSucceedOnSubmit(
      MockedStatic<OzoneManagerRatisUtils> ratisUtils, OzoneManagerProtocolProtos.Type cmd) {
    stubRatisUtilsCreateClientRequest(ratisUtils);
    ratisUtils.when(() -> OzoneManagerRatisUtils.submitRequest(
            any(OzoneManager.class), any(OMRequest.class), any(), anyLong()))
        .thenReturn(
            OMResponse.newBuilder()
                .setCmdType(cmd)
                .setStatus(OzoneManagerProtocolProtos.Status.OK).build()
        );
  }

  private static OmKeyInfo createTrashKeyInfo(String keyName) {
    return createOmKeyInfo(VOLUME, BUCKET, keyName, REPLICATION).build();
  }
}
