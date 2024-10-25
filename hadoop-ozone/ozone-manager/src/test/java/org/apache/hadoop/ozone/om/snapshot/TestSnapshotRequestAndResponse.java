/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotCreateRequest;
import org.apache.hadoop.ozone.om.request.snapshot.TestOMSnapshotCreateRequest;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createOmKeyInfo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class to test snapshot functionalities.
 */
public class TestSnapshotRequestAndResponse {
  @TempDir
  private File testDir;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OmMetadataManagerImpl omMetadataManager;
  private BatchOperation batchOperation;
  private OmSnapshotManager omSnapshotManager;

  private String volumeName;
  private String bucketName;
  private boolean isAdmin;

  public BatchOperation getBatchOperation() {
    return batchOperation;
  }

  public String getBucketName() {
    return bucketName;
  }

  public boolean isAdmin() {
    return isAdmin;
  }

  public OmMetadataManagerImpl getOmMetadataManager() {
    return omMetadataManager;
  }

  public OMMetrics getOmMetrics() {
    return omMetrics;
  }

  public OmSnapshotManager getOmSnapshotManager() {
    return omSnapshotManager;
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  public File getTestDir() {
    return testDir;
  }

  public String getVolumeName() {
    return volumeName;
  }

  protected TestSnapshotRequestAndResponse() {
    this.isAdmin = false;
  }

  protected TestSnapshotRequestAndResponse(boolean isAdmin) {
    this.isAdmin = isAdmin;
  }

  @BeforeEach
  public void baseSetup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        testDir.getAbsolutePath());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    when(ozoneManager.isFilesystemSnapshotEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any())).thenReturn(isAdmin);
    when(ozoneManager.isOwner(any(), any())).thenReturn(false);
    when(ozoneManager.getBucketOwner(any(), any(),
        any(), any())).thenReturn("dummyBucketOwner");
    IAccessAuthorizer accessAuthorizer = mock(IAccessAuthorizer.class);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(accessAuthorizer);
    when(accessAuthorizer.isNative()).thenReturn(false);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    batchOperation = omMetadataManager.getStore().initBatchOperation();

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    omSnapshotManager = new OmSnapshotManager(ozoneManager);
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  protected Path createSnapshotCheckpoint(String volume, String bucket, String snapshotName) throws Exception {
    OzoneManagerProtocolProtos.OMRequest omRequest = OMRequestTestUtils
        .createSnapshotRequest(volume, bucket, snapshotName);
    // Pre-Execute OMSnapshotCreateRequest.
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest, ozoneManager);

    // validateAndUpdateCache OMSnapshotCreateResponse.
    OMSnapshotCreateResponse omClientResponse = (OMSnapshotCreateResponse)
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1);
    // Add to batch and commit to DB.
    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      omClientResponse.addToDBBatch(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }

    String key = SnapshotInfo.getTableKey(volume, bucket, snapshotName);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);

    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    Path snapshotDirPath = Paths.get(store.getSnapshotsParentDir(),
        checkpointPrefix + snapshotInfo.getCheckpointDir());
    // Check the DB is still there
    assertTrue(Files.exists(snapshotDirPath));
    return snapshotDirPath;
  }

  protected List<Pair<String, List<OmKeyInfo>>> getDeletedKeys(String volume, String bucket,
                                                               int startRange, int endRange,
                                                               int numberOfKeys,
                                                               int minVersion) {
    return IntStream.range(startRange, endRange).boxed()
        .map(i -> Pair.of(omMetadataManager.getOzoneDeletePathKey(i,
                omMetadataManager.getOzoneKey(volume, bucket, "key" + String.format("%010d", i))),
            IntStream.range(0, numberOfKeys).boxed().map(cnt -> createOmKeyInfo(volume, bucket, "key" + i,
                    ReplicationConfig.getDefault(ozoneManager.getConfiguration()),
                    new OmKeyLocationInfoGroup(minVersion + cnt, new ArrayList<>(), false))
                    .setCreationTime(0).setModificationTime(0).build())
                .collect(Collectors.toList())))
        .collect(Collectors.toList());
  }

  protected List<Pair<String, String>> getRenameKeys(String volume, String bucket,
                                                     int startRange, int endRange,
                                                     String renameKeyPrefix) {
    return IntStream.range(startRange, endRange).boxed()
        .map(i -> {
          try {
            return Pair.of(omMetadataManager.getRenameKey(volume, bucket, i),
                omMetadataManager.getOzoneKeyFSO(volume, bucket, renameKeyPrefix + i));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());
  }

  protected List<Pair<String, List<OmKeyInfo>>> getDeletedDirKeys(String volume, String bucket,
                                                                  int startRange, int endRange, int numberOfKeys) {
    return IntStream.range(startRange, endRange).boxed()
        .map(i -> {
          try {
            return Pair.of(omMetadataManager.getOzoneDeletePathKey(i,
                    omMetadataManager.getOzoneKeyFSO(volume, bucket, "1/key" + i)),
                IntStream.range(0, numberOfKeys).boxed().map(cnt -> createOmKeyInfo(volume, bucket, "key" + i,
                        ReplicationConfig.getDefault(ozoneManager.getConfiguration())).build())
                    .collect(Collectors.toList()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

}
