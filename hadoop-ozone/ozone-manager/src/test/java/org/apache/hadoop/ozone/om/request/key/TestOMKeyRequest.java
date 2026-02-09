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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.setupReplicationConfigValidation;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotCreateRequest;
import org.apache.hadoop.ozone.om.request.snapshot.TestOMSnapshotCreateRequest;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.event.Level;

/**
 * Base test class for key request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMKeyRequest {
  @TempDir
  private Path folder;

  protected OzoneManager ozoneManager;
  protected KeyManager keyManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;

  protected ScmClient scmClient;
  protected OzoneBlockTokenSecretManager ozoneBlockTokenSecretManager;
  protected ScmBlockLocationProtocol scmBlockLocationProtocol;
  protected StorageContainerLocationProtocol scmContainerLocationProtocol;
  protected OMPerformanceMetrics perfMetrics;
  protected DeletingServiceMetrics delMetrics;

  protected static final long CONTAINER_ID = 1000L;
  protected static final long LOCAL_ID = 100L;

  protected String volumeName;
  protected String bucketName;
  protected String keyName;
  protected ReplicationConfig replicationConfig;
  protected long clientID;
  protected long scmBlockSize = 1000L;
  protected long dataSize;
  protected Random random;
  protected long txnLogId = 100000L;
  protected long version = 0L;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    perfMetrics = OMPerformanceMetrics.register();
    delMetrics = DeletingServiceMetrics.create();
    OzoneConfiguration ozoneConfiguration = getOzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        folder.toAbsolutePath().toString());
    ozoneConfiguration.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    ozoneConfiguration.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    omMetadataManager = spy(new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager));
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);
    when(ozoneManager.getDeletionMetrics()).thenReturn(delMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getConfig()).thenReturn(ozoneConfiguration.getObject(OmConfig.class));
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.isFilesystemSnapshotEnabled()).thenReturn(true);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);
    when(ozoneManager.getBucketInfo(anyString(), anyString())).thenReturn(
        new OmBucketInfo.Builder().setVolumeName("").setBucketName("").build());
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    AuditMessage mockAuditMessage = mock(AuditMessage.class);
    when(mockAuditMessage.getOp()).thenReturn("MOCK_OP");
    when(ozoneManager.buildAuditMessageForSuccess(any(), any())).thenReturn(mockAuditMessage);
    when(ozoneManager.buildAuditMessageForFailure(any(), any(), any())).thenReturn(mockAuditMessage);

    setupReplicationConfigValidation(ozoneManager, ozoneConfiguration);

    scmClient = mock(ScmClient.class);
    ozoneBlockTokenSecretManager = mock(OzoneBlockTokenSecretManager.class);
    scmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    perfMetrics = mock(OMPerformanceMetrics.class);
    keyManager = new KeyManagerImpl(ozoneManager, scmClient, ozoneConfiguration,
        perfMetrics);
    when(ozoneManager.getScmClient()).thenReturn(scmClient);
    when(ozoneManager.getBlockTokenSecretManager())
        .thenReturn(ozoneBlockTokenSecretManager);
    when(ozoneManager.getScmBlockSize()).thenReturn(scmBlockSize);
    when(ozoneManager.getPreallocateBlocksMax()).thenReturn(2);
    when(ozoneManager.isGrpcBlockTokenEnabled()).thenReturn(false);
    when(ozoneManager.getOMNodeId()).thenReturn(UUID.randomUUID().toString());
    when(ozoneManager.getOMServiceId()).thenReturn(
        UUID.randomUUID().toString());
    when(scmClient.getBlockClient()).thenReturn(scmBlockLocationProtocol);
    scmContainerLocationProtocol = Mockito.mock(StorageContainerLocationProtocol.class);
    when(scmClient.getContainerClient()).thenReturn(scmContainerLocationProtocol);

    when(ozoneManager.getKeyManager()).thenReturn(keyManager);
    when(ozoneManager.getAccessAuthorizer())
        .thenReturn(new OzoneNativeAuthorizer());

    UncheckedAutoCloseableSupplier<IOmMetadataReader> rcOmMetadataReader =
        mock(UncheckedAutoCloseableSupplier.class);
    when(ozoneManager.getOmMetadataReader()).thenReturn(rcOmMetadataReader);
    // Init OmMetadataReader to let the test pass
    OmMetadataReader omMetadataReader = mock(OmMetadataReader.class);
    when(omMetadataReader.isNativeAuthorizerEnabled()).thenReturn(true);
    when(rcOmMetadataReader.get()).thenReturn(omMetadataReader);

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setNodes(new ArrayList<>())
        .build();

    AllocatedBlock.Builder blockBuilder = new AllocatedBlock.Builder()
        .setPipeline(pipeline);

    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(),
        any(ReplicationConfig.class),
        anyString(), any(ExcludeList.class),
        anyString())).thenAnswer(invocation -> {
          int num = invocation.getArgument(1);
          List<AllocatedBlock> allocatedBlocks = new ArrayList<>(num);
          for (int i = 0; i < num; i++) {
            blockBuilder.setContainerBlockID(
                new ContainerBlockID(CONTAINER_ID + i, LOCAL_ID + i));
            allocatedBlocks.add(blockBuilder.build());
          }
          return allocatedBlocks;
        });

    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(1L)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setPipelineID(pipeline.getId())
        .build();
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, pipeline);
    when(scmContainerLocationProtocol.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline);

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
    replicationConfig = RatisReplicationConfig.getInstance(ReplicationFactor.ONE);
    clientID = Time.now();
    dataSize = 1000L;
    random = new Random();
    version = 0L;

    ResolvedBucket bucket = new ResolvedBucket(volumeName, bucketName,
        volumeName, bucketName, "owner", BucketLayout.OBJECT_STORE);
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenReturn(bucket);
    when(ozoneManager.resolveBucketLink(any(Pair.class),
        any(OMClientRequest.class)))
        .thenAnswer(i -> {
          Pair<String, String> bucketLink = i.getArgument(0);
          OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
              omMetadataManager.getBucketKey(bucketLink.getKey(), bucketLink.getValue()));
          return new ResolvedBucket(bucketLink.getKey(), bucketLink.getValue(),
              bucketLink.getKey(), bucketLink.getValue(), bucketInfo.getOwner(), bucketInfo.getBucketLayout());
        });
    when(ozoneManager.resolveBucketLink(any(Pair.class)))
        .thenReturn(bucket);
    OmSnapshotManager omSnapshotManager = Mockito.spy(new OmSnapshotManager(ozoneManager));
    when(ozoneManager.getOmSnapshotManager())
        .thenReturn(omSnapshotManager);

    // Enable DEBUG level logging for relevant classes
    GenericTestUtils.setLogLevel(OMKeyRequest.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMKeyCommitRequest.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMKeyCommitRequestWithFSO.class, Level.DEBUG);
  }

  @Nonnull
  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  /**
   * Verify path in open key table. Also, it returns OMKeyInfo for the given
   * key path.
   *
   * @param key      key name
   * @param id       client id
   * @param doAssert if true then do assertion, otherwise it just skip.
   * @return om key info for the given key path.
   * @throws Exception DB failure
   */
  protected OmKeyInfo verifyPathInOpenKeyTable(String key, long id,
                                               boolean doAssert)
      throws Exception {
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        key, id);
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);
    if (doAssert) {
      assertNotNull(omKeyInfo, "Failed to find key in OpenKeyTable");
    }
    return omKeyInfo;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  protected SnapshotInfo createSnapshot(String snapshot) throws Exception {
    return createSnapshot(volumeName, bucketName, snapshot);
  }

  /**
   * Create snapshot and checkpoint directory.
   */
  protected SnapshotInfo createSnapshot(String volume, String bucket, String snapshotName) throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    BatchOperation batchOperation = omMetadataManager.getStore()
        .initBatchOperation();
    OzoneManagerProtocolProtos.OMRequest omRequest = OMRequestTestUtils
        .createSnapshotRequest(volume, bucket, snapshotName);
    // Pre-Execute OMSnapshotCreateRequest.
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest, ozoneManager);

    // validateAndUpdateCache OMSnapshotCreateResponse.
    OMSnapshotCreateResponse omClientResponse = (OMSnapshotCreateResponse)
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1L);
    // Add to batch and commit to DB.
    omClientResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getTransactionInfoTable().putWithBatch(batchOperation, TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(1L, 1L));
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    batchOperation.close();

    String key = SnapshotInfo.getTableKey(volume,
        bucket, snapshotName);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    return snapshotInfo;
  }
}
