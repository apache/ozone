/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.setupReplicationConfigValidation;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class for key request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMKeyRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OzoneManager ozoneManager;
  protected KeyManager keyManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;
  protected OzoneManagerPrepareState prepareState;

  protected ScmClient scmClient;
  protected OzoneBlockTokenSecretManager ozoneBlockTokenSecretManager;
  protected ScmBlockLocationProtocol scmBlockLocationProtocol;
  protected OMPerformanceMetrics metrics;

  protected static final long CONTAINER_ID = 1000L;
  protected static final long LOCAL_ID = 100L;

  protected String volumeName;
  protected String bucketName;
  protected String keyName;
  protected HddsProtos.ReplicationType replicationType;
  protected HddsProtos.ReplicationFactor replicationFactor;
  protected long clientID;
  protected long scmBlockSize = 1000L;
  protected long dataSize;
  protected Random random;
  protected long txnLogId = 100000L;
  protected long version = 0L;

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  protected OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });


  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = getOzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);
    when(ozoneManager.getBucketInfo(anyString(), anyString())).thenReturn(
        new OmBucketInfo.Builder().setVolumeName("").setBucketName("").build());
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    setupReplicationConfigValidation(ozoneManager, ozoneConfiguration);

    scmClient = Mockito.mock(ScmClient.class);
    ozoneBlockTokenSecretManager =
        Mockito.mock(OzoneBlockTokenSecretManager.class);
    scmBlockLocationProtocol = Mockito.mock(ScmBlockLocationProtocol.class);
    metrics = Mockito.mock(OMPerformanceMetrics.class);
    keyManager = new KeyManagerImpl(ozoneManager, scmClient, ozoneConfiguration,
        metrics);
    when(ozoneManager.getScmClient()).thenReturn(scmClient);
    when(ozoneManager.getBlockTokenSecretManager())
        .thenReturn(ozoneBlockTokenSecretManager);
    when(ozoneManager.getScmBlockSize()).thenReturn(scmBlockSize);
    when(ozoneManager.getPreallocateBlocksMax()).thenReturn(2);
    when(ozoneManager.isGrpcBlockTokenEnabled()).thenReturn(false);
    when(ozoneManager.getOMNodeId()).thenReturn(UUID.randomUUID().toString());
    when(scmClient.getBlockClient()).thenReturn(scmBlockLocationProtocol);
    when(ozoneManager.getKeyManager()).thenReturn(keyManager);

    OmMetadataReader omMetadataReader = Mockito.mock(OmMetadataReader.class);
    when(ozoneManager.getOmMetadataReader()).thenReturn(omMetadataReader);

    prepareState = new OzoneManagerPrepareState(ozoneConfiguration);
    when(ozoneManager.getPrepareState()).thenReturn(prepareState);

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
        anyString(), any(ExcludeList.class))).thenAnswer(invocation -> {
          int num = invocation.getArgument(1);
          List<AllocatedBlock> allocatedBlocks = new ArrayList<>(num);
          for (int i = 0; i < num; i++) {
            blockBuilder.setContainerBlockID(
                new ContainerBlockID(CONTAINER_ID + i, LOCAL_ID + i));
            allocatedBlocks.add(blockBuilder.build());
          }
          return allocatedBlocks;
        });


    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
    replicationFactor = HddsProtos.ReplicationFactor.ONE;
    replicationType = HddsProtos.ReplicationType.RATIS;
    clientID = Time.now();
    dataSize = 1000L;
    random = new Random();
    version = 0L;

    Pair<String, String> volumeAndBucket = Pair.of(volumeName, bucketName);
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenReturn(new ResolvedBucket(volumeAndBucket, volumeAndBucket));
    when(ozoneManager.resolveBucketLink(any(Pair.class),
        any(OMClientRequest.class)))
        .thenReturn(new ResolvedBucket(volumeAndBucket, volumeAndBucket));
    OmSnapshotManager omSnapshotManager = new OmSnapshotManager(ozoneManager);
    when(ozoneManager.getOmSnapshotManager())
        .thenReturn(omSnapshotManager);
  }

  @NotNull
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
      Assert.assertNotNull("Failed to find key in OpenKeyTable", omKeyInfo);
    }
    return omKeyInfo;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }
}
