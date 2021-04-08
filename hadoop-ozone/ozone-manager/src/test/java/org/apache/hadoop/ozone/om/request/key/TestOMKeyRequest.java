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
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.util.Time;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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

  protected ScmClient scmClient;
  protected OzoneBlockTokenSecretManager ozoneBlockTokenSecretManager;
  protected ScmBlockLocationProtocol scmBlockLocationProtocol;

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
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    scmClient = Mockito.mock(ScmClient.class);
    ozoneBlockTokenSecretManager =
        Mockito.mock(OzoneBlockTokenSecretManager.class);
    scmBlockLocationProtocol = Mockito.mock(ScmBlockLocationProtocol.class);
    keyManager = new KeyManagerImpl(ozoneManager, scmClient, ozoneConfiguration,
        "");
    when(ozoneManager.getScmClient()).thenReturn(scmClient);
    when(ozoneManager.getBlockTokenSecretManager())
        .thenReturn(ozoneBlockTokenSecretManager);
    when(ozoneManager.getScmBlockSize()).thenReturn(scmBlockSize);
    when(ozoneManager.getPreallocateBlocksMax()).thenReturn(2);
    when(ozoneManager.isGrpcBlockTokenEnabled()).thenReturn(false);
    when(ozoneManager.getOMNodeId()).thenReturn(UUID.randomUUID().toString());
    when(scmClient.getBlockClient()).thenReturn(scmBlockLocationProtocol);
    when(ozoneManager.getKeyManager()).thenReturn(keyManager);

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setNodes(new ArrayList<>())
        .build();

    AllocatedBlock allocatedBlock =
        new AllocatedBlock.Builder()
            .setContainerBlockID(new ContainerBlockID(CONTAINER_ID, LOCAL_ID))
            .setPipeline(pipeline).build();

    List<AllocatedBlock> allocatedBlocks = new ArrayList<>();

    allocatedBlocks.add(allocatedBlock);

    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(),
        any(HddsProtos.ReplicationType.class),
        any(HddsProtos.ReplicationFactor.class),
        anyString(), any(ExcludeList.class))).thenReturn(allocatedBlocks);


    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
    replicationFactor = HddsProtos.ReplicationFactor.ONE;
    replicationType = HddsProtos.ReplicationType.RATIS;
    clientID = Time.now();
    dataSize = 1000L;
    random = new Random();

    Pair<String, String> volumeAndBucket = Pair.of(volumeName, bucketName);
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenReturn(new ResolvedBucket(volumeAndBucket, volumeAndBucket));
    when(ozoneManager.resolveBucketLink(any(Pair.class),
        any(OMClientRequest.class)))
        .thenReturn(new ResolvedBucket(volumeAndBucket, volumeAndBucket));
  }

  @NotNull
  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }
}
