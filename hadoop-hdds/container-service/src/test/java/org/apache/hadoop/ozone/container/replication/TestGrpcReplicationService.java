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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.toTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link GrpcReplicationService}.
 */
class TestGrpcReplicationService {

  @TempDir
  private Path tempDir;

  private ReplicationServer replicationServer;
  private OzoneConfiguration conf;
  private ContainerController containerController;
  private DatanodeDetails datanode;
  private static final long CONTAINER_ID = 123456L;
  private final AtomicLong pushContainerId = new AtomicLong();

  @BeforeEach
  public void setUp() throws Exception {
    init();
  }

  public void init() throws Exception {
    conf = new OzoneConfiguration();

    ReplicationServer.ReplicationConfig replicationConfig =
        conf.getObject(ReplicationServer.ReplicationConfig.class);

    SecurityConfig secConf = new SecurityConfig(conf);

    ContainerSet containerSet = newContainerSet();

    DatanodeDetails.Builder dn =
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .setHostName("localhost").setIpAddress("127.0.0.1")
            .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
            .setPersistedOpStateExpiry(0);
    DatanodeDetails.Port containerPort =
        DatanodeDetails.newStandalonePort(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
    DatanodeDetails.Port ratisPort =
        DatanodeDetails.newRatisPort(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT_DEFAULT);
    DatanodeDetails.Port replicationPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.REPLICATION,
            replicationConfig.getPort());
    DatanodeDetails.Port streamPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM,
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_PORT_DEFAULT);
    dn.addPort(containerPort);
    dn.addPort(ratisPort);
    dn.addPort(replicationPort);
    dn.addPort(streamPort);

    datanode = dn.build();

    final String testDir =
        Files.createDirectory(tempDir.resolve("VolumeDir")).toString();

    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getVolumesList()).thenReturn(Collections.singletonList(
        new HddsVolume.Builder(testDir).conf(conf).build()));

    Handler containerHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanode.getUuidString(), containerSet, volumeSet);

    containerController = new ContainerController(containerSet,
        Collections.singletonMap(
            ContainerProtos.ContainerType.KeyValueContainer, containerHandler));

    KeyValueContainerData data = new KeyValueContainerData(
        CONTAINER_ID,
        ContainerLayoutVersion.FILE_PER_BLOCK, GB, UUID.randomUUID().toString(),
        datanode.getUuidString());
    KeyValueContainer container = new KeyValueContainer(data, conf);
    StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
        .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
        "test-replication");
    containerSet.addContainer(container);
    container.close();

    ContainerImporter importer = mock(ContainerImporter.class);
    doAnswer(invocation -> {
      pushContainerId.set((long) invocation.getArguments()[0]);
      return null;
    }).when(importer).importContainer(anyLong(), any(), any(), any());
    doReturn(true).when(importer).isAllowedContainerImport(eq(
        CONTAINER_ID));
    when(importer.chooseNextVolume(anyLong())).thenReturn(new HddsVolume.Builder(
        Files.createDirectory(tempDir.resolve("ImporterDir")).toString()).conf(
        conf).build());

    replicationServer =
        new ReplicationServer(containerController, replicationConfig, secConf,
            null, importer, datanode.threadNamePrefix());
    replicationServer.start();
  }

  @AfterEach
  public void cleanup() {
    replicationServer.stop();
  }

  @Test
  public void testDownload() throws IOException {
    SimpleContainerDownloader downloader =
        new SimpleContainerDownloader(conf, null);
    Path downloadDir = Files.createDirectory(tempDir.resolve("DownloadDir"));
    Path result = downloader.getContainerDataFromReplicas(
        CONTAINER_ID,
        Collections.singletonList(datanode), downloadDir,
        CopyContainerCompression.NO_COMPRESSION);

    assertTrue(result.toString().startsWith(downloadDir.toString()));

    File[] files = downloadDir.toFile().listFiles();

    assertNotNull(files);
    assertEquals(files.length, 1);

    assertTrue(files[0].getName().startsWith("container-" +
        CONTAINER_ID + "-"));

    downloader.close();
  }

  @Test
  public void testUpload() {
    ContainerReplicationSource source =
        new OnDemandContainerReplicationSource(containerController);

    GrpcContainerUploader uploader = new GrpcContainerUploader(conf, null, containerController);

    PushReplicator pushReplicator = new PushReplicator(conf, source, uploader);

    ReplicationTask task =
        new ReplicationTask(toTarget(CONTAINER_ID, datanode), pushReplicator);

    pushReplicator.replicate(task);

    assertEquals(pushContainerId.get(), CONTAINER_ID);
  }

  @Test
  void closesStreamOnError() {
    // GIVEN
    ContainerReplicationSource source = new ContainerReplicationSource() {
      @Override
      public void prepare(long containerId) {
        // no-op
      }

      @Override
      public void copyData(long containerId, OutputStream destination,
          CopyContainerCompression compression) throws IOException {
        throw new IOException("testing");
      }
    };
    ContainerImporter importer = mock(ContainerImporter.class);
    GrpcReplicationService subject =
        new GrpcReplicationService(source, importer);

    CopyContainerRequestProto request = CopyContainerRequestProto.newBuilder()
        .setContainerID(1)
        .setReadOffset(0)
        .setLen(123)
        .build();
    CallStreamObserver<CopyContainerResponseProto> observer =
        mock(CallStreamObserver.class);
    when(observer.isReady()).thenReturn(true);

    // WHEN
    subject.download(request, observer);

    // THEN
    // onCompleted is called by GrpcOutputStream#close
    // so we indirectly verify that the stream is closed
    verify(observer).onCompleted();
  }

}
