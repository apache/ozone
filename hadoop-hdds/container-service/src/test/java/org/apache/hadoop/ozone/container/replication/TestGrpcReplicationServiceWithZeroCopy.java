/*
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
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link GrpcReplicationService}.
 */
class TestGrpcReplicationServiceWithZeroCopy {

  @TempDir
  private Path tempDir;

  private ReplicationServer replicationServer;
  private OzoneConfiguration conf;

  private DatanodeDetails datanode;

  private long containerId;

  @BeforeEach
  public void setUp() throws Exception {
    containerId = 123456L;
    conf = new OzoneConfiguration();
    conf.set(ReplicationServer.ReplicationConfig.PREFIX + "." +
        ReplicationServer.ReplicationConfig.ZEROCOPY_ENABLE_KEY, "true");

    ReplicationServer.ReplicationConfig replicationConfig =
        conf.getObject(ReplicationServer.ReplicationConfig.class);

    assertTrue(replicationConfig.isZeroCopyEnable());

    SecurityConfig secConf = new SecurityConfig(conf);

    KeyValueContainerData data = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, GB,
        UUID.randomUUID().toString(), null);

    KeyValueContainer container = new KeyValueContainer(data, conf);

    ContainerSet containerSet = new ContainerSet(1000);
    containerSet.addContainer(container);

    DatanodeDetails.Builder dn =
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .setHostName("localhost").setIpAddress("127.0.0.1")
            .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
            .setPersistedOpStateExpiry(0);
    DatanodeDetails.Port containerPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE,
            OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    DatanodeDetails.Port ratisPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS,
            OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT_DEFAULT);
    DatanodeDetails.Port replicationPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.REPLICATION,
            replicationConfig.getPort());
    DatanodeDetails.Port streamPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM,
            OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_PORT_DEFAULT);
    dn.addPort(containerPort);
    dn.addPort(ratisPort);
    dn.addPort(replicationPort);
    dn.addPort(streamPort);

    datanode = dn.build();

    final String testDir =
        Files.createDirectory(tempDir.resolve("VolumeDir")).toString();

    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getVolumesList()).thenReturn(
        Collections.singletonList(new HddsVolume.Builder(testDir).conf(conf).build()));

    Handler containerHandler = mock(Handler.class);
    ContainerController containerController =
        new ContainerController(containerSet,
            Collections.singletonMap(ContainerProtos.ContainerType.KeyValueContainer,
                containerHandler));

    ContainerImporter importer =
        new ContainerImporter(conf, containerSet, containerController,
            volumeSet);

    replicationServer =
        new ReplicationServer(containerController, replicationConfig, secConf,
            null,
            importer, datanode.threadNamePrefix());
    replicationServer.start();
  }

  @AfterEach
  public void cleanup() {
    replicationServer.stop();
  }

  @Test
  public void testZeroCopyDownload() throws IOException {
    SimpleContainerDownloader downloader =
        new SimpleContainerDownloader(conf, null);
    Path downloadDir = Files.createDirectory(tempDir.resolve("DownloadDir"));
    Path result = downloader.getContainerDataFromReplicas(containerId,
        Collections.singletonList(datanode), downloadDir,
        CopyContainerCompression.NO_COMPRESSION);

    assertTrue(result.toString().startsWith(downloadDir.toString()));

    File[] files = downloadDir.toFile().listFiles();

    assertNotNull(files);
    assertEquals(files.length, 1);

    assertTrue(files[0].getName().startsWith("container-" + containerId + "-"));

    downloader.close();
  }

  @Test
  public void testZeroCopyUpload() throws IOException {
    GrpcContainerUploader uploader =
        new GrpcContainerUploader(conf, null);

    CompletableFuture<Void> callback = new CompletableFuture<>();
    OutputStream out = uploader.startUpload(containerId,
        datanode, callback,
        CopyContainerCompression.NO_COMPRESSION);
    out.close();
  }
}
