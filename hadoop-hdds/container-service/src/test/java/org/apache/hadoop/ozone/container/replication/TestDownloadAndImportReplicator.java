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

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for DownloadAndImportReplicator.
 */
@Timeout(300)
public class TestDownloadAndImportReplicator {

  @TempDir
  private File tempDir;

  private MutableVolumeSet volumeSet;
  private SimpleContainerDownloader downloader;
  private DownloadAndImportReplicator replicator;
  private long containerMaxSize;

  @BeforeEach
  void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.getAbsolutePath());
    VolumeChoosingPolicy volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    ContainerSet containerSet = newContainerSet(0);
    volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    ContainerImporter importer = new ContainerImporter(conf, containerSet,
        mock(ContainerController.class), volumeSet, volumeChoosingPolicy);
    downloader = mock(SimpleContainerDownloader.class);
    replicator = new DownloadAndImportReplicator(conf, containerSet, importer,
        downloader);
    containerMaxSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  @Test
  public void testCommitSpaceReleasedOnReplicationFailure() throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();

    // Mock downloader to throw exception
    Semaphore semaphore = new Semaphore(1);
    when(downloader.getContainerDataFromReplicas(anyLong(), any(), any(), any()))
        .thenAnswer(invocation -> {
          semaphore.acquire();
          throw new IOException("Download failed");
        });

    ReplicationTask task = new ReplicationTask(containerId,
        Collections.singletonList(mock(DatanodeDetails.class)), replicator);

    // Acquire semaphore so that container import will pause before downloading.
    semaphore.acquire();
    CompletableFuture.runAsync(() -> {
      assertThrows(IOException.class, () -> replicator.replicate(task));
    });

    // Wait such that first container import reserve space
    GenericTestUtils.waitFor(() ->
        volume.getCommittedBytes() > initialCommittedBytes,
        1000, 50000);
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes + 2 * containerMaxSize);
    semaphore.release();

    GenericTestUtils.waitFor(() ->
        volume.getCommittedBytes() == initialCommittedBytes,
        1000, 50000);

    // Verify commit space is released
    assertEquals(initialCommittedBytes, volume.getCommittedBytes());
  }
}
