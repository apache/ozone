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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
    importer = spy(importer);
    downloader = mock(SimpleContainerDownloader.class);
    replicator = new DownloadAndImportReplicator(conf, containerSet, importer,
        downloader);
    containerMaxSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    // Mock the space reservation logic to be independent of the
    // importer's implementation details.
    doAnswer(invocation -> 2 * (long) invocation.getArgument(0))
        .when(importer).getRequiredReplicationSpace(anyLong());
    when(importer.getDefaultReplicationSpace()).thenReturn(2 * containerMaxSize);

  }

  /**
   * Provides a stream of different container sizes for tests.
   */
  static Stream<Arguments> replicateSizeProvider() {
    return Stream.of(
        Arguments.of("Null replicate size (fallback to default)", null),
        Arguments.of("Normal 2GB", 2L * 1024L * 1024L * 1024L),
        Arguments.of("Overallocated 20GB", 20L * 1024L * 1024L * 1024L)
    );
  }

  @ParameterizedTest(name = "for {0}")
  @MethodSource("replicateSizeProvider")
  public void testSpaceReservedAndReleasedOnSuccess(String testName, Long replicateSize)
      throws IOException {
    // GIVEN
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();
    long expectedReservedSpace = replicateSize != null ?
        importer.getRequiredReplicationSpace(replicateSize) :
        importer.getDefaultReplicationSpace();

    // Mock downloader to check reservation and return a dummy path
    Path dummyPath = tempDir.toPath().resolve("dummy.tar");
    Files.createFile(dummyPath);
    when(downloader.getContainerDataFromReplicas(anyLong(), any(), any(), any()))
        .thenAnswer(invocation -> {
          // Check that space was reserved before download attempt
          assertEquals(initialCommittedBytes + expectedReservedSpace,
              volume.getCommittedBytes());
          return dummyPath;
        });
    // Mock the import itself to avoid file system operations
    doAnswer(invocation -> null).when(importer)
        .importContainer(anyLong(), any(), any(), any());

    ReplicationTask task = createTask(containerId, replicateSize);

    // WHEN
    // The replicator should reserve space, "download", "import", and then
    // release the space in the finally block.
    replicator.replicate(task);

    // THEN
    assertEquals(AbstractReplicationTask.Status.DONE, task.getStatus());
    assertEquals(initialCommittedBytes, volume.getCommittedBytes(),
        "Committed space should be released on success");
  }

  @ParameterizedTest(name = "for {0}")
  @MethodSource("replicateSizeProvider")
  public void testCommitSpaceReleasedOnReplicationFailure(String testName, Long replicateSize)
      throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();
    long expectedReservedSpace = replicateSize != null ?
        importer.getRequiredReplicationSpace(replicateSize) :
        importer.getDefaultReplicationSpace();

    // Mock downloader to throw exception
    Semaphore semaphore = new Semaphore(1);
    when(downloader.getContainerDataFromReplicas(anyLong(), any(), any(), any()))
        .thenAnswer(invocation -> {
          semaphore.acquire();
          throw new IOException("Download failed");
        });

    // Create task with the parameterized replicateSize
    ReplicationTask task = createTask(containerId, replicateSize);

    // Acquire semaphore so that container import will pause before downloading.
    semaphore.acquire();
    CompletableFuture.runAsync(() -> {
      assertThrows(IOException.class, () -> replicator.replicate(task));
    });

    // Wait such that first container import reserve space
    GenericTestUtils.waitFor(() ->
        volume.getCommittedBytes() > initialCommittedBytes,
        1000, 50000);
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes + expectedReservedSpace);
    semaphore.release();

    GenericTestUtils.waitFor(() ->
        volume.getCommittedBytes() == initialCommittedBytes,
        1000, 50000);

    // Verify commit space is released
    assertEquals(initialCommittedBytes, volume.getCommittedBytes());
  }

  private ReplicationTask createTask(long containerId, Long replicateSize) {
    ReplicateContainerCommand cmd = ReplicateContainerCommand.fromSources(
        containerId,
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()));
    if (replicateSize != null) {
      cmd.setReplicateSize(replicateSize);
    }
    return new ReplicationTask(cmd, replicator);
  }
}
