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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.apache.ozone.test.SpyOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Timeout;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.apache.commons.io.output.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.toTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link PushReplicator}.
 */
@Timeout(30)
class TestPushReplicator {

  private OzoneConfiguration conf;

  @BeforeEach
  void setup() {
    conf = new OzoneConfiguration();
  }

  @ParameterizedTest
  @EnumSource
  void uploadCompletesNormally(CopyContainerCompression compression)
      throws IOException {
    // GIVEN
    compression.setOn(conf);
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    Consumer<CompletableFuture<Void>> completion =
        fut -> fut.complete(null);
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion, compression);
    ReplicationTask task = new ReplicationTask(toTarget(containerID, target),
        subject);

    // WHEN
    subject.replicate(task);

    // THEN
    assertEquals(Status.DONE, task.getStatus());
    output.assertClosedExactlyOnce();
  }


  @Test
  void uploadFailsWithException() throws IOException {
    // GIVEN
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    Consumer<CompletableFuture<Void>> completion =
        fut -> fut.completeExceptionally(new Exception("testing"));
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion, NO_COMPRESSION);
    ReplicationTask task = new ReplicationTask(toTarget(containerID, target),
        subject);

    // WHEN
    subject.replicate(task);

    // THEN
    assertEquals(Status.FAILED, task.getStatus());
    output.assertClosedExactlyOnce();
  }

  @Test
  void packFailsWithException() throws IOException {
    // GIVEN
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    Consumer<CompletableFuture<Void>> completion = fut -> {
      throw new RuntimeException();
    };
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion, NO_COMPRESSION);
    ReplicationTask task = new ReplicationTask(toTarget(containerID, target),
        subject);

    // WHEN
    subject.replicate(task);

    // THEN
    assertEquals(Status.FAILED, task.getStatus());
    output.assertClosedExactlyOnce();
  }

  @Test
  void importSameContainerWhenAlreadyImport() throws Exception {
    long containerId = 1;
    // create container
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test");
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    ContainerController controllerMock = mock(ContainerController.class);
    // create containerImporter object
    ContainerSet containerSet = new ContainerSet(0);
    containerSet.addContainer(container);
    MutableVolumeSet volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    ContainerImporter containerImporter = new ContainerImporter(conf,
        containerSet, controllerMock, volumeSet);
    File tarFile = new File("dummy.tar");
    // second import should fail immediately
    try {
      containerImporter.importContainer(containerId, tarFile.toPath(),
          null, NO_COMPRESSION);
      assertFalse(true, "exception should occur");
    } catch (StorageContainerException ex) {
      assertTrue(ex.getResult().equals(
          ContainerProtos.Result.CONTAINER_EXISTS));
    }
  }

  @Test
  void importSameContainerWhenFirstInProgress() throws Exception {
    long containerId = 1;
    // create container
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test");
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    // mock controller for return container data with delay
    ContainerController controllerMock = mock(ContainerController.class);
    when(controllerMock.importContainer(any(), any(), any()))
        .thenAnswer((invocation) -> {
          Thread.sleep(5000);
          return container;
        });
    // create containerImporter object
    ContainerSet containerSet = new ContainerSet(0);
    MutableVolumeSet volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    ContainerImporter containerImporter = new ContainerImporter(conf,
        containerSet, controllerMock, volumeSet);
    // run import async first time having delay
    File tarFile = containerTarFile(containerId, containerData);
    CompletableFuture.runAsync(() -> {
      try {
        containerImporter.importContainer(containerId, tarFile.toPath(),
            null, NO_COMPRESSION);
      } catch (Exception ex) {
        // do nothing
      }
    });
    Thread.sleep(1000);
    // run import second time and should fail immediately as
    // first import in progress
    try {
      containerImporter.importContainer(containerId, tarFile.toPath(),
          null, NO_COMPRESSION);
      assertFalse(true, "exception should occur");
    } catch (StorageContainerException ex) {
      assertTrue(ex.getResult().equals(
          ContainerProtos.Result.CONTAINER_EXISTS));
    }
  }

  private File containerTarFile(
      long containerId, ContainerData containerData) throws IOException {
    TemporaryFolder tempFolder = new TemporaryFolder();
    tempFolder.create();
    File yamlFile = tempFolder.newFile("container.yaml");
    ContainerDataYaml.createContainerFile(
        ContainerProtos.ContainerType.KeyValueContainer, containerData,
        yamlFile);
    File tarFile = tempFolder.newFile(
        ContainerUtils.getContainerTarName(containerId));
    try (FileOutputStream output = new FileOutputStream(tarFile)) {
      TarArchiveOutputStream archive = new TarArchiveOutputStream(output);
      ArchiveEntry entry = archive.createArchiveEntry(yamlFile,
          "container.yaml");
      archive.putArchiveEntry(entry);
      try (InputStream input = new FileInputStream(yamlFile)) {
        IOUtils.copy(input, archive);
      }
      archive.closeArchiveEntry();
    }
    return tarFile;
  }

  private static long randomContainerID() {
    return ThreadLocalRandom.current().nextLong();
  }

  private ContainerReplicator createSubject(
      long containerID, DatanodeDetails target, OutputStream outputStream,
      Consumer<CompletableFuture<Void>> completion,
      CopyContainerCompression compression
  ) throws IOException {
    ContainerReplicationSource source = mock(ContainerReplicationSource.class);
    ContainerUploader uploader = mock(ContainerUploader.class);
    ArgumentCaptor<CompletableFuture<Void>> futureArgument =
        ArgumentCaptor.forClass(CompletableFuture.class);
    ArgumentCaptor<CopyContainerCompression> compressionArgument =
        ArgumentCaptor.forClass(CopyContainerCompression.class);

    when(
        uploader.startUpload(eq(containerID), eq(target),
            futureArgument.capture(), compressionArgument.capture()
        ))
        .thenReturn(outputStream);

    doAnswer(invocation -> {
      compressionArgument.getAllValues().forEach(
          c -> assertEquals(compression, c)
      );
      completion.accept(futureArgument.getValue());
      return null;
    })
        .when(source)
        .copyData(eq(containerID), any(), compressionArgument.capture());

    return new PushReplicator(conf, source, uploader);
  }

}
