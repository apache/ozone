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
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
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
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for {@link ContainerImporter}.
 */
class TestContainerImporter {

  @TempDir
  private File tempDir;

  private OzoneConfiguration conf;

  @BeforeEach
  void setup() {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.getAbsolutePath());
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
    ContainerSet containerSet = newContainerSet(0);
    containerSet.addContainer(container);
    MutableVolumeSet volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    ContainerImporter containerImporter = new ContainerImporter(conf,
        containerSet, controllerMock, volumeSet);
    File tarFile = new File("dummy.tar");
    // second import should fail immediately
    StorageContainerException ex = assertThrows(StorageContainerException.class,
        () -> containerImporter.importContainer(containerId, tarFile.toPath(),
            null, NO_COMPRESSION));
    assertEquals(ContainerProtos.Result.CONTAINER_EXISTS, ex.getResult());
    assertThat(ex.getMessage()).contains("Container already exists");
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
    Semaphore semaphore = new Semaphore(0);
    when(controllerMock.importContainer(any(), any(), any()))
        .thenAnswer((invocation) -> {
          semaphore.acquire();
          return container;
        });
    // create containerImporter object
    ContainerSet containerSet = newContainerSet(0);
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
    GenericTestUtils.waitFor(semaphore::hasQueuedThreads, 10, 5000);
    // run import second time and should fail immediately as
    // first import in progress
    StorageContainerException ex = assertThrows(
        StorageContainerException.class,
        () -> containerImporter.importContainer(containerId, tarFile.toPath(),
            null, NO_COMPRESSION));
    assertEquals(ContainerProtos.Result.CONTAINER_EXISTS,
        ex.getResult());
    assertThat(ex.getMessage()).contains("import in progress");
    semaphore.release();
  }

  @Test
  public void testInconsistentChecksumContainerShouldThrowError() throws Exception {
    // create container
    long containerId = 1;
    KeyValueContainerData containerData = spy(new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test"));
    // mock to return different checksum
    when(containerData.getChecksum()).thenReturn("checksum1", "checksum2");
    doNothing().when(containerData).setChecksumTo0ByteArray();
    // create containerImporter object
    ContainerController controllerMock = mock(ContainerController.class);
    ContainerSet containerSet = newContainerSet(0);
    MutableVolumeSet volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    ContainerImporter containerImporter = spy(new ContainerImporter(conf,
        containerSet, controllerMock, volumeSet));

    TarContainerPacker packer = mock(TarContainerPacker.class);
    when(packer.unpackContainerDescriptor(any())).thenReturn("test".getBytes(
        StandardCharsets.UTF_8));
    when(containerImporter.getPacker(any())).thenReturn(packer);

    doReturn(containerData).when(containerImporter).getKeyValueContainerData(any(byte[].class));
    when(containerImporter.getImportContainerProgress()).thenReturn(new HashSet<>());

    File tarFile = File.createTempFile("temp_" + System
        .currentTimeMillis(), ".tar");

    StorageContainerException scException =
        assertThrows(StorageContainerException.class,
            () -> containerImporter.importContainer(containerId,
                tarFile.toPath(), null, NO_COMPRESSION));
    Assertions.assertTrue(scException.getMessage().
        contains("Container checksum error"));
  }

  private File containerTarFile(
      long containerId, ContainerData containerData) throws IOException {
    File yamlFile = new File(tempDir, "container.yaml");
    ContainerDataYaml.createContainerFile(containerData, yamlFile);
    File tarFile = new File(tempDir,
        ContainerUtils.getContainerTarName(containerId));
    try (OutputStream output = Files.newOutputStream(tarFile.toPath())) {
      ArchiveOutputStream<TarArchiveEntry> archive = new TarArchiveOutputStream(output);
      TarArchiveEntry entry = archive.createArchiveEntry(yamlFile,
          "container.yaml");
      archive.putArchiveEntry(entry);
      try (InputStream input = Files.newInputStream(yamlFile.toPath())) {
        IOUtils.copy(input, archive);
      }
      archive.closeArchiveEntry();
    }
    return tarFile;
  }
}
