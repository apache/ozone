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
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
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
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * Test for {@link ContainerImporter}.
 */
class TestContainerImporter {

  @TempDir
  private File tempDir;

  private OzoneConfiguration conf;
  private VolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData containerData;
  private KeyValueContainer container;
  private ContainerController controllerMock;
  private long containerId = 1;
  private ContainerSet containerSet;
  private MutableVolumeSet volumeSet;
  private ContainerImporter containerImporter;

  @BeforeEach
  void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.getAbsolutePath());
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    // create container
    containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test");
    container = new KeyValueContainer(containerData, conf);
    controllerMock = mock(ContainerController.class);
    when(controllerMock.importContainer(any(ContainerData.class), any(), any())).thenAnswer(
        i -> {
          containerData = i.getArgument(0);
          container = new KeyValueContainer(containerData, conf);
          return container;
        });
    containerSet = spy(newContainerSet(0));
    volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    // create containerImporter object
    containerImporter = new ContainerImporter(conf,
        containerSet, controllerMock, volumeSet, volumeChoosingPolicy);
  }

  @Test
  void importSameContainerWhenAlreadyImport() throws Exception {
    containerSet.addContainer(container);
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
    // mock controller for return container data with delay
    Semaphore semaphore = new Semaphore(0);
    when(controllerMock.importContainer(any(), any(), any()))
        .thenAnswer((invocation) -> {
          semaphore.acquire();
          return container;
        });
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
    // create container with mock to return different checksums
    KeyValueContainerData data = spy(new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test"));
    // mock to return different checksum
    when(data.getContainerFileChecksum()).thenReturn("checksum1", "checksum2");
    // create containerImporter object with mock
    ContainerImporter importer = spy(new ContainerImporter(conf,
        containerSet, controllerMock, volumeSet, volumeChoosingPolicy));

    TarContainerPacker packer = mock(TarContainerPacker.class);
    when(packer.unpackContainerDescriptor(any())).thenReturn("test".getBytes(
        StandardCharsets.UTF_8));
    when(importer.getPacker(any())).thenReturn(packer);

    doReturn(data).when(importer).getKeyValueContainerData(any(byte[].class));
    when(importer.getImportContainerProgress()).thenReturn(new HashSet<>());

    File tarFile = File.createTempFile("temp_" + System
        .currentTimeMillis(), ".tar");

    StorageContainerException scException =
        assertThrows(StorageContainerException.class,
            () -> importer.importContainer(containerId,
                tarFile.toPath(), null, NO_COMPRESSION));
    Assertions.assertTrue(scException.getMessage().
        contains("Container checksum error"));
  }

  @Test
  public void testImportContainerTriggersOnDemandScanner() throws Exception {
    // create containerImporter object
    HddsVolume targetVolume = mock(HddsVolume.class);
    doNothing().when(targetVolume).incrementUsedSpace(anyLong());

    // import the container
    File tarFile = containerTarFile(containerId, containerData);
    containerImporter.importContainer(containerId, tarFile.toPath(),
        targetVolume, NO_COMPRESSION);

    verify(containerSet, atLeastOnce()).scanContainer(containerId, "Imported container");
  }

  @Test
  public void testImportContainerFailureTriggersVolumeScan() throws Exception {
    HddsVolume targetVolume = mock(HddsVolume.class);
    try (MockedStatic<StorageVolumeUtil> mockedStatic = mockStatic(StorageVolumeUtil.class)) {
      when(controllerMock.importContainer(any(ContainerData.class), any(), any())).thenThrow(new IOException());
      // import the container
      File tarFile = containerTarFile(containerId, containerData);
      assertThrows(IOException.class, () -> containerImporter.importContainer(containerId, tarFile.toPath(),
          targetVolume, NO_COMPRESSION));
      mockedStatic.verify(() -> StorageVolumeUtil.onFailure(any()), times(1));
    }
  }

  @Test
  public void testImportContainerResetsLastScanTime() throws Exception {
    containerData.setDataScanTimestamp(Time.monotonicNow());

    // create containerImporter object
    HddsVolume targetVolume = mock(HddsVolume.class);
    doNothing().when(targetVolume).incrementUsedSpace(anyLong());

    // import the container
    File tarFile = containerTarFile(containerId, containerData);
    containerImporter.importContainer(containerId, tarFile.toPath(),
        targetVolume, NO_COMPRESSION);

    assertEquals(Optional.empty(), containerData.lastDataScanTime());
  }

  private File containerTarFile(long id, ContainerData data) throws IOException {
    File yamlFile = new File(tempDir, "container.yaml");
    ContainerDataYaml.createContainerFile(data, yamlFile);
    File tarFile = new File(tempDir,
        ContainerUtils.getContainerTarName(id));
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
