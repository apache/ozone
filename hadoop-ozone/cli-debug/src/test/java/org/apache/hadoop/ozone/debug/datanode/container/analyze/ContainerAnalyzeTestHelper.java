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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared on-disk DataNode volume and container directory setup for analyze tests.
 */
final class ContainerAnalyzeTestHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerAnalyzeTestHelper.class);

  private final Path tempDir;
  private final OzoneConfiguration conf;
  private final String clusterId;
  private final String datanodeUuid;

  ContainerAnalyzeTestHelper(Path tempDir, OzoneConfiguration conf,
      String clusterId, String datanodeUuid) {
    this.tempDir = tempDir;
    this.conf = conf;
    this.clusterId = clusterId;
    this.datanodeUuid = datanodeUuid;
  }

  File formatVolume(String name) throws IOException {
    File volumeRoot = tempDir.resolve(name).toFile();
    HddsVolume volume = new HddsVolume.Builder(volumeRoot.getAbsolutePath())
        .conf(conf)
        .datanodeUuid(datanodeUuid)
        .clusterID(clusterId)
        .build();
    StorageVolumeUtil.checkVolume(volume, clusterId, clusterId, conf, LOG, null);
    return volumeRoot;
  }

  Path containerTopDir(File volumeRoot) {
    return volumeRoot.toPath()
        .resolve(HddsVolume.HDDS_VOLUME_DIR)
        .resolve(clusterId)
        .resolve(Storage.STORAGE_DIR_CURRENT)
        .resolve("containerDir0");
  }

  String containerPath(File volumeRoot, long containerId) {
    return containerTopDir(volumeRoot).resolve(Long.toString(containerId)).toFile().getAbsolutePath();
  }

  void createContainerDirectory(File volumeRoot, long containerId,
      boolean writeMetadata, long metadataContainerId) throws IOException {
    Path containerBase = containerTopDir(volumeRoot).resolve(Long.toString(containerId));
    Files.createDirectories(containerBase.resolve("metadata"));
    Files.createDirectories(containerBase.resolve("chunks"));

    if (writeMetadata) {
      KeyValueContainerData containerData = new KeyValueContainerData(
          metadataContainerId,
          ContainerLayoutVersion.FILE_PER_BLOCK,
          (long) StorageUnit.GB.toBytes(1),
          UUID.randomUUID().toString(),
          datanodeUuid);
      containerData.setChunksPath(containerBase.resolve("chunks").toString());
      containerData.setMetadataPath(containerBase.resolve("metadata").toString());
      File containerFile = ContainerUtils.getContainerFile(containerBase.toFile());
      ContainerDataYaml.createContainerFile(containerData, containerFile);
    }
  }

  void createEmptyContainerFileOnVolume(File volumeRoot, long containerId) throws IOException {
    Path containerBase = containerTopDir(volumeRoot).resolve(Long.toString(containerId));
    Files.createDirectories(containerBase.resolve("metadata"));
    Files.createDirectories(containerBase.resolve("chunks"));
    Files.createFile(ContainerUtils.getContainerFile(containerBase.toFile()).toPath());
  }

  void corruptVersionFile(File volumeRoot) throws IOException {
    File hddsRoot = new File(volumeRoot, HddsVolume.HDDS_VOLUME_DIR);
    File versionFile = StorageVolumeUtil.getVersionFile(hddsRoot);
    Files.write(versionFile.toPath(), new byte[0]);
  }
}
