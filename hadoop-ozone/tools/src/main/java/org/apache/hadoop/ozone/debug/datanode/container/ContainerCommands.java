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

package org.apache.hadoop.ozone.debug.datanode.container;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerReader;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * Subcommand to group container replica related operations.
 */
@Command(
    name = "container",
    description = "Container replica specific operations" +
        " to be executed on datanodes only",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        ListSubcommand.class,
        InfoSubcommand.class,
        ExportSubcommand.class,
        InspectSubcommand.class
    })
public class ContainerCommands extends AbstractSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerCommands.class);

  private MutableVolumeSet volumeSet;

  private ContainerController controller;

  public void loadContainersFromVolumes() throws IOException {
    OzoneConfiguration conf = getOzoneConf();

    validateAllStorageDirectories(conf);

    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);

    ContainerMetrics metrics = ContainerMetrics.create(conf);

    String firstStorageDir = getFirstStorageDir(conf);

    String datanodeUuid = getDatanodeUUID(firstStorageDir, conf);

    String clusterId = getClusterId(firstStorageDir);

    volumeSet = new MutableVolumeSet(datanodeUuid, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    VolumeChoosingPolicy volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);

    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      MutableVolumeSet dbVolumeSet =
          HddsServerUtil.getDatanodeDbDirs(conf).isEmpty() ? null :
          new MutableVolumeSet(datanodeUuid, conf, null,
              StorageVolume.VolumeType.DB_VOLUME, null);
      // load rocksDB with readOnly mode, otherwise it will fail.
      HddsVolumeUtil.loadAllHddsVolumeDbStore(
          volumeSet, dbVolumeSet, true, LOG);
    }

    Map<ContainerProtos.ContainerType, Handler> handlers = new HashMap<>();

    for (ContainerProtos.ContainerType containerType
        : ContainerProtos.ContainerType.values()) {
      final Handler handler =
          Handler.getHandlerForContainerType(
              containerType,
              conf,
              datanodeUuid,
              containerSet,
              volumeSet,
              volumeChoosingPolicy,
              metrics,
              containerReplicaProto -> {
              },
              // Since this is an Ozone debug CLI, this instance is not part of a running datanode.
              new ContainerChecksumTreeManager(conf));
      handler.setClusterID(clusterId);
      handlers.put(containerType, handler);
    }

    controller = new ContainerController(containerSet, handlers);

    List<HddsVolume> volumes = StorageVolumeUtil.getHddsVolumesList(
        volumeSet.getVolumesList());
    Iterator<HddsVolume> volumeSetIterator = volumes.iterator();

    LOG.info("Starting the read all the container metadata");

    while (volumeSetIterator.hasNext()) {
      HddsVolume volume = volumeSetIterator.next();
      LOG.info("Loading container metadata from volume " + volume.toString());
      final ContainerReader reader =
          new ContainerReader(volumeSet, volume, containerSet, conf, false);
      reader.run();
    }

    LOG.info("All the container metadata is loaded.");
  }

  public MutableVolumeSet getVolumeSet() {
    return this.volumeSet;
  }

  public ContainerController getController() {
    return this.controller;
  }

  private String getClusterId(String storageDir) throws IOException {
    Objects.requireNonNull(storageDir, "storageDir == null");
    try (Stream<Path> stream = Files.list(Paths.get(storageDir, "hdds"))) {
      final Path firstStorageDirPath = stream.filter(Files::isDirectory)
          .findFirst().get().getFileName();
      if (firstStorageDirPath == null) {
        throw new IllegalArgumentException(
            "HDDS storage dir couldn't be identified!");
      }
      return firstStorageDirPath.toString();
    }
  }

  private String getDatanodeUUID(String storageDir, ConfigurationSource config)
      throws IOException {

    final File versionFile = new File(storageDir, "hdds/VERSION");

    Properties props = DatanodeVersionFile.readFrom(versionFile);
    if (props.isEmpty()) {
      throw new InconsistentStorageStateException(
          "Version file " + versionFile + " is missing");
    }

    return StorageVolumeUtil
        .getProperty(props, OzoneConsts.DATANODE_UUID, versionFile);
  }

  private String getFirstStorageDir(ConfigurationSource config)
      throws IOException {
    final Collection<String> storageDirs =
        HddsServerUtil.getDatanodeStorageDirs(config);

    return
        StorageLocation.parse(storageDirs.iterator().next())
            .getUri().getPath();
  }

  public static void outputContainer(ContainerData data) throws IOException {
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(data));
  }

  private void validateAllStorageDirectories(ConfigurationSource config) throws IOException {
    final Collection<String> storageDirs = HddsServerUtil.getDatanodeStorageDirs(config);
    LOG.info("Configured storage directories for '{}': {}", ScmConfigKeys.HDDS_DATANODE_DIR_KEY, storageDirs);

    List<String> missingDirs = new ArrayList<>();
    for (String storageDir : storageDirs) {
      String storageDirPath = StorageLocation.parse(storageDir).getUri().getPath();

      if (!new File(storageDirPath).exists()) {
        missingDirs.add(storageDirPath);
      }
    }
    if (!missingDirs.isEmpty()) {
      String errorMsg = String.join(", ", missingDirs) +
          "' configured in '" + ScmConfigKeys.HDDS_DATANODE_DIR_KEY +
          "' does not exist. Please provide the correct value for config.";
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }
}
