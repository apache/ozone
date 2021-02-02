/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerReader;
import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;

import com.google.common.base.Preconditions;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "export-container",
    description = "Export one container to a tarball")
@MetaInfServices(SubcommandWithParent.class)
public class ExportContainer implements SubcommandWithParent, Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ExportContainer.class);
  @ParentCommand
  private GenericParentCommand parent;

  @CommandLine.Option(names = {"--container"},
      required = true,
      description = "Container Id")
  private long containerId;

  @CommandLine.Option(names = {"--dest"},
      defaultValue = "/tmp",
      description = "Destination directory")
  private String destination;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  public Void call() throws Exception {

    ConfigurationSource conf = parent.createOzoneConfiguration();

    ContainerSet containerSet = new ContainerSet();

    ContainerMetrics metrics = ContainerMetrics.create(conf);

    String firstStorageDir = getFirstStorageDir(conf);

    String datanodeUuid = getDatanodeUUID(firstStorageDir, conf);

    String scmId = getScmId(firstStorageDir);

    MutableVolumeSet volumeSet = new MutableVolumeSet(datanodeUuid, conf);

    Map<ContainerType, Handler> handlers = new HashMap<>();

    for (ContainerType containerType : ContainerType.values()) {
      final Handler handler =
          Handler.getHandlerForContainerType(
              containerType,
              conf,
              datanodeUuid,
              containerSet,
              volumeSet,
              metrics,
              containerReplicaProto -> {
              });
      handler.setScmID(scmId);
      handlers.put(containerType, handler);
    }

    ContainerController controller =
        new ContainerController(containerSet, handlers);

    final ContainerReplicationSource replicationSource =
        new OnDemandContainerReplicationSource(controller);

    Iterator<HddsVolume> volumeSetIterator = volumeSet.getVolumesList()
        .iterator();

    LOG.info("Starting the read all the container metadata");

    while (volumeSetIterator.hasNext()) {
      HddsVolume volume = volumeSetIterator.next();
      LOG.info("Loading container metadata from volume " + volume.toString());
      final ContainerReader reader =
          new ContainerReader(volumeSet, volume, containerSet, conf);
      reader.run();
    }

    LOG.info("All the container metadata is loaded. Starting to replication");

    replicationSource.prepare(containerId);
    LOG.info("Preparation is done");

    final File destinationFile =
        new File(destination, "container-" + containerId + ".tar.gz");
    try (FileOutputStream fos = new FileOutputStream(destinationFile)) {
      replicationSource.copyData(containerId, fos);
    }
    LOG.info("Container is exported to {}", destinationFile);

    return null;
  }

  public String getScmId(String storageDir) throws IOException {
    Preconditions.checkNotNull(storageDir);
    final Path firstStorageDirPath = Files.list(Paths.get(storageDir, "hdds"))
        .filter(Files::isDirectory)
        .findFirst().get().getFileName();
    if (firstStorageDirPath == null) {
      throw new IllegalArgumentException(
          "HDDS storage dir couldn't be identified!");
    }
    return firstStorageDirPath.toString();
  }

  public String getDatanodeUUID(String storageDir, ConfigurationSource config)
      throws IOException {

    final File versionFile = new File(storageDir, "hdds/VERSION");

    Properties props = DatanodeVersionFile.readFrom(versionFile);
    if (props.isEmpty()) {
      throw new InconsistentStorageStateException(
          "Version file " + versionFile + " is missing");
    }

    return HddsVolumeUtil
        .getProperty(props, OzoneConsts.DATANODE_UUID, versionFile);
  }

  private String getFirstStorageDir(ConfigurationSource config)
      throws IOException {
    final Collection<String> storageDirs =
        MutableVolumeSet.getDatanodeStorageDirs(config);

    return
        StorageLocation.parse(storageDirs.iterator().next())
            .getUri().getPath();
  }

}
