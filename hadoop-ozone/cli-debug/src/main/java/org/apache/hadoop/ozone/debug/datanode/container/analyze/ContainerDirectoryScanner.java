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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only walker for container directories under {@code hdds.datanode.dir}.
 *
 * <p>Unlike {@link org.apache.hadoop.ozone.container.ozoneimpl.ContainerReader},
 * this scanner records every container directory it finds, including those with
 * missing or invalid metadata and duplicate copies across volumes.
 */
public class ContainerDirectoryScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerDirectoryScanner.class);

  /**
   * Scan all configured DataNode storage volumes and return every container
   * directory found on disk.
   *
   * @return map of container ID to all on-disk occurrences for that ID
   */
  public Map<Long, List<ContainerDiskOccurrence>> scan(ConfigurationSource conf) throws IOException {
    Collection<String> storageDirs = HddsServerUtil.getDatanodeStorageDirs(conf);
    
    if (storageDirs.isEmpty()) {
      return Collections.emptyMap();
    }
    
    validateStorageDirectories(storageDirs);
    Map<Long, List<ContainerDiskOccurrence>> result = new ConcurrentHashMap<>();
    int volumeCount = storageDirs.size();
    ExecutorService executor = Executors.newFixedThreadPool(volumeCount,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("ContainerDirectoryScanner-%d")
            .build());
    
    try {
      List<Future<?>> futures = new ArrayList<>(volumeCount);
      for (String storageDir : storageDirs) {
        String volumeRoot = StorageLocation.parse(storageDir).getUri().getPath();
        futures.add(executor.submit(() -> {
          try {
            scanVolume(volumeRoot, result);
          } catch (IOException e) {
            throw new RuntimeException(
                "Failed to scan volume " + volumeRoot, e);
          }
        }));
      }
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          }
          throw new IOException(cause);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Volume scan interrupted", e);
        }
      }
    } finally {
      executor.shutdownNow();
    }

    return Collections.unmodifiableMap(toImmutableLists(result));
  }

  /**
   * Scan a single DataNode storage volume root and merge results into {@code result}.
   */
  private void scanVolume(String volumeRoot, Map<Long, List<ContainerDiskOccurrence>> result) throws IOException {
    File hddsRoot = new File(volumeRoot, HddsVolume.HDDS_VOLUME_DIR);
    if (!hddsRoot.isDirectory()) {
      LOG.warn("HDDS root {} does not exist or is not a directory, skipping volume {}", hddsRoot, volumeRoot);
      return;
    }

    File versionFile = StorageVolumeUtil.getVersionFile(hddsRoot);
    Properties props = DatanodeVersionFile.readFrom(versionFile);
    if (props.isEmpty()) {
      throw new IOException("Version file " + versionFile + " is missing or empty");
    }
    String clusterId;
    try {
      clusterId = StorageVolumeUtil.getClusterID(props, versionFile, null);
    } catch (InconsistentStorageStateException e) {
      throw new IOException("Invalid version file " + versionFile, e);
    }

    File currentDir = resolveCurrentDir(hddsRoot, clusterId);
    if (currentDir == null || !currentDir.isDirectory()) {
      LOG.info("No current container directory under {}, skipping volume {}", hddsRoot, volumeRoot);
      return;
    }

    LOG.info("Scanning container directories under {}", currentDir);
    File[] containerTopDirs = currentDir.listFiles(File::isDirectory);
    if (containerTopDirs == null) {
      throw new IOException("Failed to list container top-level directories under " + currentDir);
    }

    for (File containerTopDir : containerTopDirs) {
      File[] containerDirs = containerTopDir.listFiles(File::isDirectory);
      if (containerDirs == null) {
        LOG.warn("Failed to list container directories under {}", containerTopDir);
        continue;
      }
      for (File containerDir : containerDirs) {
        recordContainerDir(containerDir, volumeRoot, result);
      }
    }
  }

  /**
   * Resolve {@code .../current} using the same rules as
   * {@link org.apache.hadoop.ozone.container.ozoneimpl.ContainerReader#readVolume(File)}.
   */
  static File resolveCurrentDir(File hddsRoot, String clusterId) throws IOException {
    File[] storageDirs = hddsRoot.listFiles(File::isDirectory);
    if (storageDirs == null) {
      throw new IOException("IO error listing " + hddsRoot);
    }
    if (storageDirs.length == 0) {
      return null;
    }

    File clusterIdDir = new File(hddsRoot, clusterId);
    File idDir = clusterIdDir;
    if (storageDirs.length == 1 && !clusterIdDir.exists()) {
      idDir = storageDirs[0];
    } else if (!clusterIdDir.exists()) {
      throw new IOException("Volume " + hddsRoot + " is in an inconsistent state. "
          + "Expected cluster ID directory " + clusterIdDir + " not found.");
    }
    return new File(idDir, Storage.STORAGE_DIR_CURRENT);
  }

  private static void recordContainerDir(File containerDir, String volumeRoot,
      Map<Long, List<ContainerDiskOccurrence>> result) {
    long containerId;
    try {
      containerId = ContainerUtils.getContainerID(containerDir);
    } catch (NumberFormatException e) {
      LOG.warn("Skipping non-numeric container directory {}", containerDir);
      return;
    }

    File containerFile = ContainerUtils.getContainerFile(containerDir);
    ContainerDiskScanStatus status;
    if (!containerFile.exists()) {
      status = ContainerDiskScanStatus.MISSING_METADATA;
    } else {
      status = readMetadataStatus(containerId, containerFile);
    }

    long sizeBytes;
    try {
      sizeBytes = FileUtils.sizeOfDirectory(containerDir);
    } catch (IllegalArgumentException e) {
      LOG.warn("Failed to compute size for container directory {}, using 0", containerDir, e);
      sizeBytes = 0L;
    }

    ContainerDiskOccurrence occurrence = new ContainerDiskOccurrence(
        containerId,
        containerDir.getAbsolutePath(),
        volumeRoot,
        sizeBytes,
        status);
    result.compute(containerId, (id, existing) -> {
      if (existing == null) {
        List<ContainerDiskOccurrence> list = new ArrayList<>();
        list.add(occurrence);
        return list;
      }
      existing.add(occurrence);
      return existing;
    });
  }

  private static ContainerDiskScanStatus readMetadataStatus(long containerId,
      File containerFile) {
    try {
      ContainerData containerData = ContainerDataYaml.readContainerFile(containerFile);
      if (containerId != containerData.getContainerID()) {
        LOG.warn("Container ID mismatch in {}. Directory name is {} but metadata has {}.",
            containerFile, containerId, containerData.getContainerID());
        return ContainerDiskScanStatus.INVALID_METADATA;
      }
      return ContainerDiskScanStatus.VALID;
    } catch (IOException e) {
      LOG.warn("Failed to parse container metadata file {}", containerFile, e);
      return ContainerDiskScanStatus.INVALID_METADATA;
    }
  }

  private static void validateStorageDirectories(Collection<String> storageDirs) throws IOException {
    List<String> missingDirs = new ArrayList<>();
    for (String storageDir : storageDirs) {
      String storageDirPath = StorageLocation.parse(storageDir).getUri().getPath();
      if (!new File(storageDirPath).exists()) {
        missingDirs.add(storageDirPath);
      }
    }
    if (!missingDirs.isEmpty()) {
      throw new IOException(String.join(", ", missingDirs)
          + " configured in '" + ScmConfigKeys.HDDS_DATANODE_DIR_KEY
          + "' does not exist. Please provide the correct value for config.");
    }
  }

  private static Map<Long, List<ContainerDiskOccurrence>> toImmutableLists(
      Map<Long, List<ContainerDiskOccurrence>> mutable) {
    Map<Long, List<ContainerDiskOccurrence>> immutable = new HashMap<>();
    for (Map.Entry<Long, List<ContainerDiskOccurrence>> entry : mutable.entrySet()) {
      immutable.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
    }
    return immutable;
  }

  /**
   * On-disk status of a container directory discovered during a DN scan.
   */
  public enum ContainerDiskScanStatus {
    /** {@code metadata/{containerId}.container} exists and parses correctly. */
    VALID,
    /** Container directory exists but the {@code .container} file is missing. */
    MISSING_METADATA,
    /** {@code .container} exists but is unreadable or its ID does not match the directory name. */
    INVALID_METADATA
  }
}
