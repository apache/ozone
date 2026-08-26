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
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
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
 * <p>This scanner surfaces duplicate copies across volumes. Singleton container IDs
 * are stored as a single path in {@link ContainerScanResult#getSingles()}; duplicate
 * IDs are stored as path lists in {@link ContainerScanResult#getDuplicates()}.
 * Size and metadata status are computed later via {@link #enrichDuplicates(Map)}.
 */
public final class ContainerDirectoryScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerDirectoryScanner.class);

  private ContainerDirectoryScanner() {
    //Never constructed
  }

  public static ContainerScanResult scan(ConfigurationSource conf) throws IOException {
    Map<Long, String> singles = new ConcurrentHashMap<>();
    Map<Long, List<String>> duplicates = new ConcurrentHashMap<>();
    List<String> volumeScanErrors = Collections.synchronizedList(new ArrayList<>());
    List<String> volumeRootsToScan = resolveExistingVolumeRoots(conf);
    if (volumeRootsToScan.isEmpty()) {
      return new ContainerScanResult(singles, duplicates, volumeScanErrors);
    }

    int volumeCount = volumeRootsToScan.size();
    ExecutorService executor = Executors.newFixedThreadPool(volumeCount,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("ContainerDirectoryScanner-%d")
            .build());
    
    try {
      List<Future<?>> futures = new ArrayList<>(volumeCount);
      for (String volumeRoot : volumeRootsToScan) {
        futures.add(executor.submit(() -> {
          try {
            scanVolume(volumeRoot, singles, duplicates);
          } catch (IOException e) {
            LOG.warn("Failed to scan volume {}", volumeRoot, e);
            volumeScanErrors.add(volumeRoot + ": " + e.getMessage());
          }
        }));
      }
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          throw new IOException("Unexpected error scanning volume", e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Volume scan interrupted", e);
        }
      }
    } finally {
      executor.shutdownNow();
    }
    return new ContainerScanResult(singles, duplicates, volumeScanErrors);
  }

  private static List<String> resolveExistingVolumeRoots(ConfigurationSource conf) throws IOException {
    List<String> volumeRootsToScan = new ArrayList<>();
    for (String storageDir : HddsServerUtil.getDatanodeStorageDirs(conf)) {
      String volumeRoot = StorageLocation.parse(storageDir).getUri().getPath();
      if (!new File(volumeRoot).exists()) {
        LOG.warn("Configured storage path {} does not exist, skipping", volumeRoot);
        continue;
      }
      volumeRootsToScan.add(volumeRoot);
    }
    return volumeRootsToScan;
  }

  /**
   * Scan a single DataNode storage volume root and merge results into {@code singles}
   * and {@code duplicates}.
   */
  private static void scanVolume(String volumeRoot, Map<Long, String> singles,
      Map<Long, List<String>> duplicates) throws IOException {
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
        recordContainerDir(containerDir, singles, duplicates);
      }
    }
  }

  private static File resolveCurrentDir(File hddsRoot, String clusterId) throws IOException {
    File[] storageDirs = hddsRoot.listFiles(File::isDirectory);
    if (storageDirs == null) {
      throw new IOException("IO error listing " + hddsRoot);
    }
    if (storageDirs.length == 0) {
      return null;
    }
    return StorageVolumeUtil.resolveContainerCurrentDir(hddsRoot, clusterId, storageDirs);
  }

  private static void recordContainerDir(File containerDir, Map<Long, String> singles,
      Map<Long, List<String>> duplicates) {
    long containerId;
    try {
      containerId = ContainerUtils.getContainerID(containerDir);
    } catch (NumberFormatException e) {
      LOG.warn("Skipping non-numeric container directory {}", containerDir);
      return;
    }

    String containerPath = containerDir.getAbsolutePath();
    singles.compute(containerId, (id, firstPath) -> {
      List<String> dupList = duplicates.get(id);
      if (dupList != null) {
        dupList.add(containerPath);
        return null;
      }
      if (firstPath == null) {
        return containerPath;
      }
      List<String> list = new ArrayList<>(2);
      list.add(firstPath);
      list.add(containerPath);
      duplicates.put(id, list);
      return null;
    });
  }

  public static Map<Long, List<ContainerDiskOccurrence>> enrichDuplicates(Map<Long, List<String>> duplicates) {
    Map<Long, List<ContainerDiskOccurrence>> enriched = new HashMap<>(duplicates.size());
    for (Map.Entry<Long, List<String>> entry : duplicates.entrySet()) {
      long containerId = entry.getKey();
      List<String> containerPaths = new ArrayList<>(entry.getValue());
      Collections.sort(containerPaths);
      List<ContainerDiskOccurrence> occurrences = new ArrayList<>(containerPaths.size());
      for (String containerPath : containerPaths) {
        occurrences.add(enrichOccurrence(containerId, containerPath));
      }
      enriched.put(containerId, Collections.unmodifiableList(occurrences));
    }
    return Collections.unmodifiableMap(enriched);
  }

  /**
   * Compute directory size and metadata status for on-disk container path.
   */
  static ContainerDiskOccurrence enrichOccurrence(long containerId, String containerPath) {
    File containerDir = new File(containerPath);
    File containerFile = ContainerUtils.getContainerFile(containerDir);
    ContainerDiskScanStatus status;
    if (!containerFile.exists()) {
      status = ContainerDiskScanStatus.MISSING_METADATA;
    } else {
      status = readMetadataStatus(containerId, containerFile);
    }

    boolean sizeKnown = true;
    long sizeBytes;
    try {
      sizeBytes = FileUtils.sizeOfDirectory(containerDir);
    } catch (IllegalArgumentException e) {
      LOG.warn("Failed to compute size for container directory {}", containerDir, e);
      sizeBytes = 0L;
      sizeKnown = false;
    }

    return new ContainerDiskOccurrence(containerId, containerPath, sizeBytes, sizeKnown, status);
  }

  private static ContainerDiskScanStatus readMetadataStatus(long containerId, File containerFile) {
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
