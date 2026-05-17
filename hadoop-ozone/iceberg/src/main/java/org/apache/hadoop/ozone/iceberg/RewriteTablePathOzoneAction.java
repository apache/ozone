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

package org.apache.hadoop.ozone.iceberg;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.RewriteTablePathUtil.RewriteResult;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.actions.ImmutableRewriteTablePath;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.Pair;

/**
 * An implementation of {@link RewriteTablePath} for Apache Ozone backed Iceberg tables.
 *
 * <p>This action rewrites table's metadata and position delete file paths by replacing a source
 * prefix with a target prefix. It processes table versions, snapshots, manifests and position delete files.</p>
 *
 * <p>The rewrite can be scoped between optional start and end metadata versions,
 * and all rewritten files are staged in a temporary directory.</p>
 */
public class RewriteTablePathOzoneAction implements RewriteTablePath {

  private String sourcePrefix;
  private String targetPrefix;
  private String startVersionName;
  private String endVersionName;
  private String stagingDir;
  private int parallelism;

  private ExecutorService executorService;
  private static final int MAX_INFLIGHT_MULTIPLIER = 4;
  private static final int DEFAULT_THREAD_COUNT = 10;

  private final Table table;

  public RewriteTablePathOzoneAction(Table table) {
    this.table = table;
    this.parallelism = DEFAULT_THREAD_COUNT;
  }

  public RewriteTablePathOzoneAction(Table table, int parallelism) {
    this.table = table;
    this.parallelism = parallelism;
  }

  @Override
  public RewriteTablePath rewriteLocationPrefix(String sPrefix, String tPrefix) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(sPrefix, "Source prefix");
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(tPrefix, "Target prefix");
    this.sourcePrefix = sPrefix;
    this.targetPrefix = tPrefix;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String sVersion) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(sVersion, "Start version");
    this.startVersionName = sVersion;
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String eVersion) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(eVersion, "End version");
    this.endVersionName = eVersion;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String stagingLocation) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(stagingLocation, "Staging location");
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    executorService = Executors.newFixedThreadPool(parallelism);
    try {
      return doExecute();
    } finally {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private Result doExecute() {
    String resultLocation = rebuildMetadata();
    return ImmutableRewriteTablePath.Result.builder()
        .stagingLocation(stagingDir)
        .fileListLocation(resultLocation)
        .latestVersion(RewriteTablePathUtil.fileName(endVersionName))
        .build();
  }

  private void validateInputs() {
    if (sourcePrefix.equals(targetPrefix)) {
      throw new IllegalArgumentException(
          String.format(
              "Source prefix cannot be the same as target prefix (%s)", sourcePrefix));
    }

    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    validateAndSetEndVersion(tableMetadata);
    validateAndSetStartVersion(tableMetadata);

    if (stagingDir == null) {
      stagingDir =
              RewriteTablePathOzoneUtils.getMetadataLocation(table)
              + "copy-table-staging-"
              + UUID.randomUUID()
              + RewriteTablePathUtil.FILE_SEPARATOR;
    } else {
      stagingDir = RewriteTablePathUtil.maybeAppendFileSeparator(stagingDir);
    }
  }

  private void validateAndSetEndVersion(TableMetadata tableMetadata) {
    if (endVersionName == null) {
      Objects.requireNonNull(
          tableMetadata.metadataFileLocation(), "Metadata file location should not be null");
      this.endVersionName = tableMetadata.metadataFileLocation();
    } else {
      this.endVersionName = validateVersion(tableMetadata, endVersionName);
    }
  }

  private void validateAndSetStartVersion(TableMetadata tableMetadata) {
    if (startVersionName != null) {
      this.startVersionName = validateVersion(tableMetadata, startVersionName);
    }
  }

  private String validateVersion(TableMetadata tableMetadata, String versionFileName) {
    String versionFile = null;
    if (versionInFilePath(tableMetadata.metadataFileLocation(), versionFileName)) {
      versionFile = tableMetadata.metadataFileLocation();
    } else {
      for (MetadataLogEntry log : tableMetadata.previousFiles()) {
        if (versionInFilePath(log.file(), versionFileName)) {
          versionFile = log.file();
          break;
        }
      }
    }

    if (versionFile == null) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot find provided version file %s in metadata log.", versionFileName));
    }
    if (!RewriteTablePathOzoneUtils.fileExist(versionFile, table.io())) {
      throw new IllegalArgumentException(
          String.format("Version file %s does not exist.", versionFile));
    }
    return versionFile;
  }

  private boolean versionInFilePath(String path, String version) {
    return RewriteTablePathUtil.fileName(path).equals(version);
  }

  private String rebuildMetadata() {
    //TODO need to implement rewrite of manifest files and position delete files.
    TableMetadata startMetadata = startVersionName != null
        ? new StaticTableOperations(startVersionName, table.io()).current()
        : null;
    TableMetadata endMetadata = new StaticTableOperations(endVersionName, table.io()).current();

    List<PartitionStatisticsFile> partitionStats = endMetadata.partitionStatisticsFiles();
    if (partitionStats != null && !partitionStats.isEmpty()) {
      throw new IllegalArgumentException("Partition statistics files are not supported yet.");
    }

    RewriteResult<Snapshot> rewriteVersionResult = rewriteVersionFiles(endMetadata);
    Set<Snapshot> deltaSnapshots = deltaSnapshots(startMetadata, rewriteVersionResult.toRewrite());
    Set<Long> deltaSnapshotIds =  deltaSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Set<Snapshot> validSnapshots = new HashSet<>(RewriteTablePathOzoneUtils.snapshotSet(endMetadata));
    validSnapshots.removeAll(RewriteTablePathOzoneUtils.snapshotSet(startMetadata));
    Set<String> manifestsToRewrite = manifestsToRewrite(validSnapshots, 
        startMetadata != null ? deltaSnapshotIds : null);
    RewriteResult<ManifestFile> rewriteManifestListResult = 
        rewriteManifestLists(validSnapshots, endMetadata, manifestsToRewrite);

    Set<Pair<String, String>> copyPlan = new HashSet<>();
    copyPlan.addAll(rewriteVersionResult.copyPlan());
    copyPlan.addAll(rewriteManifestListResult.copyPlan());

    return RewriteTablePathOzoneUtils.saveFileList(copyPlan, stagingDir, table.io());
  }

  private RewriteResult<Snapshot> rewriteVersionFiles(TableMetadata endMetadata) {
    RewriteResult<Snapshot> result = new RewriteResult<>();
    result.toRewrite().addAll(endMetadata.snapshots());
    result.copyPlan().addAll(rewriteVersionFile(endMetadata, endVersionName));

    List<MetadataLogEntry> versions = endMetadata.previousFiles();
    for (int i = versions.size() - 1; i >= 0; i--) {
      String versionFilePath = versions.get(i).file();
      if (versionFilePath.equals(startVersionName)) {
        break;
      }

      if (!RewriteTablePathOzoneUtils.fileExist(versionFilePath, table.io())) {
        throw new IllegalArgumentException(String.format("Version file %s doesn't exist", versionFilePath));
      }

      TableMetadata tableMetadata = new StaticTableOperations(versionFilePath, table.io()).current();

      result.toRewrite().addAll(tableMetadata.snapshots());
      result.copyPlan().addAll(rewriteVersionFile(tableMetadata, versionFilePath));
    }

    return result;
  }

  private Set<Pair<String, String>> rewriteVersionFile(TableMetadata metadata, String versionFilePath) {
    Set<Pair<String, String>> result = new HashSet<>();
    String stagingPath = RewriteTablePathUtil.stagingPath(versionFilePath, sourcePrefix, stagingDir);
    
    System.out.println("Processing version file " + versionFilePath);
    TableMetadata newTableMetadata = RewriteTablePathUtil.replacePaths(metadata, sourcePrefix, targetPrefix);
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    
    result.add(Pair.of(stagingPath, RewriteTablePathUtil.newPath(versionFilePath, sourcePrefix, targetPrefix)));
    result.addAll(RewriteTablePathOzoneUtils.statsFileCopyPlan(
            metadata.statisticsFiles(), newTableMetadata.statisticsFiles()));

    return result;
  }

  private Set<String> manifestsToRewrite(Set<Snapshot> validSnapshots, Set<Long> deltaSnapshotIds) {

    Set<String> manifestPaths = ConcurrentHashMap.newKeySet();
    int maxInFlight = parallelism * MAX_INFLIGHT_MULTIPLIER;
    Semaphore semaphore = new Semaphore(maxInFlight);

    ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

    int submittedTasks = 0;
    int completedTasks = 0;

    try {
      for (Snapshot snapshot : validSnapshots) {
        semaphore.acquire(); // blocks when maxInFlight tasks are already in-flight

        final long snapshotId = snapshot.snapshotId();
        final String manifestListLocation = snapshot.manifestListLocation();

        boolean taskSubmitted = false;
        try {
          completionService.submit(() -> {
            try (CloseableIterable<ManifestFile> manifests =
                     InternalData.read(
                             FileFormat.AVRO,
                             table.io().newInputFile(manifestListLocation))
                         .setRootType(GenericManifestFile.class)
                         .setCustomType(
                             ManifestFile.PARTITION_SUMMARIES_ELEMENT_ID,
                             GenericPartitionFieldSummary.class)
                         .project(ManifestFile.schema())
                         .build()) {

              for (ManifestFile manifest : manifests) {
                if (deltaSnapshotIds == null) {
                  manifestPaths.add(manifest.path());
                } else if (manifest.snapshotId() != null
                    && deltaSnapshotIds.contains(manifest.snapshotId())) {
                  manifestPaths.add(manifest.path());
                }
              }

            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to read manifests for snapshot " + snapshotId, e);
            } finally {
              semaphore.release();
            }
            return null;
          });
          taskSubmitted = true;
          submittedTasks++;
        } finally {
          if (!taskSubmitted) {
            semaphore.release();
          }
        }
        Future<Void> done;
        while ((done = completionService.poll()) != null) {
          done.get();
          completedTasks++;
        }
      }
      
      while (completedTasks < submittedTasks) {
        completionService.take().get();
        completedTasks++;
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executorService.shutdownNow();
      throw new RuntimeException("Interrupted while processing manifests", e);

    } catch (ExecutionException e) {
      executorService.shutdownNow();
      throw new RuntimeException(
          "Failed to collect manifests to rewrite. "
              + "The end version may contain invalid snapshots. "
              + "Please choose an earlier version.",
          e.getCause());
    }

    return manifestPaths;
  }

  private RewriteResult<ManifestFile> rewriteManifestList(
      Snapshot snapshot, TableMetadata tableMetadata, Set<String> manifestsToRewrite) {
    RewriteResult<ManifestFile> result = new RewriteResult<>();

    String path = snapshot.manifestListLocation();
    String outputPath = RewriteTablePathUtil.stagingPath(path, sourcePrefix, stagingDir);
    RewriteResult<ManifestFile> rewriteResult =
        RewriteTablePathUtil.rewriteManifestList(
            snapshot,
            table.io(),
            tableMetadata,
            manifestsToRewrite,
            sourcePrefix,
            targetPrefix,
            stagingDir,
            outputPath);

    result.append(rewriteResult);
    result
        .copyPlan()
        .add(Pair.of(outputPath, RewriteTablePathUtil.newPath(path, sourcePrefix, targetPrefix)));
    return result;
  }

  private RewriteResult<ManifestFile> rewriteManifestLists(Set<Snapshot> validSnapshots, TableMetadata endMetadata,
      Set<String> manifestsToRewrite) {

    if (validSnapshots.isEmpty()) {
      return new RewriteResult<>();
    }

    int maxInFlight = parallelism * MAX_INFLIGHT_MULTIPLIER;
    Semaphore semaphore = new Semaphore(maxInFlight);
    ExecutorCompletionService<RewriteResult<ManifestFile>> completionService =
        new ExecutorCompletionService<>(executorService);

    RewriteResult<ManifestFile> combined = new RewriteResult<>();
    int submittedTasks = 0;
    int completedTasks = 0;

    try {
      for (Snapshot snapshot : validSnapshots) {
        semaphore.acquire();

        boolean taskSubmitted = false;
        try {
          completionService.submit(() -> {
            try {
              return rewriteManifestList(snapshot, endMetadata, manifestsToRewrite);
            } finally {
              semaphore.release();
            }
          });
          taskSubmitted = true;
          submittedTasks++;
        } finally {
          if (!taskSubmitted) {
            semaphore.release();
          }
        }

        Future<RewriteResult<ManifestFile>> done;
        while ((done = completionService.poll()) != null) {
          combined.append(done.get());
          completedTasks++;
        }
      }
      
      while (completedTasks < submittedTasks) {
        combined.append(completionService.take().get());
        completedTasks++;
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executorService.shutdownNow();
      throw new RuntimeException("Interrupted while rewriting manifest lists", e);
    } catch (ExecutionException e) {
      executorService.shutdownNow();
      throw new RuntimeException("Failed to rewrite manifest list", e.getCause());
    }

    return combined;
  }

  private Set<Snapshot> deltaSnapshots(TableMetadata startMetadata, Set<Snapshot> allSnapshots) {
    if (startMetadata == null) {
      return allSnapshots;
    } else {
      Set<Long> startSnapshotIds =
          startMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      return allSnapshots.stream()
          .filter(s -> !startSnapshotIds.contains(s.snapshotId()))
          .collect(Collectors.toSet());
    }
  }
}
