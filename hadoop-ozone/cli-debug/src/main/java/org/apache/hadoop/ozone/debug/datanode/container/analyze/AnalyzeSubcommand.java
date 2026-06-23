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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.shell.ListLimitOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * {@code ozone debug datanode container analyze}.
 *
 * <p>Compares on-disk container directories on this DataNode against SCM
 * metadata to report inconsistencies.
 */
@Command(
    name = "analyze",
    description = {
        "Analyze container consistency between on-disk container directories on this DataNode and SCM metadata.",
        "Must be run locally on a DataNode.",
        "",
        "Reports:",
        "  Duplicate container directories: same containerID found on more than one volume.",
        "  Orphan containers (requires --scm-db): present on disk but not present in SCM metadata.",
        "  Containers marked DELETED in SCM but present on disk (requires --scm-db).",
        "",
        "Each reported occurrence includes container directory path(s), size and an on-disk metadata status:",
        "  MISSING_METADATA: metadata/{containerId}.container does not exist.",
        "  INVALID_METADATA: metadata file exists but cannot be parsed, or the containerID in the",
        "      file does not match the directory name.",
        "  VALID: metadata file is present, parses correctly, and its containerID matches the directory name."
    })
public class AnalyzeSubcommand extends AbstractSubcommand implements Callable<Void> {
  @CommandLine.Mixin
  private ListLimitOptions listOptions;

  @CommandLine.Option(names = {"--scm-db"},
      description = "Path to an offline scm.db directory, or its parent metadata directory.")
  private File scmDb;

  @Override
  public Void call() throws Exception {
    validateOptions();
    OzoneConfiguration conf = getOzoneConf();
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);
    Map<Long, List<ContainerDiskOccurrence>> enrichedDuplicates =
        ContainerDirectoryScanner.enrichDuplicates(scanResult.getDuplicates());

    if (scmDb != null) {
      findOrphanAndDeletedButPresentContainers(conf, scanResult, enrichedDuplicates);
    } else {
      out().println("To identify orphan containers (wrt SCM) and containers that are marked as DELETED in SCM but"
          + " exist in the datanode's current directory, provide the SCM database path using the --scm-db option."
      );
    }

    printDuplicates(enrichedDuplicates);
    printVolumeScanErrors(scanResult.getVolumeScanErrors());
    return null;
  }

  /**
   * Validate CLI options before starting the on-disk DN scan.
   * {@link ListLimitOptions#getLimit()} is also called from
   * {@link #printContainerOccurrenceReport(String, Map)}, but validating here fails fast
   * before the DN volume scan and SCM DB lookup.
   */
  private void validateOptions() {
    listOptions.getLimit();
  }

  private void findOrphanAndDeletedButPresentContainers(OzoneConfiguration conf, ContainerScanResult scanResult,
      Map<Long, List<ContainerDiskOccurrence>> enrichedDuplicates) throws IOException {
    Map<Long, List<ContainerDiskOccurrence>> enrichedOrphanContainers = new HashMap<>();
    Map<Long, List<ContainerDiskOccurrence>> enrichedDeletedButPresent = new HashMap<>();

    try (ScmContainerMetadataReader reader = new ScmContainerMetadataReader(conf, scmDb)) {
      Set<Long> containerIds = new HashSet<>(scanResult.getSingles().keySet());
      containerIds.addAll(enrichedDuplicates.keySet());

      for (long containerId : containerIds) {
        Optional<ScmContainerMetadataReader.ScmContainerClassification> classification = reader.classify(containerId);
        if (!classification.isPresent()) {
          continue;
        }
        List<ContainerDiskOccurrence> occurrences = enrichedDuplicates.get(containerId);
        if (occurrences == null) {
          String path = scanResult.getSingles().get(containerId);
          occurrences = Collections.singletonList(ContainerDirectoryScanner.enrichOccurrence(containerId, path));
        }
        if (classification.get() == ScmContainerMetadataReader.ScmContainerClassification.NOT_IN_SCM) {
          enrichedOrphanContainers.put(containerId, occurrences);
        } else {
          enrichedDeletedButPresent.put(containerId, occurrences);
        }
      }
    }

    printContainerOccurrenceReport("Number of orphan containers(wrt SCM) on this DataNode: %d%n",
        enrichedOrphanContainers);
    printContainerOccurrenceReport(
        "Number of containers marked DELETED in SCM but present on disk on this DataNode: %d%n",
        enrichedDeletedButPresent);
  }

  private void printContainerOccurrenceReport(String countFormat, 
      Map<Long, List<ContainerDiskOccurrence>> containersById) {
    long total = containersById.size();
    out().printf(countFormat, total);
    if (total == 0) {
      return;
    }

    Stream<Map.Entry<Long, List<ContainerDiskOccurrence>>> stream =
        containersById.entrySet().stream().sorted(Map.Entry.comparingByKey());
    if (!listOptions.isAll()) {
      int limit = listOptions.getLimit();
      if (total > limit) {
        out().printf("Showing first %d:%n", limit);
      }
      stream = stream.limit(limit);
    }
    stream.forEach(entry -> printContainerEntry(entry.getKey(), entry.getValue()));
  }

  private void printContainerEntry(long containerId, List<ContainerDiskOccurrence> occurrences) {
    out().printf("Container %d (%d occurrence%s):%n",
        containerId,
        occurrences.size(),
        occurrences.size() == 1 ? "" : "s");
    for (ContainerDiskOccurrence occurrence : occurrences) {
      out().printf("  path=%s%n", occurrence.getContainerPath());
      if (occurrence.isSizeKnown()) {
        out().printf("  status=%s size=%d bytes%n", occurrence.getStatus(), occurrence.getSizeBytes());
      } else {
        out().printf("  status=%s size=unavailable (failed to compute directory size)%n", occurrence.getStatus());
      }
      out().println();
    }
  }

  private void printDuplicates(Map<Long, List<ContainerDiskOccurrence>> duplicates) {
    printContainerOccurrenceReport(
        "Number of containers with duplicate container directories on this DataNode: %d%n",
        duplicates);
  }

  private void printVolumeScanErrors(List<String> volumeScanErrors) {
    if (volumeScanErrors.isEmpty()) {
      return;
    }
    err().printf("%nVolumes that failed to scan (%d):%n", volumeScanErrors.size());
    for (String error : volumeScanErrors) {
      err().printf("  %s%n", error);
    }
  }
}
