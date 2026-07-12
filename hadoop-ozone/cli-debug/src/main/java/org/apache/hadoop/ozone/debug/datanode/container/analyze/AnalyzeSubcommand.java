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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
    description = "Analyze container consistency between on-disk container " +
            "directories on this DataNode and SCM metadata. Must be run locally on a DataNode.")
public class AnalyzeSubcommand extends AbstractSubcommand implements Callable<Void> {
  @CommandLine.Option(names = {"--count"},
          defaultValue = "20",
          description = "Number of containers to display")
  private int count;

  @Override
  public Void call() throws Exception {
    if (count < 1) {
      throw new IOException("Count must be an integer greater than 0.");
    }
    OzoneConfiguration conf = getOzoneConf();
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);
    Map<Long, List<ContainerDiskOccurrence>> enrichedDuplicates =
        ContainerDirectoryScanner.enrichDuplicates(scanResult.getDuplicates());

    // TODO: SCM metadata lookup from --scm-db when provided.
    // TODO: For each id in scanResult.getSingles().keySet() classified NOT_IN_SCM or DELETED:
    //   enrichOccurrence(id, scanResult.getSingles().get(id)) and report.
    // TODO: For each id in enrichedDuplicates.keySet() classified NOT_IN_SCM or DELETED:
    //   enrichedDuplicates.get(id) is already enriched — just report.

    printDuplicates(enrichedDuplicates);
    printVolumeScanErrors(scanResult.getVolumeScanErrors());
    return null;
  }

  private void printDuplicates(Map<Long, List<ContainerDiskOccurrence>> duplicates) {
    long totalDuplicateIds = duplicates.size();
    out().printf("Number of containers with duplicate container directories on this DataNode: %d%n", totalDuplicateIds);

    if (totalDuplicateIds == 0) {
      return;
    }

    if (totalDuplicateIds > count) {
      out().printf("Showing first %d:%n", count);
    }

    duplicates.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .limit(count)
        .forEach(entry -> {
          long containerId = entry.getKey();
          List<ContainerDiskOccurrence> occurrences = entry.getValue();
          out().printf("Container %d (%d occurrences):%n", containerId, occurrences.size());
          for (ContainerDiskOccurrence o : occurrences) {
            out().printf("  path=%s%n", o.getContainerPath());
            if (o.isSizeKnown()) {
              out().printf("  status=%s size=%d bytes%n", o.getStatus(), o.getSizeBytes());
            } else {
              out().printf("  status=%s size=unavailable (failed to compute directory size)%n",
                  o.getStatus());
            }
            out().println();
          }
        });
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
