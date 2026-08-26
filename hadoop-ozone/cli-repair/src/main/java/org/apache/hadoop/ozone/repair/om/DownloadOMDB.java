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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_CHECKPOINT_USE_INODE_BASED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.ratis_snapshot.OmRatisSnapshotProvider;
import org.apache.hadoop.ozone.repair.RepairTool;
import picocli.CommandLine;

/**
 * Tool to download OM metadata using the follower bootstrap checkpoint flow.
 */
@CommandLine.Command(
    name = "download",
    description = "Downloads OM metadata (om.db and db.snapshots) from an OM node using the same "
        + "checkpoint transfer flow as follower bootstrap.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class DownloadOMDB extends RepairTool {

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--node-id"},
      description = "Node ID of the OM to download om.db from. Required when OM HA is configured.",
      required = false
  )
  private String nodeId;

  @CommandLine.Option(
      names = {"--output-dir"},
      description = "Path where the downloaded OM metadata directory will be written. "
          + "The output matches follower bootstrap layout: om.db and db.snapshots.",
      required = true
  )
  private Path outputDir;

  @CommandLine.Option(
      names = {"--overwrite"},
      description = "Overwrite output directory if it already exists."
  )
  private boolean overwrite;

  @Override
  public void execute() throws Exception {
    OzoneConfiguration conf = getOzoneConf();
    String effectiveServiceId = resolveServiceId(conf);

    boolean outputExists = Files.exists(outputDir);
    if (outputExists && !overwrite) {
      fatal("Output directory already exists: %s. Use --overwrite to replace it.",
          outputDir.toAbsolutePath());
    }

    if (outputExists && !Files.isDirectory(outputDir)) {
      fatal("Output path is not a directory: %s", outputDir.toAbsolutePath());
    }

    if (isDryRun()) {
      info("Would download OM metadata at %s (using follower bootstrap flow).",
          outputDir.toAbsolutePath());
      return;
    }

    if (outputExists) {
      FileUtils.forceDelete(outputDir.toFile());
    }
    Files.createDirectories(outputDir);

    // This tool intentionally follows the inode-based follower bootstrap transfer.
    conf.setBoolean(OZONE_OM_DB_CHECKPOINT_USE_INODE_BASED_KEY, true);

    Path snapshotWorkDir = Files.createTempDirectory(outputDir, ".omdb-bootstrap-");
    DBCheckpoint checkpoint = null;
    try {
      checkpoint = downloadCheckpoint(conf, effectiveServiceId, snapshotWorkDir);
      Path checkpointRoot = checkpoint.getCheckpointLocation();
      Path omDbPath = checkpointRoot.resolve(OM_DB_NAME);
      if (!Files.isDirectory(omDbPath)) {
        throw new IOException("Constructed OM DB directory not found in checkpoint: " + omDbPath);
      }
      Path completionMarker = checkpointRoot.resolve(OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME);
      if (Files.exists(completionMarker)) {
        Files.delete(completionMarker);
      }
      moveDirectoryContents(checkpointRoot, outputDir);
      checkpoint = null;
      info("Successfully downloaded OM metadata at: %s (includes %s and %s if present on the leader).",
          outputDir.toAbsolutePath(), OM_DB_NAME, OM_SNAPSHOT_DIR);
    } finally {
      if (checkpoint != null) {
        checkpoint.cleanupCheckpoint();
      }
      FileUtils.deleteQuietly(snapshotWorkDir.toFile());
    }
  }

  private DBCheckpoint downloadCheckpoint(OzoneConfiguration conf,
      String serviceId, Path snapshotWorkDir) throws Exception {
    List<String> nodeIds = getCandidateNodeIds(conf, serviceId);
    Exception lastFailure = null;
    for (String candidateNodeId : nodeIds) {
      OMNodeDetails omNodeDetails =
          OMNodeDetails.getOMNodeDetailsFromConf(conf, serviceId, candidateNodeId);
      if (omNodeDetails == null) {
        if (nodeId != null) {
          fatal("Couldn't determine OM node from the given service-id: %s and node-id: %s.",
              serviceId, nodeId);
        }
        continue;
      }
      String providerNodeId = omNodeDetails.getNodeId();
      if (providerNodeId == null) {
        providerNodeId = "non-ha";
      }
      try (OmRatisSnapshotProvider provider = new OmRatisSnapshotProvider(
          conf, snapshotWorkDir.toFile(),
          Collections.singletonMap(providerNodeId, omNodeDetails))) {
        return provider.downloadDBSnapshotFromLeader(providerNodeId);
      } catch (Exception ex) {
        lastFailure = ex;
        if (nodeId != null) {
          throw ex;
        }
      }
    }
    if (lastFailure != null) {
      throw lastFailure;
    }
    fatal("Couldn't determine OM node from the given service-id: %s and node-id: %s.",
        serviceId, nodeId);
    return null;
  }

  private List<String> getCandidateNodeIds(OzoneConfiguration conf,
      String serviceId) {
    if (nodeId != null) {
      return Collections.singletonList(nodeId);
    }
    if (!OmUtils.isServiceIdsDefined(conf)) {
      return Collections.singletonList(null);
    }
    String omNodesKey = ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, serviceId);
    Collection<String> omNodeIds = conf.getTrimmedStringCollection(omNodesKey);
    return new ArrayList<>(omNodeIds);
  }

  private static void moveDirectoryContents(Path sourceDir, Path targetDir)
      throws IOException {
    try (java.util.stream.Stream<Path> entries = Files.list(sourceDir)) {
      for (Path entry : (Iterable<Path>) entries::iterator) {
        Files.move(entry, targetDir.resolve(entry.getFileName()),
            StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private String resolveServiceId(OzoneConfiguration conf) throws IOException {
    if (omServiceId != null && !omServiceId.isEmpty()) {
      return omServiceId;
    }
    return OmUtils.getOzoneManagerServiceId(conf);
  }
}
