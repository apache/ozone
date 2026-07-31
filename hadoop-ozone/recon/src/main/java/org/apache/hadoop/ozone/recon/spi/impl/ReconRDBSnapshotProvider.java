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

package org.apache.hadoop.ozone.recon.spi.impl;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.RDBSnapshotProvider;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.InodeMetadataRocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.ratis_snapshot.OmRatisSnapshotProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort.Type;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's {@link RDBSnapshotProvider} implementation that downloads the OM DB
 * checkpoint using the same bootstrap mechanism an OM follower uses: a chunked
 * {@code POST /v2/dbCheckpoint} request that carries a {@code toExcludeList[]}
 * of the parts already received so an interrupted transfer can resume, with
 * hard-link dedup on the leader and a completion sentinel to end the transfer.
 *
 * <p>This mirrors {@link OmRatisSnapshotProvider}: it overrides
 * {@link #downloadSnapshot} to POST to the leader's {@code /v2/dbCheckpoint}
 * endpoint and {@link #getCheckpointFromUntarredDb} to assemble and promote the
 * downloaded DB into a stable snapshot dir Recon can open. Like the OM follower,
 * it honors {@code ozone.om.db.checkpoint.use.inode.based.transfer}: when that is
 * {@code false} it falls back to the v1 {@code /dbCheckpoint} endpoint.
 */
public class ReconRDBSnapshotProvider extends RDBSnapshotProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconRDBSnapshotProvider.class);

  private final URLConnectionFactory connectionFactory;
  private final boolean spnegoEnabled;
  private final boolean httpsEnabled;
  private final boolean flushBeforeCheckpoint;
  private final boolean useV2CheckpointApi;
  private final Supplier<ServiceInfo> leaderInfoSupplier;
  // Leader pinned for the duration of a single (possibly multi-part) transfer.
  private final AtomicReference<ServiceInfo> pinnedLeader =
      new AtomicReference<>();

  public ReconRDBSnapshotProvider(File snapshotDir,
      URLConnectionFactory connectionFactory, boolean spnegoEnabled,
      HttpConfig.Policy httpPolicy, boolean flushBeforeCheckpoint,
      boolean useV2CheckpointApi,
      Supplier<ServiceInfo> leaderInfoSupplier) {
    super(snapshotDir, RECON_OM_SNAPSHOT_DB);
    this.connectionFactory = connectionFactory;
    this.spnegoEnabled = spnegoEnabled;
    this.httpsEnabled = httpPolicy.isHttpsEnabled();
    this.flushBeforeCheckpoint = flushBeforeCheckpoint;
    this.useV2CheckpointApi = useV2CheckpointApi;
    this.leaderInfoSupplier = leaderInfoSupplier;
  }

  /**
   * Pin the OM leader for the whole transfer before delegating to the shared
   * driver loop. The base loop resolves the leader only once (via
   * {#checkLeaderConsistency}); re-resolving per part would let a
   * mid-transfer OM failover merge parts from two leaders into the same
   * candidate dir. Recon's transfer is single-part today (it excludes snapshot
   * data), but pinning keeps this correct if that ever changes, matching how
   * {@link OmRatisSnapshotProvider} pins the leader for every part.
   */
  @Override
  public DBCheckpoint downloadDBSnapshotFromLeader(String leaderNodeID)
      throws IOException {
    pinnedLeader.set(leaderInfoSupplier.get());
    try {
      return super.downloadDBSnapshotFromLeader(leaderNodeID);
    } finally {
      pinnedLeader.set(null);
    }
  }

  @Override
  public void downloadSnapshot(String leaderNodeID, File targetFile)
      throws IOException {
    // Use the leader pinned for this transfer. The fallback only applies to a
    // direct downloadSnapshot call outside downloadDBSnapshotFromLeader.
    ServiceInfo leader = pinnedLeader.get();
    if (leader == null) {
      leader = leaderInfoSupplier.get();
    }
    URL checkpointUrl = buildCheckpointUrl(leader);
    LOG.info("Downloading OM DB checkpoint from leader {}. Checkpoint: {}, "
        + "URL: {}", leaderNodeID, targetFile.getName(), checkpointUrl);
    SecurityUtil.doAsLoginUser(() -> {
      HttpURLConnection connection = (HttpURLConnection)
          connectionFactory.openConnection(checkpointUrl, spnegoEnabled);
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type",
          "multipart/form-data; boundary=" + MULTIPART_FORM_DATA_BOUNDARY);
      connection.setDoOutput(true);

      List<String> existingFiles = useV2CheckpointApi
          ? HAUtils.getExistingFiles(getCandidateDir())
          : HAUtils.getExistingSstFilesRelativeToDbDir(getCandidateDir());
      OmRatisSnapshotProvider.writeFormData(connection, existingFiles);

      connection.connect();
      int errorCode = connection.getResponseCode();
      if (errorCode != HTTP_OK && errorCode != HTTP_CREATED) {
        throw new IOException("Unexpected response code " + errorCode
            + " when downloading OM DB checkpoint from " + checkpointUrl);
      }
      try (InputStream inputStream = connection.getInputStream()) {
        OmRatisSnapshotProvider.downloadFileWithProgress(inputStream,
            targetFile);
      } catch (IOException ex) {
        if (!FileUtils.deleteQuietly(targetFile)) {
          LOG.error("Failed to delete partial checkpoint file {}", targetFile);
        }
        throw ex;
      } finally {
        connection.disconnect();
      }
      return null;
    });
  }

  /**
   * After the transfer completes, install the leader's hard-link inventory,
   * normalize the layout to {@code <candidate>/om.db}, then move that DB out of
   * the reused candidate dir into a stable timestamped snapshot dir that Recon
   * opens as its new live DB. The candidate dir is emptied so the next sync
   * starts from a clean candidate dir.
   */
  @Override
  public DBCheckpoint getCheckpointFromUntarredDb(Path untarredDbDir)
      throws IOException {
    // The base class only calls this once it has seen the leader's
    // end-of-tarball marker. That marker is named "ratis snapshot complete"
    // for historical reasons, but it is not Ratis-specific: OM's shared
    // DBCheckpointServlet appends it to the end of every /v2/dbCheckpoint
    // response (the same one an OM follower bootstraps from), so here it just
    // means "the leader finished sending the checkpoint".

    // Installs hard links from hardLinkFile (tolerates a missing/empty file)
    // and moves root-level DB files into <untarredDbDir>/om.db. deleteSourceFiles
    // follows the endpoint: true for v2 (inode-based), false for the v1 layout.
    new InodeMetadataRocksDBCheckpoint(untarredDbDir, useV2CheckpointApi);

    Path omDbDir = untarredDbDir.resolve(OM_DB_NAME);
    if (!Files.isDirectory(omDbDir)) {
      throw new IOException("Expected RocksDB directory not found after "
          + "assembling checkpoint: " + omDbDir);
    }

    String stableName = RECON_OM_SNAPSHOT_DB + "_" + System.currentTimeMillis();
    Path stablePath = getSnapshotDir().toPath().resolve(stableName);
    Files.move(omDbDir, stablePath);
    LOG.info("Assembled OM DB moved from {} to {}", omDbDir, stablePath);

    // Clear residual entries (completion flag, orphan seeded files, empty dirs)
    // so the candidate dir is empty for the next sync cycle.
    cleanupCandidateDir(untarredDbDir.toFile());

    return new RocksDBCheckpoint(stablePath);
  }

  URL buildCheckpointUrl(ServiceInfo leader) throws IOException {
    Type portType = httpsEnabled ? Type.HTTPS : Type.HTTP;
    String scheme = httpsEnabled ? "https" : "http";
    String path = useV2CheckpointApi ? OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2
        : OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
    // Recon does not need OM's nested snapshot data.
    String query = OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA + "=false&"
        + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "="
        + (flushBeforeCheckpoint ? "true" : "false");
    try {
      return new URI(scheme, null, leader.getHostname(),
          leader.getPort(portType), path, query, null).toURL();
    } catch (URISyntaxException | MalformedURLException e) {
      throw new IOException("Could not build OM DB checkpoint URL", e);
    }
  }

  ServiceInfo getPinnedLeader() {
    return pinnedLeader.get();
  }

  private void cleanupCandidateDir(File candidate) {
    File[] entries = candidate.listFiles();
    if (entries == null) {
      return;
    }
    for (File entry : entries) {
      if (!FileUtils.deleteQuietly(entry)) {
        LOG.warn("Failed to clean up candidate dir entry {}", entry);
      }
    }
  }

  @Override
  public void close() {
    // The URLConnectionFactory is owned and destroyed by
    // OzoneManagerServiceProviderImpl; nothing to release here.
  }
}
