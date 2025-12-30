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

package org.apache.hadoop.ozone.om.ratis_snapshot;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdds.utils.RDBSnapshotProvider;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.InodeMetadataRocksDBCheckpoint;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmRatisSnapshotProvider downloads the latest checkpoint from the
 * leader OM and loads the checkpoint into State Machine.  In addtion
 * to the latest checkpoint, it also downloads any previous
 * omSnapshots the leader has created.
 *
 * The term "snapshot" has two related but slightly different meanings
 * in ozone.  An "omSnapshot" is a copy of the om's metadata at a
 * point in time.  It is created by users through the "ozone sh
 * snapshot create" cli.
 *
 * A "ratisSnapshot", (provided by this class), is used by om
 * followers to bootstrap themselves to the current state of the om
 * leader.  ratisSnapshots will contain copies of all the individual
 * "omSnapshot"s that exist on the leader at the time of the
 * bootstrap.  The follower needs these copies to respond the users
 * snapshot requests when it becomes the leader.
 */
public class OmRatisSnapshotProvider extends RDBSnapshotProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmRatisSnapshotProvider.class);

  private final Map<String, OMNodeDetails> peerNodesMap;
  private final HttpConfig.Policy httpPolicy;
  private final boolean spnegoEnabled;
  private final URLConnectionFactory connectionFactory;

  public OmRatisSnapshotProvider(File snapshotDir,
      Map<String, OMNodeDetails> peerNodesMap, HttpConfig.Policy httpPolicy,
      boolean spnegoEnabled, URLConnectionFactory connectionFactory) {
    super(snapshotDir, OM_DB_NAME);
    this.peerNodesMap = new ConcurrentHashMap<>(peerNodesMap);
    this.httpPolicy = httpPolicy;
    this.spnegoEnabled = spnegoEnabled;
    this.connectionFactory = connectionFactory;
  }

  public OmRatisSnapshotProvider(MutableConfigurationSource conf,
      File omRatisSnapshotDir, Map<String, OMNodeDetails> peerNodeDetails) {
    super(omRatisSnapshotDir, OM_DB_NAME);
    LOG.info("Initializing OM Snapshot Provider");
    this.peerNodesMap = new ConcurrentHashMap<>();
    peerNodesMap.putAll(peerNodeDetails);

    this.httpPolicy = HttpConfig.getHttpPolicy(conf);
    this.spnegoEnabled = conf.get(OZONE_OM_HTTP_AUTH_TYPE, "simple")
        .equals("kerberos");

    TimeUnit connectionTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT.getUnit();
    int connectionTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT.getDuration(),
        connectionTimeoutUnit);

    TimeUnit requestTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT.getUnit();
    int requestTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT.getDuration(),
        requestTimeoutUnit);

    connectionFactory = URLConnectionFactory
      .newDefaultURLConnectionFactory(connectionTimeoutMS, requestTimeoutMS,
            LegacyHadoopConfigurationSource.asHadoopConfiguration(conf));
  }

  /**
   * When a new OM is bootstrapped, add it to the peerNode map.
   */
  public void addNewPeerNode(OMNodeDetails newOMNode) {
    peerNodesMap.put(newOMNode.getNodeId(), newOMNode);
  }

  /**
   * When an OM is decommissioned, remove it from the peerNode map.
   */
  public void removeDecommissionedPeerNode(String decommNodeId) {
    peerNodesMap.remove(decommNodeId);
  }

  @Override
  public void downloadSnapshot(String leaderNodeID, File targetFile)
      throws IOException {
    OMNodeDetails leader = peerNodesMap.get(leaderNodeID);
    URL omCheckpointUrl = leader.getOMDBCheckpointEndpointUrl(
        httpPolicy.isHttpEnabled(), true);
    LOG.info("Downloading latest checkpoint from Leader OM {}. Checkpoint: {} URL: {}",
        leaderNodeID, targetFile.getName(), omCheckpointUrl);
    SecurityUtil.doAsCurrentUser(() -> {
      HttpURLConnection connection = (HttpURLConnection)
          connectionFactory.openConnection(omCheckpointUrl, spnegoEnabled);

      connection.setRequestMethod("POST");
      String contentTypeValue = "multipart/form-data; boundary=" +
          MULTIPART_FORM_DATA_BOUNDARY;
      connection.setRequestProperty("Content-Type", contentTypeValue);
      connection.setDoOutput(true);
      writeFormData(connection,
          HAUtils.getExistingFiles(getCandidateDir()));

      connection.connect();
      int errorCode = connection.getResponseCode();
      if ((errorCode != HTTP_OK) && (errorCode != HTTP_CREATED)) {
        throw new IOException("Unexpected exception when trying to reach " +
            "OM to download latest checkpoint. Checkpoint URL: " +
            omCheckpointUrl + ". ErrorCode: " + errorCode);
      }

      try (InputStream inputStream = connection.getInputStream()) {
        downloadFileWithProgress(inputStream, targetFile);
      } catch (IOException ex) {
        boolean deleted = FileUtils.deleteQuietly(targetFile);
        if (!deleted) {
          LOG.error("OM snapshot which failed to download {} cannot be deleted",
              targetFile);
        }
        throw ex;
      } finally {
        connection.disconnect();
      }
      return null;
    });
  }

  /**
   * Writes data from the given InputStream to the target file while logging download progress every 30 seconds.
   */
  public static void downloadFileWithProgress(InputStream inputStream, File targetFile)
          throws IOException {
    try (OutputStream outputStream = Files.newOutputStream(targetFile.toPath())) {
      byte[] buffer = new byte[8 * 1024];
      long totalBytesRead = 0;
      int bytesRead;
      long lastLoggedTime = Time.monotonicNow();

      while ((bytesRead = inputStream.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;

        // Log progress every 30 seconds
        if (Time.monotonicNow() - lastLoggedTime >= 30000) {
          LOG.info("Downloading '{}': {} KB downloaded so far...",
              targetFile.getName(), totalBytesRead / (1024));
          lastLoggedTime = Time.monotonicNow();
        }
      }

      LOG.info("Download completed for '{}'. Total size: {} KB",
          targetFile.getName(), totalBytesRead / (1024));
    }
  }

  @Override
  public DBCheckpoint getCheckpointFromUntarredDb(Path untarredDbDir) throws IOException {
    return new InodeMetadataRocksDBCheckpoint(untarredDbDir);
  }

  /**
   * Writes form data to output stream as any HTTP client would for a
   * multipart/form-data request.
   * Proper form data includes separator, content disposition and value
   * separated by a new line.
   * Example:
   * <pre>
   * -----XXX
   * Content-Disposition: form-data; name="field1"
   *
   * value1</pre>
   * @param connection HTTP URL connection which output stream is used.
   * @param sstFiles SST files for exclusion.
   * @throws IOException if an exception occured during writing to output
   * stream.
   */
  public static void writeFormData(HttpURLConnection connection,
      List<String> sstFiles) throws IOException {
    try (DataOutputStream out =
             new DataOutputStream(connection.getOutputStream())) {
      String toExcludeSstField =
          "name=\"" + OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST + "[]" + "\"";
      String crNl = "\r\n";
      String contentDisposition =
          "Content-Disposition: form-data; " + toExcludeSstField + crNl + crNl;
      String separator = "--" + MULTIPART_FORM_DATA_BOUNDARY;

      if (sstFiles.isEmpty()) {
        out.writeBytes(separator + crNl);
        out.writeBytes(contentDisposition);
      }

      for (String sstFile : sstFiles) {
        out.writeBytes(separator + crNl);
        out.writeBytes(contentDisposition);
        out.writeBytes(sstFile + crNl);
      }
      out.writeBytes(separator + "--" + crNl);
    }
  }

  @Override
  public void close() throws IOException {
    if (connectionFactory != null) {
      connectionFactory.destroy();
    }
  }

}
