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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_CHECKSUM_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_DATA_DIR;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getContainerCommandResponse;
import static org.apache.hadoop.ozone.container.common.impl.ContainerData.CHARSET_ENCODING;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * A mix of helper functions for containers.
 */
public final class ContainerUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerUtils.class);

  private ContainerUtils() {
    //never constructed.
  }

  /**
   * Logs the error and returns a response to the caller.
   *
   * @param log - Logger
   * @param ex - Exception
   * @param request - Request Object
   * @return Response
   */
  public static ContainerCommandResponseProto logAndReturnError(
      Logger log, StorageContainerException ex,
      ContainerCommandRequestProto request) {
    String logInfo = "Operation: {} , Trace ID: {} , Message: {} , " +
        "Result: {} , StorageContainerException Occurred.";
    if (ex.getResult() == CLOSED_CONTAINER_IO ||
        ex.getResult() == CONTAINER_NOT_OPEN) {
      if (log.isDebugEnabled()) {
        log.debug(logInfo, request.getCmdType(), request.getTraceID(),
            ex.getMessage(), ex.getResult().getValueDescriptor().getName(), ex);
      }
    } else {
      log.warn(logInfo, request.getCmdType(), request.getTraceID(),
          ex.getMessage(), ex.getResult().getValueDescriptor().getName(), ex);
    }
    return getContainerCommandResponse(request, ex.getResult(), ex.getMessage())
        .build();
  }

  /**
   * get containerName from a container file.
   *
   * @param containerFile - File
   * @return Name of the container.
   */
  public static String getContainerNameFromFile(File containerFile) {
    Preconditions.checkNotNull(containerFile);
    return Paths.get(containerFile.getParent()).resolve(
        removeExtension(containerFile.getName())).toString();
  }

  public static long getContainerIDFromFile(File containerFile) {
    Preconditions.checkNotNull(containerFile);
    String containerID = getContainerNameFromFile(containerFile);
    return Long.parseLong(containerID);
  }

  /**
   * Verifies that this is indeed a new container.
   *
   * @param containerFile - Container File to verify
   */
  public static void verifyIsNewContainer(File containerFile) throws
      FileAlreadyExistsException {
    Logger log = LoggerFactory.getLogger(ContainerSet.class);
    Preconditions.checkNotNull(containerFile, "containerFile Should not be " +
        "null");
    if (containerFile.getParentFile().exists()) {
      log.error("Container already exists on disk. File: {}", containerFile
          .toPath());
      throw new FileAlreadyExistsException("container already exists on " +
          "disk.");
    }
  }

  public static String getContainerDbFileName(String containerName) {
    return containerName + OzoneConsts.DN_CONTAINER_DB;
  }

  /**
   * Persistent a {@link DatanodeDetails} to a local file.
   *
   * @throws IOException when read/write error occurs
   */
  public static synchronized void writeDatanodeDetailsTo(
      DatanodeDetails datanodeDetails, File path, ConfigurationSource conf)
      throws IOException {
    if (path.exists()) {
      if (!path.delete() || !path.createNewFile()) {
        throw new IOException("Unable to overwrite the datanode ID file.");
      }
    } else {
      if (!path.getParentFile().exists() &&
          !path.getParentFile().mkdirs()) {
        throw new IOException("Unable to create datanode ID directories.");
      }
    }
    DatanodeIdYaml.createDatanodeIdFile(datanodeDetails, path, conf);
  }

  /**
   * Read {@link DatanodeDetails} from a local ID file.
   * Use {@link DatanodeDetails#validateDatanodeIpAddress()} to ensure that the IP address matches with the hostname
   *
   * @param path ID file local path
   * @return {@link DatanodeDetails}
   * @throws IOException If the id file is malformed or other I/O exceptions
   */
  public static synchronized DatanodeDetails readDatanodeDetailsFrom(File path)
      throws IOException {
    if (!path.exists()) {
      throw new IOException("Datanode ID file not found.");
    }
    try {
      return DatanodeIdYaml.readDatanodeIdFile(path);
    } catch (IOException e) {
      LOG.warn("Error loading DatanodeDetails yaml from {}",
          path.getAbsolutePath(), e);
      // Try to load as protobuf before giving up
      try (InputStream in = Files.newInputStream(path.toPath())) {
        return DatanodeDetails.getFromProtoBuf(
            HddsProtos.DatanodeDetailsProto.parseFrom(in));
      } catch (IOException io) {
        throw new IOException("Failed to parse DatanodeDetails from "
            + path.getAbsolutePath(), io);
      }
    }
  }

  /**
   * Verify that the checksum stored in containerData is equal to the
   * computed checksum.
   */
  public static void verifyChecksum(ContainerData containerData,
      ConfigurationSource conf) throws IOException {
    boolean enabled = conf.getBoolean(
            HddsConfigKeys.HDDS_CONTAINER_CHECKSUM_VERIFICATION_ENABLED,
            HddsConfigKeys.
                    HDDS_CONTAINER_CHECKSUM_VERIFICATION_ENABLED_DEFAULT);
    if (enabled) {
      String storedChecksum = containerData.getChecksum();

      Yaml yaml = ContainerDataYaml.getYamlForContainerType(
          containerData.getContainerType(),
          containerData instanceof KeyValueContainerData &&
              ((KeyValueContainerData)containerData).getReplicaIndex() > 0);
      containerData.computeAndSetChecksum(yaml);
      String computedChecksum = containerData.getChecksum();

      if (storedChecksum == null || !storedChecksum.equals(computedChecksum)) {
        throw new StorageContainerException("Container checksum error for " +
                "ContainerID: " + containerData.getContainerID() + ". " +
                "\nStored Checksum: " + storedChecksum +
                "\nExpected Checksum: " + computedChecksum,
                CONTAINER_CHECKSUM_ERROR);
      }
    }
  }

  /**
   * Return the SHA-256 checksum of the containerData.
   * @param containerDataYamlStr ContainerData as a Yaml String
   * @return Checksum of the container data
   */
  public static String getChecksum(String containerDataYamlStr)
      throws StorageContainerException {
    MessageDigest sha;
    try {
      sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      sha.update(containerDataYamlStr.getBytes(CHARSET_ENCODING));
      return DigestUtils.sha256Hex(sha.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new StorageContainerException("Unable to create Message Digest, " +
          "usually this is a java configuration issue.", NO_SUCH_ALGORITHM);
    }
  }

  public static boolean recentlyScanned(Container<?> container,
      long minScanGap, Logger log) {
    Optional<Instant> lastScanTime =
        container.getContainerData().lastDataScanTime();
    Instant now = Instant.now();
    // Container is considered recently scanned if it was scanned within the
    // configured time frame. If the optional is empty, the container was
    // never scanned.
    boolean recentlyScanned = lastScanTime.map(scanInstant ->
        Duration.between(now, scanInstant).abs()
            .compareTo(Duration.ofMillis(minScanGap)) < 0)
        .orElse(false);

    if (recentlyScanned && log.isDebugEnabled()) {
      log.debug("Skipping scan for container {} which was last " +
              "scanned at {}. Current time is {}.",
          container.getContainerData().getContainerID(), lastScanTime.get(),
          now);
    }

    return recentlyScanned;
  }

  /**
   * Get the .container file from the containerBaseDir.
   * @param containerBaseDir container base directory. The name of this
   *                         directory is same as the containerID
   * @return the .container file
   */
  public static File getContainerFile(File containerBaseDir) {
    // Container file layout is
    // .../<<containerID>>/metadata/<<containerID>>.container
    String containerFilePath = OzoneConsts.CONTAINER_META_PATH + File.separator
        + getContainerID(containerBaseDir) + OzoneConsts.CONTAINER_EXTENSION;
    return new File(containerBaseDir, containerFilePath);
  }

  /**
   * Get the chunk directory from the containerData.
   *
   * @param containerData {@link ContainerData}
   * @return the file of chunk directory
   * @throws StorageContainerException
   */
  public static File getChunkDir(ContainerData containerData)
      throws StorageContainerException {
    Preconditions.checkNotNull(containerData, "Container data can't be null");

    String chunksPath = containerData.getChunksPath();
    if (chunksPath == null) {
      LOG.error("Chunks path is null in the container data");
      throw new StorageContainerException("Unable to get Chunks directory.",
          UNABLE_TO_FIND_DATA_DIR);
    }

    File chunksDir = new File(chunksPath);
    if (!chunksDir.exists()) {
      LOG.error("Chunks dir {} does not exist", chunksDir.getAbsolutePath());
      throw new StorageContainerException("Chunks directory " +
          chunksDir.getAbsolutePath() + " does not exist.",
          UNABLE_TO_FIND_DATA_DIR);
    }
    return chunksDir;
  }

  /**
   * ContainerID can be decoded from the container base directory name.
   */
  public static long getContainerID(File containerBaseDir) {
    return Long.parseLong(containerBaseDir.getName());
  }

  public static String getContainerTarName(long containerId) {
    return "container-" + containerId + "-" + UUID.randomUUID() + ".tar";
  }

  public static long retrieveContainerIdFromTarName(String tarName)
      throws IOException {
    assert tarName != null;
    Pattern pattern = Pattern.compile("container-(\\d+)-.*\\.tar");
    // Now create matcher object.
    Matcher m = pattern.matcher(tarName);

    if (m.find()) {
      return Long.parseLong(m.group(1));
    } else {
      throw new IOException("Illegal container tar gz file " +
          tarName);
    }
  }

  public static long getPendingDeletionBlocks(ContainerData containerData) {
    if (containerData.getContainerType()
        .equals(ContainerProtos.ContainerType.KeyValueContainer)) {
      return ((KeyValueContainerData) containerData)
          .getNumPendingDeletionBlocks();
    } else {
      // If another ContainerType is available later, implement it
      throw new IllegalArgumentException(
          "getPendingDeletionBlocks for ContainerType: " +
              containerData.getContainerType() +
              " not support.");
    }
  }
}
