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

package org.apache.hadoop.ozone.container.common.utils;

import static org.apache.hadoop.hdds.HddsUtils.checksumToString;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class defining methods to write to the datanode container log.
 *
 * The container log contains brief messages about container replica level
 * events like state changes or replication/reconstruction. Messages in this
 * log are minimal, so it can be used to track the history of every container
 * on a datanode for a long period of time without rolling off.
 *
 * All messages should be logged after their corresponding event succeeds, to
 * prevent misleading state changes or logs filling with retries on errors.
 * Errors and retries belong in the main datanode application log.
 */
public final class ContainerLogger {

  @VisibleForTesting
  public static final String LOG_NAME = "ContainerLog";
  private static final Logger LOG = LogManager.getLogger(LOG_NAME);
  private static final String FIELD_SEPARATOR = " | ";

  private ContainerLogger() { }

  /**
   * Logged when an open container is first created.
   *
   * @param containerData The container that was created and opened.
   */
  public static void logOpen(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container is moved to the closing state.
   *
   * @param containerData The container that was marked as closing.
   */
  public static void logClosing(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a Ratis container is moved to the quasi-closed state.
   *
   * @param containerData The container that was quasi-closed.
   * @param message The reason the container was quasi-closed, if known.
   */
  public static void logQuasiClosed(ContainerData containerData,
      String message) {
    LOG.warn(getMessage(containerData, message));
  }

  /**
   * Logged when a container is moved to the closed state.
   *
   * @param containerData The container that was closed.
   */
  public static void logClosed(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container is moved to the unhealthy state.
   *
   * @param containerData The container that was marked unhealthy.
   * @param reason The reason the container was marked unhealthy.
   */
  public static void logUnhealthy(ContainerData containerData,
      ScanResult reason) {
    LOG.error(getMessage(containerData, reason.toString()));
  }

  /**
   * Logged when a container is lost from this datanode. Currently this would
   * only happen on volume failure. Container deletes do not count as lost
   * containers.
   *
   * @param containerData The container that was lost.
   * @param message The reason the container was lost.
   */
  public static void logLost(ContainerData containerData, String message) {
    LOG.error(getMessage(containerData, message));
  }

  /**
   * Logged when a container is deleted because it is empty.
   *
   * @param containerData The container that was deleted.
   */
  public static void logDeleted(ContainerData containerData, boolean force) {
    if (force) {
      LOG.info(getMessage(containerData, "Container force deleted"));
    } else {
      LOG.info(getMessage(containerData, "Empty container deleted"));
    }
  }

  /**
   * Logged when a container is copied in to this datanode.
   *
   * @param containerData The container that was imported to this datanode.
   */
  public static void logImported(ContainerData containerData) {
    LOG.info(getMessage(containerData, "Container imported"));
  }

  /**
   * Logged when a container is copied from this datanode.
   *
   * @param containerData The container that was exported from this datanode.
   */
  public static void logExported(ContainerData containerData) {
    LOG.info(getMessage(containerData, "Container exported"));
  }

  /**
   * Logged when a container is recovered using EC offline reconstruction.
   *
   * @param containerData The container that was recovered on this datanode.
   */
  public static void logRecovered(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container's checksum is updated.
   *
   * @param containerData The container which has the updated data checksum.
   * @param oldDataChecksum The old data checksum.
   */
  public static void logChecksumUpdated(ContainerData containerData, long oldDataChecksum) {
    LOG.warn(getMessage(containerData,
        "Container data checksum updated from " + checksumToString(oldDataChecksum) + " to "
            + checksumToString(containerData.getDataChecksum())));
  }

  /**
   * Logged when a container is reconciled.
   *
   * @param containerData The container that was reconciled on this datanode.
   * @param oldDataChecksum The old data checksum.
   */
  public static void logReconciled(ContainerData containerData, long oldDataChecksum, DatanodeDetails peer) {
    if (containerData.getDataChecksum() == oldDataChecksum) {
      LOG.info(getMessage(containerData, "Container reconciled with peer " + peer.toString() +
          ". No change in checksum."));
    } else {
      LOG.warn(getMessage(containerData, "Container reconciled with peer " + peer.toString() +
          ". Checksum updated from " + checksumToString(oldDataChecksum) + " to "
          + checksumToString(containerData.getDataChecksum())));
    }
  }

  /**
   * Logged when a container is successfully moved from one data volume to another.
   *
   * @param containerId The ID of the moved container.
   * @param sourceVolume The source volume path.
   * @param destinationVolume The destination volume path.
   * @param containerSize The size of data moved from container in bytes.
   * @param timeTaken The time taken for the move in milliseconds.
   */
  public static void logMoveSuccess(long containerId, StorageVolume sourceVolume,
      StorageVolume destinationVolume, long containerSize, long timeTaken) {
    LOG.info(getMessage(containerId, sourceVolume, destinationVolume, containerSize, timeTaken));
  }

  private static String getMessage(ContainerData containerData,
                                   String message) {
    return String.join(FIELD_SEPARATOR, getMessage(containerData), message);
  }

  private static String getMessage(ContainerData containerData) {
    return String.join(FIELD_SEPARATOR,
        "ID=" + containerData.getContainerID(),
        "Index=" + containerData.getReplicaIndex(),
        "BCSID=" + containerData.getBlockCommitSequenceId(),
        "State=" + containerData.getState(),
        "Volume=" + containerData.getVolume(),
        "DataChecksum=" + checksumToString(containerData.getDataChecksum()));
  }

  private static String getMessage(long containerId, StorageVolume sourceVolume,
      StorageVolume destinationVolume, long containerSize, long timeTaken) {
    return String.join(FIELD_SEPARATOR,
        "ID=" + containerId,
        "SrcVolume=" + sourceVolume,
        "DestVolume=" + destinationVolume,
        "Size=" + containerSize + " bytes",
        "TimeTaken=" + timeTaken + " ms",
        "Container is moved from SrcVolume to DestVolume");
  }
}
