/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;

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
  private static final Logger LOG = LoggerFactory.getLogger(LOG_NAME);
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
    String message = reason.getFailureType() + " for file " +
        reason.getUnhealthyFile() +
        ". Message: " + reason.getException().getMessage();
    LOG.error(getMessage(containerData, message));
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
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container is copied from this datanode.
   *
   * @param containerData The container that was exported from this datanode.
   */
  public static void logExported(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container is recovered using EC offline reconstruction.
   *
   * @param containerData The container that was recovered on this datanode.
   */
  public static void logRecovered(ContainerData containerData) {
    LOG.info(getMessage(containerData));
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
        "State=" + containerData.getState());
  }
}
