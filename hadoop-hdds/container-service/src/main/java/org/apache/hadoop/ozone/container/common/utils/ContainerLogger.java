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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

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
public class ContainerLogger {

  private static final String LOG_NAME = "ContainerLog";
  private static final Logger LOG = LoggerFactory.getLogger(LOG_NAME);
  private static final String FIELD_SEPARATOR = " | ";

  /**
   * Represents the valid roles a datanode can play in container replication 
   * or reconstruction.
   */
  private enum DatanodeRoles {
    SOURCES("Sources"),
    DESTINATION("Destination");
    
    private final String name;

    DatanodeRoles(String name) {
      this.name = name;
    }
    
    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Logged when an open container is first created.
   *
   * @param containerData The container that was created and opened.
   */
  public void logOpen(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container is moved to the closing state.
   *
   * @param containerData The container that was marked as closing.
   */
  public void logClosing(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a Ratis container is moved to the quasi-closed state.
   *
   * @param containerData The container that was quasi-closed.
   * @param message The reason the container was quasi-closed, if known.
   */
  public void logQuasiClosed(ContainerData containerData, String message) {
    LOG.warn(getMessage(containerData, message));
  }

  /**
   * Logged when a container is moved to the closed state.
   *
   * @param containerData The container that was closed.
   */
  public void logClosed(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a container is moved to the unhealthy state.
   *
   * @param containerData The container that was marked unhealthy.
   * @param message The reason the container was marked unhealthy.
   */
  public void logUnhealthy(ContainerData containerData, String message) {
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
  public void logLost(ContainerData containerData, String message) {
    LOG.error(getMessage(containerData, message));
  }

  /**
   * Logged when a container is deleted because it is empty.
   *
   * @param containerData The container that was deleted.
   */
  public void logDeletedEmpty(ContainerData containerData) {
    LOG.info(getMessage(containerData));
  }

  /**
   * Logged when a non-empty container is force deleted. This is usually due
   * to over-replication.
   *
   * @param containerData The container that was deleted.
   * @param message The reason the container was force deleted.
   */
  public void logForceDeleted(ContainerData containerData, String message) {
    LOG.info(getMessage(containerData, message));
  }

  /**
   * Logged when a container is copied in to this datanode.
   *
   * @param containerData The container that was imported to this datanode.
   * @param source The datanode that this replica was imported from.
   */
  public void logImported(ContainerData containerData, DatanodeDetails source) {
    LOG.info(getMessage(containerData, DatanodeRoles.SOURCES, source));
  }

  /**
   * Logged when a container is copied from this datanode.
   *
   * @param containerData The container that was exported from this datanode.
   * @param destination The datanode that this replica was exported to.
   */
  public void logExported(ContainerData containerData, DatanodeDetails destination) {
    LOG.info(getMessage(containerData, DatanodeRoles.DESTINATION, destination));
  }

  /**
   * Logged when a container is recovered using EC offline reconstruction.
   *
   * @param containerData The container that was recovered on this datanode.
   * @param sources The datanode replicas that were used to reconstruct this
   *                container.
   */
  public void logRecovered(ContainerData containerData,
      DatanodeDetails... sources) {
    LOG.info(getMessage(containerData, DatanodeRoles.SOURCES, sources));
  }

  private String getMessage(ContainerData containerData,
      DatanodeRoles datanodeRole, DatanodeDetails... datanodes) {
    return getMessage(containerData, datanodeRole + "=" +
        String.join(", ", Arrays.stream(datanodes)
            .map(DatanodeDetails::toString)
            .toArray(String[]::new)));
  }

  private String getMessage(ContainerData containerData, String message) {
    return String.join(FIELD_SEPARATOR, getMessage(containerData), message);
  }

  private String getMessage(ContainerData containerData) {
    return String.join(FIELD_SEPARATOR,
        "ID=" + containerData.getContainerID(),
        "Index=" + containerData.getReplicaIndex(),
        "State=" + containerData.getState());
  }
}
