/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.container;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.ReportResult;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.DUPLICATE_DATANODE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_SUCH_DATANODE;

/**
 * This data structure maintains the list of containers that is on a datanode.
 * This information is built from the DN container reports.
 */
class Node2ContainerMap {
  private final Map<UUID, Set<ContainerID>> dn2ContainerMap = new ConcurrentHashMap<>();


  /**
   * Constructs a Node2ContainerMap Object.
   */
  Node2ContainerMap() {
    super();
  }

  /**
   * Returns null if there no containers associated with this datanode ID.
   *
   * @param datanode - UUID
   * @return Set of containers or Null.
   */
  public @Nonnull Set<ContainerID> getContainers(@Nonnull UUID datanode) {
    final Set<ContainerID> s = dn2ContainerMap.get(datanode);
    return s != null ? new HashSet<>(s) : Collections.emptySet();
  }

  /**
   * Returns true if this a datanode that is already tracked by
   * Node2ContainerMap.
   *
   * @param datanodeID - UUID of the Datanode.
   * @return True if this is tracked, false if this map does not know about it.
   */
  public boolean isKnownDatanode(@Nonnull UUID datanodeID) {
    return dn2ContainerMap.containsKey(datanodeID);
  }

  /**
   * Insert a new datanode into Node2Container Map.
   *
   * @param datanodeID   -- Datanode UUID
   * @param containerIDs - List of ContainerIDs.
   */
  public void insertNewDatanode(@Nonnull UUID datanodeID, @Nonnull Set<ContainerID> containerIDs)
      throws SCMException {
    if (dn2ContainerMap.putIfAbsent(datanodeID, new HashSet<>(containerIDs)) != null) {
      throw new SCMException("Node already exists in the map", DUPLICATE_DATANODE);
    }
  }

  /**
   * Removes datanode Entry from the map.
   *
   * @param datanodeID - Datanode ID.
   */
  public void removeDatanode(@Nonnull UUID datanodeID) {
    dn2ContainerMap.computeIfPresent(datanodeID, (k, v) -> null);
  }

  public @Nonnull ReportResult.ReportResultBuilder<ContainerID> newBuilder() {
    return new ReportResult.ReportResultBuilder<>();
  }

  public @Nonnull ReportResult<ContainerID> processReport(@Nonnull UUID datanodeID, @Nonnull Set<ContainerID> objects) {
    if (!isKnownDatanode(datanodeID)) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.NEW_DATANODE_FOUND)
          .setNewEntries(objects)
          .build();
    }

    // Conditions like Zero length containers should be handled by removeAll.
    Set<ContainerID> currentSet = dn2ContainerMap.get(datanodeID);
    TreeSet<ContainerID> newObjects = new TreeSet<>(objects);
    newObjects.removeAll(currentSet);

    TreeSet<ContainerID> missingObjects = new TreeSet<>(currentSet);
    missingObjects.removeAll(objects);

    if (newObjects.isEmpty() && missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.ALL_IS_WELL)
          .build();
    }

    if (newObjects.isEmpty() && !missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.MISSING_ENTRIES)
          .setMissingEntries(missingObjects)
          .build();
    }

    if (!newObjects.isEmpty() && missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.NEW_ENTRIES_FOUND)
          .setNewEntries(newObjects)
          .build();
    }

    if (!newObjects.isEmpty() && !missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.MISSING_AND_NEW_ENTRIES_FOUND)
          .setNewEntries(newObjects)
          .setMissingEntries(missingObjects)
          .build();
    }

    // default status & Make compiler happy
    return newBuilder()
        .setStatus(ReportResult.ReportStatus.ALL_IS_WELL)
        .build();
  }

  /**
   * Updates the Container list of an existing DN.
   *
   * @param datanodeID - UUID of DN.
   * @param containers - Set of Containers tht is present on DN.
   * @throws SCMException - if we don't know about this datanode, for new DN
   *                        use addDatanodeInContainerMap.
   */
  public void setContainersForDatanode(@Nonnull UUID datanodeID, @Nonnull Set<ContainerID> containers)
      throws SCMException {
    if (dn2ContainerMap.computeIfPresent(datanodeID, (k, v) -> new HashSet<>(containers)) == null) {
      throw new SCMException("No such datanode", NO_SUCH_DATANODE);
    }
  }

  public int size() {
    return dn2ContainerMap.size();
  }
}
