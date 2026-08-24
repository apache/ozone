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

package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.RECOVERING;
import static org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo.INVALID_REPLICA_INDEX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that manages Containers created on the datanode.
 */
public class ContainerSet implements Iterable<Container<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerSet.class);

  private final ConcurrentSkipListMap<Long, Container<?>> containerMap = new
      ConcurrentSkipListMap<>();
  private final ConcurrentSkipListSet<Long> missingContainerSet =
      new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListMap<Long, Long> recoveringContainerMap =
      new ConcurrentSkipListMap<>();
  private final Clock clock;
  private long recoveringTimeout;
  @Nullable
  private final WitnessedContainerMetadataStore containerMetadataStore;
  // Handler that will be invoked when a scan of a container in this set is requested.
  private OnDemandContainerScanner containerScanner;

  public static ContainerSet newReadOnlyContainerSet(long recoveringTimeout) {
    return new ContainerSet(null, recoveringTimeout);
  }

  public static ContainerSet newRwContainerSet(
      WitnessedContainerMetadataStore metadataStore, long recoveringTimeout) {
    Objects.requireNonNull(metadataStore, "metadataStore == null");
    return new ContainerSet(metadataStore, recoveringTimeout);
  }

  private ContainerSet(WitnessedContainerMetadataStore containerMetadataStore, long recoveringTimeout) {
    this(containerMetadataStore, recoveringTimeout, null);
  }

  ContainerSet(WitnessedContainerMetadataStore containerMetadataStore, long recoveringTimeout, Clock clock) {
    this.clock = clock != null ? clock : Clock.systemUTC();
    this.containerMetadataStore = containerMetadataStore;
    this.recoveringTimeout = recoveringTimeout;
  }

  public long getCurrentTime() {
    return clock.millis();
  }

  @Nullable
  public WitnessedContainerMetadataStore getContainerMetadataStore() {
    return containerMetadataStore;
  }

  @VisibleForTesting
  public void setRecoveringTimeout(long recoveringTimeout) {
    this.recoveringTimeout = recoveringTimeout;
  }

  /**
   * Add Container to container map. This would fail if the container is already present or has been marked as missing.
   * @param container container to be added
   * @return If container is added to containerMap returns true, otherwise
   * false
   */
  public boolean addContainer(Container<?> container) throws StorageContainerException {
    return addContainer(container, false);
  }

  /**
   * Add Container to container map. This would overwrite the container even if it is missing. But would fail if the
   * container is already present.
   * @param container container to be added
   * @return If container is added to containerMap returns true, otherwise
   * false
   */
  public boolean addContainerByOverwriteMissingContainer(Container<?> container) throws StorageContainerException {
    return addContainer(container, true);
  }

  public void ensureContainerNotMissing(long containerId, State state) throws StorageContainerException {
    if (missingContainerSet.contains(containerId)) {
      throw new StorageContainerException(String.format("Container with container Id %d with state : %s is missing in" +
          " the DN.", containerId, state),
          ContainerProtos.Result.CONTAINER_MISSING);
    }
  }

  /**
   * @param scanner The scanner instance will be invoked when a scan of a container in this set is requested.
   */
  public void registerOnDemandScanner(OnDemandContainerScanner scanner) {
    this.containerScanner = scanner;
  }

  /**
   * Triggers a scan of a container in this set. This is a no-op if no scanner is registered or the container does not
   * exist in the set.
   * @param containerID The container in this set to scan.
   */
  public void scanContainer(long containerID, String reasonForScan) {
    if (containerScanner != null) {
      Container<?> container = getContainer(containerID);
      if (container != null) {
        containerScanner.scanContainer(container, reasonForScan);
      } else {
        LOG.warn("Request to scan container {} which was not found in the container set", containerID);
      }
    }
  }

  /**
   * Triggers a scan of a container in this set regardless of whether it was recently scanned.
   * This is a no-op if no scanner is registered or the container does not exist in the set.
   * @param containerID The container in this set to scan.
   */
  public void scanContainerWithoutGap(long containerID, String reasonForScan) {
    if (containerScanner != null) {
      Container<?> container = getContainer(containerID);
      if (container != null) {
        containerScanner.scanContainerWithoutGap(container, reasonForScan);
      } else {
        LOG.warn("Request to scan container {} which was not found in the container set", containerID);
      }
    }
  }

  /**
   * Add Container to container map.
   * @param container container to be added
   * @param overwrite if true should overwrite the container if the container was missing.
   * @return If container is added to containerMap returns true, otherwise
   * false
   */
  private boolean addContainer(Container<?> container, boolean overwrite) throws
      StorageContainerException {
    Objects.requireNonNull(container, "container == null");

    long containerId = container.getContainerData().getContainerID();
    State containerState = container.getContainerData().getState();
    if (!overwrite) {
      ensureContainerNotMissing(containerId, containerState);
    }
    if (containerMap.putIfAbsent(containerId, container) == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container with container Id {} is added to containerMap",
            containerId);
      }
      updateContainerIdTable(containerId, container.getContainerData());
      missingContainerSet.remove(containerId);
      if (container.getContainerData().getState() == RECOVERING) {
        recoveringContainerMap.put(
            clock.millis() + recoveringTimeout, containerId);
      }
      HddsVolume volume = container.getContainerData().getVolume();
      if (volume != null) {
        volume.addContainer(containerId);
      }
      return true;
    } else {
      LOG.warn("Container already exists with container Id {}", containerId);
      throw new StorageContainerException("Container already exists with " +
          "container Id " + containerId,
          ContainerProtos.Result.CONTAINER_EXISTS);
    }
  }

  private void updateContainerIdTable(long containerId, ContainerData containerData) throws StorageContainerException {
    if (null != containerMetadataStore) {
      try {
        ContainerID containerIdObj = ContainerID.valueOf(containerId);
        Table<ContainerID, ContainerCreateInfo> containerCreateInfoTable =
            containerMetadataStore.getContainerCreateInfoTable();
        ContainerCreateInfo containerCreateInfo = containerCreateInfoTable.get(containerIdObj);
        if (containerCreateInfo == null || containerCreateInfo.getReplicaIndex() == INVALID_REPLICA_INDEX) {
          containerCreateInfoTable.put(containerIdObj,
              ContainerCreateInfo.valueOf(containerData.getState(), containerData.getReplicaIndex()));
        }
      } catch (IOException e) {
        throw new StorageContainerException(e, ContainerProtos.Result.IO_EXCEPTION);
      }
    }
  }

  /**
   * Update Container to container map.
   * @param container container to be added
   * @return If container is added to containerMap returns true, otherwise
   * false
   */
  public Container updateContainer(Container<?> container) throws
      StorageContainerException {
    Objects.requireNonNull(container, "container cannot be null");

    long containerId = container.getContainerData().getContainerID();
    if (!containerMap.containsKey(containerId)) {
      LOG.error("Container doesn't exists with container Id {}", containerId);
      throw new StorageContainerException("Container doesn't exist with " +
          "container Id " + containerId,
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
    } else {
      LOG.debug("Container with container Id {} is updated to containerMap",
          containerId);
      Container<?> oldContainer = containerMap.put(containerId, container);
      HddsVolume volume = container.getContainerData().getVolume();
      if (volume != null) {
        volume.addContainer(container.getContainerData().getContainerID());
      }
      if (oldContainer != null) {
        volume = oldContainer.getContainerData().getVolume();
        if (volume != null) {
          volume.removeContainer(oldContainer.getContainerData().getContainerID());
        }
      }
      return oldContainer;
    }
  }

  /**
   * Returns the Container with specified containerId.
   * @param containerId ID of the container to get
   * @return Container
   */
  public Container<?> getContainer(long containerId) {
    Preconditions.checkState(containerId >= 0, "Container Id cannot be negative.");
    return containerMap.get(containerId);
  }

  /**
   * Removes container from both memory and database. This should be used when the containerData on disk has been
   * removed completely from the node.
   * @param containerId
   * @return True if container is removed from containerMap.
   * @throws StorageContainerException
   */
  public boolean removeContainer(long containerId) throws StorageContainerException {
    return removeContainer(containerId, false, true);
  }

  /**
   * Removes containerId from memory. This needs to be used when the container is still present on disk, and the
   * inmemory state of the container needs to be updated.
   * @param containerId
   * @return True if container is removed from containerMap.
   * @throws StorageContainerException
   */
  public boolean removeContainerOnlyFromMemory(long containerId) throws StorageContainerException {
    return removeContainer(containerId, false, false);
  }

  /**
   * Marks a container to be missing, thus it removes the container from inmemory containerMap and marks the
   * container as missing.
   * @param containerId
   * @return True if container is removed from containerMap.
   * @throws StorageContainerException
   */
  public boolean removeMissingContainer(long containerId) throws StorageContainerException {
    return removeContainer(containerId, true, false);
  }

  /**
   * Removes the Container matching with specified containerId.
   * @param containerId ID of the container to remove
   * @return If container is removed from containerMap returns true, otherwise
   * false
   */
  private boolean removeContainer(long containerId, boolean markMissing, boolean removeFromDB)
      throws StorageContainerException {
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");
    //We need to add to missing container set before removing containerMap since there could be write chunk operation
    // that could recreate the container in another volume if we remove it from the map before adding to missing
    // container.
    if (markMissing) {
      missingContainerSet.add(containerId);
    }
    Container<?> removed = containerMap.remove(containerId);
    if (removeFromDB) {
      deleteFromContainerTable(containerId);
    }
    if (removed == null) {
      LOG.debug("Container with containerId {} is not present in " +
          "containerMap", containerId);
      return false;
    } else {
      HddsVolume volume = removed.getContainerData().getVolume();
      if (volume != null) {
        volume.removeContainer(containerId);
      }
      LOG.debug("Container with containerId {} is removed from containerMap",
          containerId);
      return true;
    }
  }

  private void deleteFromContainerTable(long containerId) throws StorageContainerException {
    if (null != containerMetadataStore) {
      try {
        containerMetadataStore.getContainerCreateInfoTable().delete(ContainerID.valueOf(containerId));
      } catch (IOException e) {
        throw new StorageContainerException(e, ContainerProtos.Result.IO_EXCEPTION);
      }
    }
  }

  /**
   * Removes the Recovering Container matching with specified containerId.
   * @param containerId ID of the container to remove.
   * @return true If container is removed from containerMap returns true,
   * otherwise false.
   */
  public boolean removeRecoveringContainer(long containerId) {
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");
    //it might take a little long time to iterate all the entries
    // in recoveringContainerMap, but it seems ok here since:
    // 1 In the vast majority of cases，there will not be too
    // many recovering containers.
    // 2 closing container is not a sort of urgent action
    //
    // we can revisit here if any performance problem happens
    Iterator<Map.Entry<Long, Long>> it = getRecoveringContainerIterator();
    while (it.hasNext()) {
      Map.Entry<Long, Long> entry = it.next();
      if (entry.getValue() == containerId) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * Return number of containers in container map.
   * @return container count
   */
  @VisibleForTesting
  public int containerCount() {
    return containerMap.size();
  }

  /**
   * Remove all containers belonging to failed volume.
   * Send FCR which will not contain removed containers.
   *
   * @param  context StateContext
   */
  public void handleVolumeFailures(StateContext context) throws StorageContainerException {
    AtomicBoolean failedVolume = new AtomicBoolean(false);
    AtomicInteger containerCount = new AtomicInteger(0);
    for (Container<?> c : containerMap.values()) {
      ContainerData data = c.getContainerData();
      if (data.getVolume().isFailed()) {
        removeMissingContainer(data.getContainerID());
        LOG.debug("Removing Container {} as the Volume {} " +
            "has failed", data.getContainerID(), data.getVolume());
        failedVolume.set(true);
        containerCount.incrementAndGet();
        ContainerLogger.logLost(data, "Volume failure");
      }
    }

    if (failedVolume.get()) {
      try {
        LOG.info("Removed {} containers on failed volumes",
            containerCount.get());
        // There are some failed volume(container), send FCR to SCM
        Message report = context.getFullContainerReportDiscardPendingICR();
        context.refreshFullReport(report);
        context.getParent().triggerHeartbeat();
      } catch (Exception e) {
        LOG.error("Failed to send FCR in Volume failure", e);
      }
    }
  }

  @Override
  public Iterator<Container<?>> iterator() {
    return containerMap.values().iterator();
  }

  /**
   * Return an container Iterator over
   * {@link ContainerSet#recoveringContainerMap}.
   * @return {@literal Iterator<Container<?>>}
   */
  public Iterator<Map.Entry<Long, Long>> getRecoveringContainerIterator() {
    return recoveringContainerMap.entrySet().iterator();
  }

  /**
   * Return an iterator of containers associated with the specified volume.
   * The iterator is sorted by last data scan timestamp in increasing order.
   *
   * @param  volume the HDDS volume which should be used to filter containers
   * @return {@literal Iterator<Container<?>>}
   */
  public Iterator<Container<?>> getContainerIterator(HddsVolume volume) {
    Objects.requireNonNull(volume, "volume == null");
    Iterator<Long> containerIdIterator = volume.getContainerIterator();

    List<Container<?>> containers = new ArrayList<>();
    while (containerIdIterator.hasNext()) {
      Long containerId = containerIdIterator.next();
      Container<?> container = containerMap.get(containerId);
      if (container != null && container.getContainerData().getVolume() == volume) {
        containers.add(container);
      }
    }
    containers.sort(ContainerDataScanOrder.INSTANCE);

    return containers.iterator();
  }

  /**
   * Get the number of containers based on the given volume.
   *
   * @param volume hdds volume.
   * @return number of containers
   */
  public long containerCount(HddsVolume volume) {
    Objects.requireNonNull(volume, "volume == null");
    return volume.getContainerCount();
  }

  /**
   * Return an containerMap iterator over {@link ContainerSet#containerMap}.
   * @return containerMap Iterator
   */
  public Iterator<Map.Entry<Long, Container<?>>> getContainerMapIterator() {
    return containerMap.entrySet().iterator();
  }

  /**
   * Return a copy of the containerMap.
   * @return containerMap
   */
  @VisibleForTesting
  public Map<Long, Container<?>> getContainerMapCopy() {
    return ImmutableMap.copyOf(containerMap);
  }

  public Map<Long, Container<?>> getContainerMap() {
    return Collections.unmodifiableMap(containerMap);
  }

  /**
   * A simple interface for container Iterations.
   * <p>
   * This call make no guarantees about consistency of the data between
   * different list calls. It just returns the best known data at that point of
   * time. It is possible that using this iteration you can miss certain
   * container from the listing.
   *
   * @param startContainerId - Return containers with Id &gt;= startContainerId.
   * @param count - how many to return
   * @param data - Actual containerData
   */
  public void listContainer(long startContainerId, long count,
                            List<ContainerData> data) throws
      StorageContainerException {
    Objects.requireNonNull(data, "data == null");
    Preconditions.checkState(startContainerId >= 0,
        "Start container Id cannot be negative");
    Preconditions.checkState(count > 0,
        "max number of containers returned " +
            "must be positive");
    LOG.debug("listContainer returns containerData starting from {} of count " +
        "{}", startContainerId, count);
    ConcurrentNavigableMap<Long, Container<?>> map;
    if (startContainerId == 0) {
      map = containerMap;
    } else {
      map = containerMap.tailMap(startContainerId, true);
    }
    int currentCount = 0;
    for (Container<?> entry : map.values()) {
      if (currentCount < count) {
        data.add(entry.getContainerData());
        currentCount++;
      } else {
        return;
      }
    }
  }

  /**
   * Get container report.
   *
   * @return The container report.
   */
  public ContainerReportsProto getContainerReport() throws IOException {
    LOG.debug("Starting container report iteration.");

    ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();
    // No need for locking since containerMap is a ConcurrentSkipListMap
    // And we can never get the exact state since close might happen
    // after we iterate a point.
    List<Container<?>> containers = new ArrayList<>(containerMap.values());
    // Incremental Container reports can read stale container information
    // This is to make sure FCR and ICR can be linearized and processed by
    // consumers such as SCM.
    synchronized (this) {
      for (Container<?> container : containers) {
        if (container.getContainerState()
            == ContainerProtos.ContainerDataProto.State.RECOVERING) {
          // Skip the recovering containers in ICR and FCR for now.
          continue;
        }
        crBuilder.addReports(container.getContainerReport());
      }
    }
    return crBuilder.build();
  }

  public Set<Long> getMissingContainerSet() {
    return missingContainerSet;
  }

  /**
   * Builds the missing container set by taking a diff between total no
   * containers actually found and number of containers which actually
   * got created. It also validates the BCSID stored in the snapshot file
   * for each container as against what is reported in containerScan.
   * This will only be called during the initialization of Datanode Service
   * when  it still not a part of any write Pipeline.
   * @param container2BCSIDMap Map of containerId to BCSID persisted in the
   *                           Ratis snapshot
   */
  public <T> void buildMissingContainerSetAndValidate(Map<T, Long> container2BCSIDMap, ToLongFunction<T> getId) {
    container2BCSIDMap.entrySet().parallelStream().forEach((mapEntry) -> {
      final long id = getId.applyAsLong(mapEntry.getKey());
      if (!containerMap.containsKey(id)) {
        LOG.warn("Adding container {} to missing container set.", id);
        missingContainerSet.add(id);
      } else {
        Container<?> container = containerMap.get(id);
        long containerBCSID = container.getBlockCommitSequenceId();
        long snapshotBCSID = mapEntry.getValue();
        if (containerBCSID < snapshotBCSID) {
          LOG.warn(
              "Marking container {} unhealthy as reported BCSID {} is smaller"
                  + " than ratis snapshot recorded value {}", id,
              containerBCSID, snapshotBCSID);
          // just mark the container unhealthy. Once the DatanodeStateMachine
          // thread starts it will send container report to SCM where these
          // unhealthy containers would be detected
          try {
            container.markContainerUnhealthy();
          } catch (StorageContainerException sce) {
            // The container will still be marked unhealthy in memory even if
            // exception occurs. It won't accept any new transactions and will
            // be handled by SCM. Eve if dn restarts, it will still be detected
            // as unheathy as its BCSID won't change.
            LOG.error("Unable to persist unhealthy state for container {}", id);
          }
        }
      }
    });
  }
}
