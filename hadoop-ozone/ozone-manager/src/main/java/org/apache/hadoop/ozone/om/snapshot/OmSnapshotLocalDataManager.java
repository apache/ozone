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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml.YAML_FILE_EXTENSION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData.VersionMeta;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.FlatResource;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager.HierarchicalResourceLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.util.ObjectSerializer;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.apache.ratis.util.function.CheckedSupplier;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Manages local data and metadata associated with Ozone Manager (OM) snapshots,
 * including the creation, storage, and representation of data as YAML files.
 */
public class OmSnapshotLocalDataManager implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OmSnapshotLocalDataManager.class);
  private static final String LOCAL_DATA_MANAGER_SERVICE_NAME = "OmSnapshotLocalDataManagerService";

  private final ObjectSerializer<OmSnapshotLocalData> snapshotLocalDataSerializer;
  private final MutableGraph<LocalDataVersionNode> localDataGraph;
  private final Map<UUID, SnapshotVersionsMeta> versionNodeMap;
  private final OMMetadataManager omMetadataManager;
  // Used for acquiring locks on the entire data structure.
  private final ReadWriteLock fullLock;
  // Locks should be always acquired by iterating through the snapshot chain to avoid deadlocks.
  private HierarchicalResourceLockManager locks;
  private Map<UUID, Integer> snapshotToBeCheckedForOrphans;
  private Scheduler scheduler;
  private volatile boolean closed;

  public OmSnapshotLocalDataManager(OMMetadataManager omMetadataManager,
      SnapshotChainManager snapshotChainManager,
      OzoneConfiguration configuration) throws IOException {
    this.localDataGraph = GraphBuilder.directed().build();
    this.omMetadataManager = omMetadataManager;
    this.snapshotLocalDataSerializer = new YamlSerializer<OmSnapshotLocalData>(
        new OmSnapshotLocalDataYaml.YamlFactory()) {

      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
    this.versionNodeMap = new HashMap<>();
    this.fullLock = new ReentrantReadWriteLock();
    init(configuration, snapshotChainManager);
  }

  @VisibleForTesting
  Map<UUID, SnapshotVersionsMeta> getVersionNodeMap() {
    return versionNodeMap;
  }

  /**
   * Returns the path to the YAML file that stores local properties for the given snapshot.
   *
   * @param snapshotPath path to the snapshot checkpoint dir
   * @return the path to the snapshot's local property YAML file
   */
  public static String getSnapshotLocalPropertyYamlPath(Path snapshotPath) {
    return snapshotPath.toString() + YAML_FILE_EXTENSION;
  }

  /**
   * Returns the path to the YAML file that stores local properties for the given snapshot.
   *
   * @param snapshotInfo snapshot metadata
   * @return the path to the snapshot's local property YAML file
   */
  public String getSnapshotLocalPropertyYamlPath(SnapshotInfo snapshotInfo) {
    return getSnapshotLocalPropertyYamlPath(snapshotInfo.getSnapshotId());
  }

  public String getSnapshotLocalPropertyYamlPath(UUID snapshotId) {
    Path snapshotPath = OmSnapshotManager.getSnapshotPath(omMetadataManager, snapshotId);
    return getSnapshotLocalPropertyYamlPath(snapshotPath);
  }

  /**
   * Creates and writes snapshot local properties to a YAML file with not defragged SST file list.
   * @param snapshotStore snapshot metadata manager.
   * @param snapshotInfo snapshot info instance corresponding to snapshot.
   */
  public void createNewOmSnapshotLocalDataFile(RDBStore snapshotStore, SnapshotInfo snapshotInfo) throws IOException {
    try (WritableOmSnapshotLocalDataProvider snapshotLocalData =
        new WritableOmSnapshotLocalDataProvider(snapshotInfo.getSnapshotId(),
            () -> Pair.of(new OmSnapshotLocalData(snapshotInfo.getSnapshotId(),
                OmSnapshotManager.getSnapshotSSTFileList(snapshotStore), snapshotInfo.getPathPreviousSnapshotId()),
                null))) {
      snapshotLocalData.commit();
    }
  }

  public ReadableOmSnapshotLocalDataProvider getOmSnapshotLocalData(SnapshotInfo snapshotInfo) throws IOException {
    return getOmSnapshotLocalData(snapshotInfo.getSnapshotId());
  }

  public ReadableOmSnapshotLocalDataProvider getOmSnapshotLocalData(UUID snapshotId) throws IOException {
    return new ReadableOmSnapshotLocalDataProvider(snapshotId);
  }

  public ReadableOmSnapshotLocalDataProvider getOmSnapshotLocalData(UUID snapshotId, UUID previousSnapshotID)
      throws IOException {
    return new ReadableOmSnapshotLocalDataProvider(snapshotId, previousSnapshotID);
  }

  public WritableOmSnapshotLocalDataProvider getWritableOmSnapshotLocalData(SnapshotInfo snapshotInfo)
      throws IOException {
    return getWritableOmSnapshotLocalData(snapshotInfo.getSnapshotId(), snapshotInfo.getPathPreviousSnapshotId());
  }

  public WritableOmSnapshotLocalDataProvider getWritableOmSnapshotLocalData(UUID snapshotId, UUID previousSnapshotId)
      throws IOException {
    return new WritableOmSnapshotLocalDataProvider(snapshotId, previousSnapshotId);
  }

  public WritableOmSnapshotLocalDataProvider getWritableOmSnapshotLocalData(UUID snapshotId)
      throws IOException {
    return new WritableOmSnapshotLocalDataProvider(snapshotId);
  }

  public OmSnapshotLocalData getOmSnapshotLocalData(File snapshotDataPath) throws IOException {
    return snapshotLocalDataSerializer.load(snapshotDataPath);
  }

  private LocalDataVersionNode getVersionNode(UUID snapshotId, int version) {
    if (!versionNodeMap.containsKey(snapshotId)) {
      return null;
    }
    return versionNodeMap.get(snapshotId).getVersionNode(version);
  }

  private void addSnapshotVersionMeta(UUID snapshotId, SnapshotVersionsMeta snapshotVersionsMeta)
      throws IOException {
    if (!versionNodeMap.containsKey(snapshotId) && !snapshotVersionsMeta.getSnapshotVersions().isEmpty()) {
      for (LocalDataVersionNode versionNode : snapshotVersionsMeta.getSnapshotVersions().values()) {
        validateVersionAddition(versionNode);
        LocalDataVersionNode previousVersionNode = versionNode.previousSnapshotId == null ? null :
            getVersionNode(versionNode.previousSnapshotId, versionNode.previousSnapshotVersion);
        localDataGraph.addNode(versionNode);
        if (previousVersionNode != null) {
          localDataGraph.putEdge(versionNode, previousVersionNode);
        }
      }
      versionNodeMap.put(snapshotId, snapshotVersionsMeta);
    }
  }

  void addVersionNodeWithDependents(OmSnapshotLocalData snapshotLocalData) throws IOException {
    if (versionNodeMap.containsKey(snapshotLocalData.getSnapshotId())) {
      return;
    }
    Set<UUID> visitedSnapshotIds = new HashSet<>();
    Stack<Pair<UUID, SnapshotVersionsMeta>> stack = new Stack<>();
    stack.push(Pair.of(snapshotLocalData.getSnapshotId(), new SnapshotVersionsMeta(snapshotLocalData)));
    while (!stack.isEmpty()) {
      Pair<UUID, SnapshotVersionsMeta> versionNodeToProcess = stack.peek();
      UUID snapId = versionNodeToProcess.getLeft();
      SnapshotVersionsMeta snapshotVersionsMeta = versionNodeToProcess.getRight();
      if (visitedSnapshotIds.contains(snapId)) {
        addSnapshotVersionMeta(snapId, snapshotVersionsMeta);
        stack.pop();
      } else {
        UUID prevSnapId = snapshotVersionsMeta.getPreviousSnapshotId();
        if (prevSnapId != null && !versionNodeMap.containsKey(prevSnapId)) {
          File previousSnapshotLocalDataFile = new File(getSnapshotLocalPropertyYamlPath(prevSnapId));
          OmSnapshotLocalData prevSnapshotLocalData = snapshotLocalDataSerializer.load(previousSnapshotLocalDataFile);
          if (!prevSnapId.equals(prevSnapshotLocalData.getSnapshotId())) {
            throw new IOException("SnapshotId mismatch: expected " + prevSnapId +
                " but found " + prevSnapshotLocalData.getSnapshotId() +
                " in file " + previousSnapshotLocalDataFile.getAbsolutePath());
          }
          stack.push(Pair.of(prevSnapshotLocalData.getSnapshotId(), new SnapshotVersionsMeta(prevSnapshotLocalData)));
        }
        visitedSnapshotIds.add(snapId);
      }
    }
  }

  private void increamentOrphanCheckCount(UUID snapshotId) {
    if (snapshotId != null) {
      this.snapshotToBeCheckedForOrphans.compute(snapshotId, (k, v) -> v == null ? 1 : (v + 1));
    }
  }

  private void decreamentOrphanCheckCount(UUID snapshotId, int decrementBy) {
    this.snapshotToBeCheckedForOrphans.compute(snapshotId, (k, v) -> {
      if (v == null) {
        return null;
      }
      int newValue = v - decrementBy;
      if (newValue <= 0) {
        return null;
      }
      return newValue;
    });
  }

  Map<UUID, Integer> getSnapshotToBeCheckedForOrphans() {
    return snapshotToBeCheckedForOrphans;
  }

  private void init(OzoneConfiguration configuration, SnapshotChainManager chainManager) throws IOException {
    this.locks = omMetadataManager.getHierarchicalLockManager();
    this.snapshotToBeCheckedForOrphans = new ConcurrentHashMap<>();
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    File snapshotDir = new File(store.getSnapshotsParentDir());
    File[] localDataFiles = snapshotDir.listFiles(
        (dir, name) -> name.startsWith(checkpointPrefix) && name.endsWith(YAML_FILE_EXTENSION));
    if (localDataFiles == null) {
      throw new IOException("Error while listing yaml files inside directory: " + snapshotDir.getAbsolutePath());
    }
    Arrays.sort(localDataFiles, Comparator.comparing(File::getName));
    for (File localDataFile : localDataFiles) {
      OmSnapshotLocalData snapshotLocalData = snapshotLocalDataSerializer.load(localDataFile);
      File file = new File(getSnapshotLocalPropertyYamlPath(snapshotLocalData.getSnapshotId()));
      String expectedPath = file.getAbsolutePath();
      String actualPath = localDataFile.getAbsolutePath();
      if (!expectedPath.equals(actualPath)) {
        throw new IOException("Unexpected path for local data file with snapshotId:" + snapshotLocalData.getSnapshotId()
            + " : " + actualPath + ". " + "Expected: " + expectedPath);
      }
      addVersionNodeWithDependents(snapshotLocalData);
    }
    for (UUID snapshotId : versionNodeMap.keySet()) {
      increamentOrphanCheckCount(snapshotId);
    }
    long snapshotLocalDataManagerServiceInterval = configuration.getTimeDuration(
        OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_INTERVAL, OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    if (snapshotLocalDataManagerServiceInterval > 0) {
      this.scheduler = new Scheduler(LOCAL_DATA_MANAGER_SERVICE_NAME, true, 1);
      this.scheduler.scheduleWithFixedDelay(
          () -> {
            try {
              checkOrphanSnapshotVersions(omMetadataManager, chainManager);
            } catch (IOException e) {
              LOG.error("Exception while checking orphan snapshot versions", e);
            }
          }, snapshotLocalDataManagerServiceInterval, snapshotLocalDataManagerServiceInterval, TimeUnit.MILLISECONDS);
    }

  }

  private void checkOrphanSnapshotVersions(OMMetadataManager metadataManager, SnapshotChainManager chainManager)
      throws IOException {
    for (Map.Entry<UUID, Integer> entry : snapshotToBeCheckedForOrphans.entrySet()) {
      UUID snapshotId = entry.getKey();
      int countBeforeCheck = entry.getValue();
      checkOrphanSnapshotVersions(metadataManager, chainManager, snapshotId);
      decreamentOrphanCheckCount(snapshotId, countBeforeCheck);
    }
  }

  @VisibleForTesting
  void checkOrphanSnapshotVersions(OMMetadataManager metadataManager, SnapshotChainManager chainManager,
      UUID snapshotId) throws IOException {
    try (WritableOmSnapshotLocalDataProvider snapshotLocalDataProvider = new WritableOmSnapshotLocalDataProvider(
        snapshotId)) {
      OmSnapshotLocalData snapshotLocalData = snapshotLocalDataProvider.getSnapshotLocalData();
      boolean isSnapshotPurged = SnapshotUtils.isSnapshotPurged(chainManager, metadataManager, snapshotId);
      for (Map.Entry<Integer, LocalDataVersionNode> integerLocalDataVersionNodeEntry : getVersionNodeMap()
          .get(snapshotId).getSnapshotVersions().entrySet()) {
        LocalDataVersionNode versionEntry = integerLocalDataVersionNodeEntry.getValue();
        // remove the version entry if it is not referenced by any other snapshot version node. For version node 0
        // a newly created snapshot version could point to a version with indegree 0 in such a scenario a version 0
        // node can be only deleted if the snapshot is also purged.
        boolean toRemove = localDataGraph.inDegree(versionEntry) == 0
            && ((versionEntry.getVersion() != 0 && versionEntry.getVersion() != snapshotLocalData.getVersion())
            || isSnapshotPurged);
        if (toRemove) {
          snapshotLocalDataProvider.removeVersion(versionEntry.getVersion());
        }
      }
      snapshotLocalDataProvider.commit();
    }
  }

  /**
   * Acquires a write lock and provides an auto-closeable supplier for specifying details
   * of the lock acquisition. The lock is released when the returned supplier is closed.
   *
   * @return an instance of {@code UncheckedAutoCloseableSupplier<OMLockDetails>} representing
   *         the acquired lock details, where the lock will automatically be released on close.
   */
  public UncheckedAutoCloseableSupplier<OMLockDetails> lock() {
    this.fullLock.writeLock().lock();
    return new UncheckedAutoCloseableSupplier<OMLockDetails>() {
      @Override
      public OMLockDetails get() {
        return OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
      }

      @Override
      public void close() {
        fullLock.writeLock().unlock();
      }
    };
  }

  private void validateVersionRemoval(UUID snapshotId, int version) throws IOException {
    LocalDataVersionNode versionNode = getVersionNode(snapshotId, version);
    if (versionNode != null && localDataGraph.inDegree(versionNode) != 0) {
      Set<LocalDataVersionNode> versionNodes = localDataGraph.predecessors(versionNode);
      throw new IOException(String.format("Cannot remove Snapshot %s with version : %d since it still has " +
          "predecessors : %s", snapshotId, version, versionNodes));
    }
  }

  private void validateVersionAddition(LocalDataVersionNode versionNode) throws IOException {
    LocalDataVersionNode previousVersionNode = getVersionNode(versionNode.previousSnapshotId,
        versionNode.previousSnapshotVersion);
    if (versionNode.previousSnapshotId != null && previousVersionNode == null) {
      throw new IOException("Unable to add " + versionNode + " since previous snapshot with version hasn't been " +
          "loaded");
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      if (snapshotLocalDataSerializer != null) {
        try {
          snapshotLocalDataSerializer.close();
        } catch (IOException e) {
          LOG.error("Failed to close snapshot local data serializer", e);
        }
      }
      if (scheduler != null) {
        scheduler.close();
      }
      closed = true;
    }
  }

  private static final class LockDataProviderInitResult {
    private final OmSnapshotLocalData snapshotLocalData;
    private final HierarchicalResourceLock lock;
    private final HierarchicalResourceLock previousLock;
    private final UUID previousSnapshotId;

    private LockDataProviderInitResult(HierarchicalResourceLock lock, OmSnapshotLocalData snapshotLocalData,
        HierarchicalResourceLock previousLock, UUID previousSnapshotId) {
      this.lock = lock;
      this.snapshotLocalData = snapshotLocalData;
      this.previousLock = previousLock;
      this.previousSnapshotId = previousSnapshotId;
    }

    private HierarchicalResourceLock getLock() {
      return lock;
    }

    private HierarchicalResourceLock getPreviousLock() {
      return previousLock;
    }

    private UUID getPreviousSnapshotId() {
      return previousSnapshotId;
    }

    private OmSnapshotLocalData getSnapshotLocalData() {
      return snapshotLocalData;
    }
  }

  private synchronized void upsertNode(UUID snapshotId, SnapshotVersionsMeta snapshotVersions) throws IOException {
    SnapshotVersionsMeta existingSnapVersions = getVersionNodeMap().remove(snapshotId);
    Map<Integer, LocalDataVersionNode> existingVersions = existingSnapVersions == null ? Collections.emptyMap() :
        existingSnapVersions.getSnapshotVersions();
    Map<Integer, LocalDataVersionNode> newVersions = snapshotVersions.getSnapshotVersions();
    Map<Integer, Set<LocalDataVersionNode>> predecessors = new HashMap<>();
    boolean versionsRemoved = false;
    // Track all predecessors of the existing versions and remove the node from the graph.
    for (Map.Entry<Integer, LocalDataVersionNode> existingVersion : existingVersions.entrySet()) {
      LocalDataVersionNode existingVersionNode = existingVersion.getValue();
      predecessors.put(existingVersion.getKey(), new HashSet<>(localDataGraph.predecessors(existingVersionNode)));
      versionsRemoved = versionsRemoved || !newVersions.containsKey(existingVersion.getKey());
      localDataGraph.removeNode(existingVersionNode);
    }

    // Add the nodes to be added in the graph and map.
    addSnapshotVersionMeta(snapshotId, snapshotVersions);
    // Reconnect all the predecessors for existing nodes.
    for (Map.Entry<Integer, LocalDataVersionNode> entry : newVersions.entrySet()) {
      for (LocalDataVersionNode predecessor : predecessors.getOrDefault(entry.getKey(), Collections.emptySet())) {
        localDataGraph.putEdge(predecessor, entry.getValue());
      }
    }
    if (existingSnapVersions != null) {
      // The previous snapshotId could have become an orphan entry or could have orphan versions.(In case of
      // version removals)
      if (versionsRemoved || !Objects.equals(existingSnapVersions.getPreviousSnapshotId(),
          snapshotVersions.getPreviousSnapshotId())) {
        increamentOrphanCheckCount(existingSnapVersions.getPreviousSnapshotId());
      }
      // If the version is also updated it could mean that there could be some orphan version present within the
      // same snapshot.
      if (existingSnapVersions.getVersion() != snapshotVersions.getVersion()) {
        increamentOrphanCheckCount(snapshotId);
      }
    }
  }

  /**
   * The ReadableOmSnapshotLocalDataProvider class is responsible for managing the
   * access and initialization of local snapshot data in a thread-safe manner.
   * It provides mechanisms to handle snapshot data, retrieve associated previous
   * snapshot data, and manage lock synchronization for safe concurrent operations.
   *
   * This class works with snapshot identifiers and ensures that the appropriate
   * local data for a given snapshot is loaded and accessible. Additionally, it
   * maintains locking mechanisms to ensure thread-safe initialization and access
   * to both the current and previous snapshot local data. The implementation also
   * supports handling errors in the snapshot data initialization process.
   *
   * Key Functionalities:
   * - Initializes and provides access to snapshot local data associated with a
   *   given snapshot identifier.
   * - Resolves and retrieves data for the previous snapshot if applicable.
   * - Ensures safe concurrent read operations using locking mechanisms.
   * - Validates the integrity and consistency of snapshot data during initialization.
   * - Ensures that appropriate locks are released upon closing.
   *
   * Thread-Safety:
   * This class utilizes locks to guarantee thread-safe operations when accessing
   * or modifying snapshot data. State variables relating to snapshot data are
   * properly synchronized to ensure consistency during concurrent operations.
   *
   * Usage Considerations:
   * - Ensure proper handling of exceptions while interacting with this class,
   *   particularly during initialization and cleanup.
   * - Always invoke the {@code close()} method after usage to release acquired locks
   *   and avoid potential deadlocks.
   */
  public class ReadableOmSnapshotLocalDataProvider implements AutoCloseable {

    private final UUID snapshotId;
    private final HierarchicalResourceLock lock;
    private final HierarchicalResourceLock previousLock;
    private final OmSnapshotLocalData snapshotLocalData;
    private OmSnapshotLocalData previousSnapshotLocalData;
    private volatile boolean isPreviousSnapshotLoaded = false;
    private final UUID resolvedPreviousSnapshotId;

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId) throws IOException {
      this(snapshotId, true);
    }

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId, UUID snapIdToResolve) throws IOException {
      this(snapshotId, true, null, snapIdToResolve, true);
    }

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId, boolean readLock) throws IOException {
      this(snapshotId, readLock, null, null, false);
    }

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId, boolean readLock,
        CheckedSupplier<Pair<OmSnapshotLocalData, File>, IOException> snapshotLocalDataSupplier,
        UUID snapshotIdToBeResolved, boolean isSnapshotToBeResolvedNullable) throws IOException {
      this.snapshotId = snapshotId;
      LockDataProviderInitResult result = initialize(readLock, snapshotId, snapshotIdToBeResolved,
          isSnapshotToBeResolvedNullable, snapshotLocalDataSupplier);
      this.snapshotLocalData = result.getSnapshotLocalData();
      this.lock = result.getLock();
      this.previousLock = result.getPreviousLock();
      this.resolvedPreviousSnapshotId = result.getPreviousSnapshotId();
      this.previousSnapshotLocalData = null;
      this.isPreviousSnapshotLoaded = false;
    }

    public OmSnapshotLocalData getSnapshotLocalData() {
      return snapshotLocalData;
    }

    public synchronized OmSnapshotLocalData getPreviousSnapshotLocalData() throws IOException {
      if (!isPreviousSnapshotLoaded) {
        if (resolvedPreviousSnapshotId != null) {
          File previousSnapshotLocalDataFile = new File(getSnapshotLocalPropertyYamlPath(resolvedPreviousSnapshotId));
          this.previousSnapshotLocalData = snapshotLocalDataSerializer.load(previousSnapshotLocalDataFile);
        }
        this.isPreviousSnapshotLoaded = true;
      }
      return previousSnapshotLocalData;
    }

    private HierarchicalResourceLock acquireLock(UUID snapId, boolean readLock) throws IOException {
      HierarchicalResourceLock acquiredLock = readLock ? locks.acquireReadLock(FlatResource.SNAPSHOT_LOCAL_DATA_LOCK,
          snapId.toString()) : locks.acquireWriteLock(FlatResource.SNAPSHOT_LOCAL_DATA_LOCK, snapId.toString());
      if (!acquiredLock.isLockAcquired()) {
        throw new IOException("Unable to acquire lock for snapshotId: " + snapId);
      }
      return acquiredLock;
    }

    /**
     * Intializes the snapshot local data by acquiring the lock on the snapshot and also acquires a read lock on the
     * snapshotId to be resolved by iterating through the chain of previous snapshot ids.
     */
    private LockDataProviderInitResult initialize(
        boolean readLock, UUID snapId, UUID toResolveSnapshotId, boolean isSnapshotToBeResolvedNullable,
        CheckedSupplier<Pair<OmSnapshotLocalData, File>, IOException> snapshotLocalDataSupplier) throws IOException {
      HierarchicalResourceLock snapIdLock = null;
      HierarchicalResourceLock previousReadLockAcquired = null;
      try {
        snapIdLock = acquireLock(snapId, readLock);
        snapshotLocalDataSupplier = snapshotLocalDataSupplier == null ? () -> {
          File snapshotLocalDataFile = new File(getSnapshotLocalPropertyYamlPath(snapId));
          return Pair.of(snapshotLocalDataSerializer.load(snapshotLocalDataFile), snapshotLocalDataFile);
        } : snapshotLocalDataSupplier;
        Pair<OmSnapshotLocalData, File> pair = snapshotLocalDataSupplier.get();
        OmSnapshotLocalData ssLocalData = pair.getKey();
        if (!Objects.equals(ssLocalData.getSnapshotId(), snapId)) {
          String loadPath = pair.getValue() == null ? null : pair.getValue().getAbsolutePath();
          throw new IOException("SnapshotId in path : " + loadPath + " contains snapshotLocalData corresponding " +
              "to snapshotId " + ssLocalData.getSnapshotId() + ". Expected snapshotId " + snapId);
        }
        // Get previous snapshotId and acquire read lock on the id. We need to do this outside the loop instead of a
        // do while loop since the nodes that need be added may not be present in the graph so it may not be possible
        // to iterate through the chain.
        UUID previousSnapshotId = ssLocalData.getPreviousSnapshotId();
        // if flag toResolveSnapshotIdIsNull is true or toResolveSnapshotId is not null, then we resolve snapshot
        // with previous snapshot id as null, which would mean if the snapshot local data is committed the snapshot
        // local data would become first snapshot in the chain with no previous snapshot id.
        toResolveSnapshotId = (isSnapshotToBeResolvedNullable || toResolveSnapshotId != null) ? toResolveSnapshotId :
            ssLocalData.getPreviousSnapshotId();
        if (toResolveSnapshotId != null && previousSnapshotId != null) {
          if (!versionNodeMap.containsKey(previousSnapshotId)) {
            throw new IOException(String.format("Operating on snapshot id : %s with previousSnapshotId: %s invalid " +
                "since previousSnapshotId is not loaded.", snapId, previousSnapshotId));
          }
          previousReadLockAcquired = acquireLock(previousSnapshotId, true);
          // Create a copy of the previous versionMap to get the previous versions corresponding to the previous
          // snapshot. This map would mutated to resolve the previous snapshot's version corresponding to the
          // toResolveSnapshotId by iterating through the chain of previous snapshot ids.
          Map<Integer, LocalDataVersionNode> previousVersionNodeMap =
              new HashMap<>(versionNodeMap.get(previousSnapshotId).getSnapshotVersions());
          UUID currentIteratedSnapshotId = previousSnapshotId;
          // Iterate through the chain of previous snapshot ids until the snapshot id to be resolved is found.
          while (!Objects.equals(currentIteratedSnapshotId, toResolveSnapshotId)) {
            // All versions for the snapshot should point to the same previous snapshot id. Otherwise this is a sign
            // of corruption.
            Set<UUID> previousIds =
                previousVersionNodeMap.values().stream().map(LocalDataVersionNode::getPreviousSnapshotId)
                .collect(Collectors.toSet());
            if (previousIds.size() > 1) {
              throw new IOException(String.format("Snapshot %s versions has multiple previous snapshotIds %s",
                  currentIteratedSnapshotId, previousIds));
            }
            if (previousIds.isEmpty()) {
              throw new IOException(String.format("Snapshot %s versions doesn't have previous Id thus snapshot " +
                      "%s cannot be resolved against id %s",
                  currentIteratedSnapshotId, snapId, toResolveSnapshotId));
            }
            UUID previousId = previousIds.iterator().next();
            HierarchicalResourceLock previousToPreviousReadLockAcquired = acquireLock(previousId, true);

            try {
              // Get the version node for the snapshot and update the version node to the successor to point to the
              // previous node.
              for (Map.Entry<Integer, LocalDataVersionNode> entry : previousVersionNodeMap.entrySet()) {
                Set<LocalDataVersionNode> versionNode = localDataGraph.successors(entry.getValue());
                if (versionNode.size() > 1) {
                  throw new IOException(String.format("Snapshot %s version %d has multiple successors %s",
                      currentIteratedSnapshotId, entry.getValue(), versionNode));
                }
                if (versionNode.isEmpty()) {
                  throw new IOException(String.format("Snapshot %s version %d doesn't have successor",
                      currentIteratedSnapshotId, entry.getValue()));
                }
                // Set the version node for iterated version to the successor corresponding to the previous snapshot id.
                entry.setValue(versionNode.iterator().next());
              }
            } finally {
              // Release the read lock acquired on the previous snapshot id acquired. Now that the instance
              // is no longer needed we can release the read lock for the snapshot iterated in the previous snapshot.
              // Make previousToPrevious previous for next iteration.
              previousReadLockAcquired.close();
              previousReadLockAcquired = previousToPreviousReadLockAcquired;
              currentIteratedSnapshotId = previousId;
            }
          }
          ssLocalData.setPreviousSnapshotId(toResolveSnapshotId);
          Map<Integer, OmSnapshotLocalData.VersionMeta> versionMetaMap = ssLocalData.getVersionSstFileInfos();
          for (Map.Entry<Integer, OmSnapshotLocalData.VersionMeta> entry : versionMetaMap.entrySet()) {
            OmSnapshotLocalData.VersionMeta versionMeta = entry.getValue();
            // Get the relative version node which corresponds to the toResolveSnapshotId corresponding to the
            // versionMeta which points to a particular version in the previous snapshot
            LocalDataVersionNode relativePreviousVersionNode =
                previousVersionNodeMap.get(versionMeta.getPreviousSnapshotVersion());
            if (relativePreviousVersionNode == null) {
              throw new IOException(String.format("Unable to resolve previous version node for snapshot: %s" +
                  " with version : %d against previous snapshot %s previous version : %d",
                  snapId, entry.getKey(), toResolveSnapshotId, versionMeta.getPreviousSnapshotVersion()));
            }
            // Set the previous snapshot version to the relativePreviousVersionNode which was captured.
            versionMeta.setPreviousSnapshotVersion(relativePreviousVersionNode.getVersion());
          }
        } else {
          toResolveSnapshotId = null;
          ssLocalData.setPreviousSnapshotId(null);
        }
        return new LockDataProviderInitResult(snapIdLock, ssLocalData, previousReadLockAcquired, toResolveSnapshotId);
      } catch (IOException e) {
        // Release all the locks in case of an exception and rethrow the exception.
        if (previousReadLockAcquired != null) {
          previousReadLockAcquired.close();
        }
        if (snapIdLock != null) {
          snapIdLock.close();
        }
        throw e;
      }
    }

    @Override
    public void close() throws IOException {
      if (previousLock != null) {
        previousLock.close();
      }
      if (lock != null) {
        lock.close();
      }
    }
  }

  /**
   * This class represents a writable provider for managing local data of
   * OmSnapshot. It extends the functionality of {@code ReadableOmSnapshotLocalDataProvider}
   * and provides support for write operations, such as committing changes.
   *
   * The writable snapshot data provider interacts with version nodes and
   * facilitates atomic updates to snapshot properties and files.
   *
   * This class is designed to ensure thread-safe operations and uses locks to
   * guarantee consistent state across concurrent activities.
   *
   * The default usage includes creating an instance of this provider with
   * specific snapshot identifiers and optionally handling additional parameters
   * such as data resolution or a supplier for snapshot data.
   */
  public final class WritableOmSnapshotLocalDataProvider extends ReadableOmSnapshotLocalDataProvider {

    private boolean dirty;

    private WritableOmSnapshotLocalDataProvider(UUID snapshotId) throws IOException {
      super(snapshotId, false);
      fullLock.readLock().lock();
    }

    private WritableOmSnapshotLocalDataProvider(UUID snapshotId, UUID snapshotIdToBeResolved) throws IOException {
      super(snapshotId, false, null, snapshotIdToBeResolved, true);
      fullLock.readLock().lock();
    }

    private WritableOmSnapshotLocalDataProvider(UUID snapshotId,
        CheckedSupplier<Pair<OmSnapshotLocalData, File>, IOException> snapshotLocalDataSupplier) throws IOException {
      super(snapshotId, false, snapshotLocalDataSupplier, null, false);
      fullLock.readLock().lock();
    }

    private SnapshotVersionsMeta validateModification(OmSnapshotLocalData snapshotLocalData)
        throws IOException {
      SnapshotVersionsMeta versionsToBeAdded = new SnapshotVersionsMeta(snapshotLocalData);
      SnapshotVersionsMeta existingVersionsMeta = getVersionNodeMap().get(snapshotLocalData.getSnapshotId());
      for (LocalDataVersionNode node : versionsToBeAdded.getSnapshotVersions().values()) {
        validateVersionAddition(node);
      }
      UUID snapshotId = snapshotLocalData.getSnapshotId();
      Map<Integer, LocalDataVersionNode> existingVersions = getVersionNodeMap().containsKey(snapshotId) ?
          getVersionNodeMap().get(snapshotId).getSnapshotVersions() : Collections.emptyMap();
      for (Map.Entry<Integer, LocalDataVersionNode> entry : existingVersions.entrySet()) {
        if (!versionsToBeAdded.getSnapshotVersions().containsKey(entry.getKey())) {
          validateVersionRemoval(snapshotId, entry.getKey());
        }
      }
      // Set Dirty if the snapshot doesn't exist or previousSnapshotId has changed.
      if (existingVersionsMeta == null || !Objects.equals(versionsToBeAdded.getPreviousSnapshotId(),
          existingVersionsMeta.getPreviousSnapshotId())) {
        setDirty();
        // Set the needsDefrag if the new previous snapshotId is different from the existing one or if this is a new
        // snapshot yaml file.
        snapshotLocalData.setNeedsDefrag(true);
      }
      return versionsToBeAdded;
    }

    public void addSnapshotVersion(RDBStore snapshotStore) throws IOException {
      List<LiveFileMetaData> sstFiles = OmSnapshotManager.getSnapshotSSTFileList(snapshotStore);
      OmSnapshotLocalData previousSnapshotLocalData = getPreviousSnapshotLocalData();
      this.getSnapshotLocalData().addVersionSSTFileInfos(sstFiles, previousSnapshotLocalData == null ? 0 :
          previousSnapshotLocalData.getVersion());
      // Set Dirty if a version is added.
      setDirty();
    }

    public void removeVersion(int version) {
      this.getSnapshotLocalData().removeVersionSSTFileInfos(version);
      // Set Dirty if a version is removed.
      setDirty();
    }

    public synchronized void commit() throws IOException {
      // Validate modification and commit the changes.
      SnapshotVersionsMeta localDataVersionNodes = validateModification(super.snapshotLocalData);
      // Need to update the disk state if and only if the dirty bit is set.
      if (isDirty()) {
        String filePath = getSnapshotLocalPropertyYamlPath(super.snapshotId);
        File snapshotLocalDataFile = new File(filePath);
        if (!localDataVersionNodes.getSnapshotVersions().isEmpty()) {
          String tmpFilePath = filePath + ".tmp";
          File tmpFile = new File(tmpFilePath);
          boolean tmpFileExists = tmpFile.exists();
          if (tmpFileExists) {
            tmpFileExists = !tmpFile.delete();
          }
          if (tmpFileExists) {
            throw new IOException("Unable to delete tmp file " + tmpFilePath);
          }
          snapshotLocalDataSerializer.save(new File(tmpFilePath), super.snapshotLocalData);
          Files.move(tmpFile.toPath(), Paths.get(filePath), StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        } else if (snapshotLocalDataFile.exists()) {
          LOG.info("Deleting Yaml file corresponding to snapshotId: {} in path : {}",
              super.snapshotId, snapshotLocalDataFile.getAbsolutePath());
          if (!snapshotLocalDataFile.delete()) {
            throw new IOException("Unable to delete file " + snapshotLocalDataFile.getAbsolutePath());
          }
        }
        upsertNode(super.snapshotId, localDataVersionNodes);
        // Reset dirty bit
        resetDirty();
      }
    }

    private void setDirty() {
      dirty = true;
    }

    private void resetDirty() {
      dirty = false;
    }

    private boolean isDirty() {
      return dirty;
    }

    @Override
    public void close() throws IOException {
      super.close();
      fullLock.readLock().unlock();
    }
  }

  static final class LocalDataVersionNode {
    private final UUID snapshotId;
    private final int version;
    private final UUID previousSnapshotId;
    private final int previousSnapshotVersion;

    private LocalDataVersionNode(UUID snapshotId, int version, UUID previousSnapshotId, int previousSnapshotVersion) {
      this.previousSnapshotId = previousSnapshotId;
      this.previousSnapshotVersion = previousSnapshotVersion;
      this.snapshotId = snapshotId;
      this.version = version;
    }

    private UUID getPreviousSnapshotId() {
      return previousSnapshotId;
    }

    private int getVersion() {
      return version;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof LocalDataVersionNode)) {
        return false;
      }
      LocalDataVersionNode that = (LocalDataVersionNode) o;
      return version == that.version && previousSnapshotVersion == that.previousSnapshotVersion &&
          snapshotId.equals(that.snapshotId) && Objects.equals(previousSnapshotId, that.previousSnapshotId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(snapshotId, version, previousSnapshotId, previousSnapshotVersion);
    }

    @Override
    public String toString() {
      return "LocalDataVersionNode{" +
          "snapshotId=" + snapshotId +
          ", version=" + version +
          ", previousSnapshotId=" + previousSnapshotId +
          ", previousSnapshotVersion=" + previousSnapshotVersion +
          '}';
    }
  }

  static final class SnapshotVersionsMeta {
    private final UUID previousSnapshotId;
    private final Map<Integer, LocalDataVersionNode> snapshotVersions;
    private int version;

    private SnapshotVersionsMeta(OmSnapshotLocalData snapshotLocalData) {
      this.previousSnapshotId = snapshotLocalData.getPreviousSnapshotId();
      this.snapshotVersions = getVersionNodes(snapshotLocalData);
      this.version = snapshotLocalData.getVersion();
    }

    private Map<Integer, LocalDataVersionNode> getVersionNodes(OmSnapshotLocalData snapshotLocalData) {
      UUID snapshotId = snapshotLocalData.getSnapshotId();
      UUID prevSnapshotId = snapshotLocalData.getPreviousSnapshotId();
      Map<Integer, LocalDataVersionNode> versionNodes = new HashMap<>();
      for (Map.Entry<Integer, VersionMeta> entry : snapshotLocalData.getVersionSstFileInfos().entrySet()) {
        versionNodes.put(entry.getKey(), new LocalDataVersionNode(snapshotId, entry.getKey(),
            prevSnapshotId, entry.getValue().getPreviousSnapshotVersion()));
      }
      return versionNodes;
    }

    UUID getPreviousSnapshotId() {
      return previousSnapshotId;
    }

    int getVersion() {
      return version;
    }

    Map<Integer, LocalDataVersionNode> getSnapshotVersions() {
      return snapshotVersions;
    }

    LocalDataVersionNode getVersionNode(int snapshotVersion) {
      return snapshotVersions.get(snapshotVersion);
    }
  }
}
