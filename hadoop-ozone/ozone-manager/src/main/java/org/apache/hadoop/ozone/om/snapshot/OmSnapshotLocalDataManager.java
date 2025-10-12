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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX;
import static org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml.YAML_FILE_EXTENSION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.util.concurrent.Striped;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.util.ObjectSerializer;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Manages local data and metadata associated with Ozone Manager (OM) snapshots,
 * including the creation, storage, and representation of data as YAML files.
 */
public class OmSnapshotLocalDataManager implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OmSnapshotLocalDataManager.class);
  private static final String SNAPSHOT_LOCAL_DATA_LOCK_RESOURCE_NAME = "snapshot_local_data_lock";

  private final ObjectSerializer<OmSnapshotLocalData> snapshotLocalDataSerializer;
  private final MutableGraph<LocalDataVersionNode> localDataGraph;
  private final Map<UUID, Map<Integer, LocalDataVersionNode>> versionNodeMap;
  private final OMMetadataManager omMetadataManager;
  private Striped<ReadWriteLock> locks;

  public OmSnapshotLocalDataManager(OMMetadataManager omMetadataManager,
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
    init(configuration);
  }

  @VisibleForTesting
  Map<UUID, Map<Integer, LocalDataVersionNode>> getVersionNodeMap() {
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
    ReadableOmSnapshotLocalDataProvider snapshotLocalData = new ReadableOmSnapshotLocalDataProvider(snapshotId);
    return snapshotLocalData;
  }

  public ReadableOmSnapshotLocalDataProvider getOmSnapshotLocalData(UUID snapshotId, UUID previousSnapshotID)
      throws IOException {
    ReadableOmSnapshotLocalDataProvider snapshotLocalData = new ReadableOmSnapshotLocalDataProvider(snapshotId,
        previousSnapshotID);
    return snapshotLocalData;
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
    return versionNodeMap.getOrDefault(snapshotId, Collections.emptyMap()).get(version);
  }

  private void addVersionNode(LocalDataVersionNode versionNode) throws IOException {
    if (getVersionNode(versionNode.snapshotId, versionNode.version) == null) {
      validateVersionAddition(versionNode);
      LocalDataVersionNode previousVersionNode = versionNode.previousSnapshotId == null ? null :
          getVersionNode(versionNode.previousSnapshotId, versionNode.previousSnapshotVersion);
      localDataGraph.addNode(versionNode);
      if (previousVersionNode != null) {
        localDataGraph.putEdge(versionNode, previousVersionNode);
      }
      versionNodeMap.computeIfAbsent(versionNode.snapshotId, k -> new HashMap<>())
          .put(versionNode.version, versionNode);
    }
  }

  private Map<Integer, LocalDataVersionNode> getVersionNodes(OmSnapshotLocalData snapshotLocalData) throws IOException {
    UUID snapshotId = snapshotLocalData.getSnapshotId();
    UUID previousSnapshotId = snapshotLocalData.getPreviousSnapshotId();
    return snapshotLocalData.getVersionSstFileInfos().entrySet().stream()
        .map(entry -> new LocalDataVersionNode(snapshotId, entry.getKey(),
            previousSnapshotId, entry.getValue().getPreviousSnapshotVersion()))
        .collect(Collectors.toMap(LocalDataVersionNode::getVersion, Function.identity()));
  }

  public void addVersionNodeWithDependents(OmSnapshotLocalData snapshotLocalData) throws IOException {
    if (versionNodeMap.containsKey(snapshotLocalData.getSnapshotId())) {
      return;
    }
    Set<UUID> visitedSnapshotIds = new HashSet<>();
    Stack<Triple<UUID, UUID, Collection<LocalDataVersionNode>>> stack = new Stack<>();
    stack.push(Triple.of(snapshotLocalData.getSnapshotId(), snapshotLocalData.getPreviousSnapshotId(),
        getVersionNodes(snapshotLocalData).values()));
    while (!stack.isEmpty()) {
      Triple<UUID, UUID, Collection<LocalDataVersionNode>> versionNodeToProcess = stack.peek();
      UUID snapId = versionNodeToProcess.getLeft();
      UUID prevSnapId = versionNodeToProcess.getMiddle();
      Collection<LocalDataVersionNode> versionNodes = versionNodeToProcess.getRight();
      if (visitedSnapshotIds.contains(snapId)) {
        for (LocalDataVersionNode versionNode : versionNodes) {
          addVersionNode(versionNode);
        }
        stack.pop();
      } else {
        if (prevSnapId != null && !versionNodeMap.containsKey(prevSnapId)) {
          File previousSnapshotLocalDataFile = new File(getSnapshotLocalPropertyYamlPath(prevSnapId));
          OmSnapshotLocalData prevSnapshotLocalData = snapshotLocalDataSerializer.load(previousSnapshotLocalDataFile);
          stack.push(Triple.of(prevSnapshotLocalData.getSnapshotId(), prevSnapshotLocalData.getPreviousSnapshotId(),
              getVersionNodes(prevSnapshotLocalData).values()));

        }
        visitedSnapshotIds.add(snapId);
      }
    }
  }

  private void init(OzoneConfiguration configuration) throws IOException {
    boolean fair = configuration.getBoolean(OZONE_MANAGER_FAIR_LOCK, OZONE_MANAGER_FAIR_LOCK_DEFAULT);
    String stripeSizeKey = OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX + SNAPSHOT_LOCAL_DATA_LOCK_RESOURCE_NAME;
    int size = configuration.getInt(stripeSizeKey, OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT);
    this.locks = SimpleStriped.readWriteLock(size, fair);
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

  private Map<Integer, LocalDataVersionNode> validateModification(OmSnapshotLocalData snapshotLocalData)
      throws IOException {
    Map<Integer, LocalDataVersionNode> versionNodes = getVersionNodes(snapshotLocalData);
    for (LocalDataVersionNode node : versionNodes.values()) {
      validateVersionAddition(node);
    }
    Map<Integer, LocalDataVersionNode> snapVersionNodeMap =
        getVersionNodeMap().getOrDefault(snapshotLocalData.getSnapshotId(), Collections.emptyMap());
    for (Map.Entry<Integer, LocalDataVersionNode> entry : snapVersionNodeMap.entrySet()) {
      if (!versionNodes.containsKey(entry.getKey())) {
        validateVersionRemoval(snapshotLocalData.getSnapshotId(), entry.getKey());
      }
    }
    return versionNodes;
  }

  private void upsertNode(UUID snapshotId, Map<Integer, LocalDataVersionNode> versionNodes) throws IOException {
    Map<Integer, LocalDataVersionNode> existingVersions = getVersionNodeMap().getOrDefault(snapshotId,
        Collections.emptyMap());
    getVersionNodeMap().remove(snapshotId);
    for (Map.Entry<Integer, LocalDataVersionNode> entry : versionNodes.entrySet()) {
      addVersionNode(entry.getValue());
      if (existingVersions.containsKey(entry.getKey())) {
        for (LocalDataVersionNode predecessor :
            localDataGraph.predecessors(existingVersions.get(entry.getKey()))) {
          localDataGraph.putEdge(predecessor, entry.getValue());
        }
      }
    }
    for (LocalDataVersionNode localDataVersionNode : existingVersions.values()) {
      localDataGraph.removeNode(localDataVersionNode);
    }
    getVersionNodeMap().put(snapshotId, versionNodes);
  }

  @Override
  public void close() {
    if (snapshotLocalDataSerializer != null) {
      try {
        snapshotLocalDataSerializer.close();
      } catch (IOException e) {
        LOG.error("Failed to close snapshot local data serializer", e);
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
    private final Lock lock;
    private final OmSnapshotLocalData snapshotLocalData;
    private OmSnapshotLocalData previousSnapshotLocalData;
    private volatile boolean isPreviousSnapshotLoaded = false;
    private final UUID resolvedPreviousSnapshotId;

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId) throws IOException {
      this(snapshotId, locks.get(snapshotId).readLock());
    }

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId, UUID snapIdToResolve) throws IOException {
      this(snapshotId, locks.get(snapshotId).readLock(), null, snapIdToResolve);
    }

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId, Lock lock) throws IOException {
      this(snapshotId, lock, null, null);
    }

    protected ReadableOmSnapshotLocalDataProvider(UUID snapshotId, Lock lock,
        CheckedSupplier<Pair<OmSnapshotLocalData, File>, IOException> snapshotLocalDataSupplier,
        UUID snapshotIdToBeResolved) throws IOException {
      this.snapshotId = snapshotId;
      this.lock = lock;
      Pair<OmSnapshotLocalData, UUID> pair = initialize(lock, snapshotId, snapshotIdToBeResolved,
          snapshotLocalDataSupplier);
      this.snapshotLocalData = pair.getKey();
      this.resolvedPreviousSnapshotId = pair.getValue();
      this.previousSnapshotLocalData = null;
      this.isPreviousSnapshotLoaded = false;
    }

    public OmSnapshotLocalData getSnapshotLocalData() {
      return snapshotLocalData;
    }

    public OmSnapshotLocalData getPreviousSnapshotLocalData() throws IOException {
      if (!isPreviousSnapshotLoaded) {
        synchronized (this) {
          if (!isPreviousSnapshotLoaded) {
            File previousSnapshotLocalDataFile = new File(getSnapshotLocalPropertyYamlPath(resolvedPreviousSnapshotId));
            this.previousSnapshotLocalData = resolvedPreviousSnapshotId == null ? null :
                snapshotLocalDataSerializer.load(previousSnapshotLocalDataFile);
            this.isPreviousSnapshotLoaded = true;
          }
        }
      }
      return previousSnapshotLocalData;
    }

    private Pair<OmSnapshotLocalData, UUID> initialize(Lock snapIdLock, UUID snapId, UUID toResolveSnapshotId,
        CheckedSupplier<Pair<OmSnapshotLocalData, File>, IOException> snapshotLocalDataSupplier)
        throws IOException {
      snapIdLock.lock();
      ReadWriteLock lockIdAcquired = locks.get(snapId);
      ReadWriteLock previousReadLockAcquired = null;
      boolean haspreviousReadLockAcquiredAcquired = false;
      try {
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

        UUID previousSnapshotId = ssLocalData.getPreviousSnapshotId();
        if (previousSnapshotId != null) {
          if (versionNodeMap.containsKey(previousSnapshotId)) {
            throw new IOException(String.format("Operating on snapshot id : %s with previousSnapshotId: %s invalid " +
                "since previousSnapshotId is not loaded.", snapId, previousSnapshotId));
          }
          toResolveSnapshotId = toResolveSnapshotId == null ? ssLocalData.getPreviousSnapshotId() :
              toResolveSnapshotId;
          previousReadLockAcquired = locks.get(previousSnapshotId);
          if (lockIdAcquired == previousReadLockAcquired) {
            previousReadLockAcquired = null;
          }
          if (previousReadLockAcquired != null) {
            previousReadLockAcquired.readLock().lock();
            haspreviousReadLockAcquiredAcquired = true;
          }
          Map<Integer, LocalDataVersionNode> previousVersionNodeMap = versionNodeMap.get(previousSnapshotId);
          UUID currentIteratedSnapshotId = previousSnapshotId;
          while (!Objects.equals(currentIteratedSnapshotId, toResolveSnapshotId)) {
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
            ReadWriteLock lockToBeAcquired = locks.get(previousId);
            if (lockToBeAcquired == lockIdAcquired) {
              lockToBeAcquired = null;
            }
            if (lockToBeAcquired != null) {
              if (lockToBeAcquired != previousReadLockAcquired) {
                lockToBeAcquired.readLock().lock();
                haspreviousReadLockAcquiredAcquired =  true;
              } else {
                previousReadLockAcquired = null;
              }
            }
            try {
              for (Map.Entry<Integer, LocalDataVersionNode> entry : previousVersionNodeMap.entrySet()) {
                Set<LocalDataVersionNode> versionNode = localDataGraph.successors(entry.getValue());
                if (versionNode.size() > 1) {
                  throw new IOException(String.format("Snapshot %s version %d has multiple successors %s",
                      currentIteratedSnapshotId, entry.getValue(), versionNode));
                }
                entry.setValue(versionNode.iterator().next());
              }
            } finally {
              if (previousReadLockAcquired != null) {
                previousReadLockAcquired.readLock().unlock();
              }
              previousReadLockAcquired = lockToBeAcquired;
              currentIteratedSnapshotId = previousId;
            }
          }
          ssLocalData.setPreviousSnapshotId(toResolveSnapshotId);
          Map<Integer, OmSnapshotLocalData.VersionMeta> versionMetaMap = ssLocalData.getVersionSstFileInfos();
          for (Map.Entry<Integer, OmSnapshotLocalData.VersionMeta> entry : versionMetaMap.entrySet()) {
            OmSnapshotLocalData.VersionMeta versionMeta = entry.getValue();
            LocalDataVersionNode relativePreviousVersionNode =
                previousVersionNodeMap.get(versionMeta.getPreviousSnapshotVersion());
            if (relativePreviousVersionNode == null) {
              throw new IOException(String.format("Unable to resolve previous version node for snapshot: %s" +
                  " with version : %d against previous snapshot %s previous version : %d",
                  snapId, entry.getKey(), toResolveSnapshotId, versionMeta.getPreviousSnapshotVersion()));
            }
          }
        } else {
          toResolveSnapshotId = null;
        }
        return Pair.of(ssLocalData, toResolveSnapshotId);
      } catch (IOException e) {
        if (previousReadLockAcquired != null && haspreviousReadLockAcquiredAcquired) {
          previousReadLockAcquired.readLock().unlock();
        }
        snapIdLock.unlock();
        throw e;
      }
    }

    @Override
    public void close() {
      if (resolvedPreviousSnapshotId != null) {
        locks.get(resolvedPreviousSnapshotId).readLock().unlock();
      }
      lock.unlock();
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

    private WritableOmSnapshotLocalDataProvider(UUID snapshotId) throws IOException {
      super(snapshotId, locks.get(snapshotId).writeLock());
    }

    private WritableOmSnapshotLocalDataProvider(UUID snapshotId, UUID snapshotIdToBeResolved) throws IOException {
      super(snapshotId, locks.get(snapshotId).writeLock(), null, snapshotIdToBeResolved);
    }

    private WritableOmSnapshotLocalDataProvider(UUID snapshotId,
        CheckedSupplier<Pair<OmSnapshotLocalData, File>, IOException> snapshotLocalDataSupplier) throws IOException {
      super(snapshotId, locks.get(snapshotId).writeLock(), snapshotLocalDataSupplier, null);
    }

    public synchronized void commit() throws IOException {
      Map<Integer, LocalDataVersionNode> localDataVersionNodes = validateModification(super.snapshotLocalData);
      String filePath = getSnapshotLocalPropertyYamlPath(super.snapshotId);
      String tmpFilePath = filePath + ".tmp";
      File tmpFile = new File(tmpFilePath);
      if (tmpFile.exists()) {
        tmpFile.delete();
      }
      snapshotLocalDataSerializer.save(new File(tmpFilePath), super.snapshotLocalData);
      FileUtils.moveFile(tmpFile, new File(filePath), StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      upsertNode(super.snapshotId, localDataVersionNodes);

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

    private int getVersion() {
      return version;
    }

    private UUID getSnapshotId() {
      return snapshotId;
    }

    private UUID getPreviousSnapshotId() {
      return previousSnapshotId;
    }

    private int getPreviousSnapshotVersion() {
      return previousSnapshotVersion;
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
}
