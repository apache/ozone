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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a proper raft group id for the provided bucket name.
 */
public class OmRaftGroupManager {
  public static final Logger LOG = LoggerFactory.getLogger(OmRaftGroupManager.class);

  private final int omRaftGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServiceId;
  private final OMMetadataManager metadataManager;
  private final OzoneManager ozoneManager;

  private final ConcurrentMap<String, UUID> bucketRaftGroups = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> bucketsPerRaftGroupCounter = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Object> bucketAssignmentLocks = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock bucketRaftGroupLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.WriteLock bucketRaftGroupWriteLock = bucketRaftGroupLock.writeLock();

  private final AtomicBoolean bucketRaftGroupAssignmentInProgress = new AtomicBoolean(false);

  private final ConcurrentMap<String, OmTransport> omTransportCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Object> omTransportInitLocks = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock raftGroupsReconstructionLock = new ReentrantReadWriteLock();

  private final ExecutorService transportCreationExecutor;

  public OmRaftGroupManager(
      OzoneManager ozoneManager,
      OzoneConfiguration configuration,
      boolean multiRaftEnabled,
      String omServiceId,
      OMMetadataManager metadataManager
  ) {
    omRaftGroupCount = configuration.getPositiveIntOrDefault(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
    this.ozoneManager = ozoneManager;
    this.multiRaftEnabled = multiRaftEnabled;
    this.omServiceId = omServiceId;
    this.metadataManager = metadataManager;

    // Create dedicated executor for client creation to avoid blocking IPC threads
    this.transportCreationExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("OmTransport-Creator-%d")
            .setDaemon(true)
            .build()
    );
  }

  public void reset() {
    bucketRaftGroups.clear();
    bucketsPerRaftGroupCounter.clear();
  }

  public void defineAndGetRaftGroupForBucket(String bucketPath, UUID raftGroupUUID) {
    bucketRaftGroupWriteLock.lock();
    try {
      UUID previous = bucketRaftGroups.putIfAbsent(bucketPath, raftGroupUUID);
      if (previous == null) {
        // Only increment when the bucket is actually newly assigned,
        // making this method idempotent for repeated raft applies.
        incrRaftGroupUsageCounter(RaftGroupId.valueOf(raftGroupUUID));
      }
    } finally {
      bucketRaftGroupWriteLock.unlock();
    }
  }

  /**
   * Restores the in-memory bucket→raft-group map from the persisted bucket
   * table, treating {@code OmBucketInfo.getRaftGroup()} as the source of
   * truth.
   *
   * <p>Why this exists: the in-memory map is otherwise populated only by
   * replaying {@code OMBucketRaftGroupAssignRequest} transactions from the
   * main raft group's log. After log compaction or a snapshot install,
   * those replay events are no longer available, so an OM that restarts
   * (or is rebuilt from a snapshot) would otherwise come up with an empty
   * map and see writes for already-assigned buckets as "unassigned" — which
   * triggers a fresh assignment to a different group, producing the
   * cross-group {@code updateID} divergence we observed.
   *
   * <p>Idempotent — safe to call multiple times. {@code putIfAbsent} inside
   * {@link #defineAndGetRaftGroupForBucket} guarantees we don't double-count
   * usage on retries.
   */
  public void initBucketMap() {
    if (!multiRaftEnabled) {
      return;
    }
    int restored = 0;
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> it =
        metadataManager.getBucketIterator();
    while (it.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = it.next();
      OmBucketInfo bucketInfo = entry.getValue().getCacheValue();
      if (bucketInfo == null) {
        continue;
      }
      UUID raftGroup = bucketInfo.getRaftGroup();
      if (raftGroup == null) {
        continue;
      }
      String bucketKey = metadataManager.getBucketKey(
          bucketInfo.getVolumeName(), bucketInfo.getBucketName());
      defineAndGetRaftGroupForBucket(bucketKey, raftGroup);
      restored++;
    }
    LOG.info("Restored {} bucket→raft-group assignments from bucket table.", restored);
  }

  public void incrRaftGroupUsageCounter(RaftGroupId raftGroupId) {
    bucketsPerRaftGroupCounter
        .compute(raftGroupId.getUuid(), (k, v) -> v == null ? 1 : v + 1);
//    LOG.debug("MULTIRAFT: bucket raft group {} usage count increased", raftGroupId);
  }

  public RaftGroupId getRaftGroupToHandleBucketWriteRequest(String volumeName, String bucketName) {
    if (bucketName == null || !multiRaftEnabled) {
      return RaftGroupId.valueOf(toUuid(omServiceId));
    }
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    return getRaftGroupToHandleBucketWriteRequest(bucketKey);
  }

  /**
   * Non-allocating lookup of the raft group assigned to a bucket. Returns
   * {@code null} when multi-raft is disabled or the bucket has no assignment
   * in the local cache. Reads use this to avoid the side-effect of allocating
   * a raft group from a read path.
   */
  public RaftGroupId lookupRaftGroupForBucket(String volumeName, String bucketName) {
    if (bucketName == null || !multiRaftEnabled) {
      return null;
    }
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    UUID assigned = bucketRaftGroups.get(bucketKey);
    return assigned == null ? null : RaftGroupId.valueOf(assigned);
  }

  private RaftGroupId getRaftGroupToHandleBucketWriteRequest(String bucketPath) {
    if (!bucketRaftGroups.containsKey(bucketPath)) {
      return trySetAndGetRaftGroupToHandleBucketWriteRequest(bucketPath);
    } else {
      UUID desiredRaftgroupUUID = bucketRaftGroups.get(bucketPath);
      return RaftGroupId.valueOf(desiredRaftgroupUUID);
    }
  }

  private RaftGroupId trySetAndGetRaftGroupToHandleBucketWriteRequest(String bucketPath) {
    UUID existing = bucketRaftGroups.get(bucketPath);
    if (existing != null) {
      return RaftGroupId.valueOf(existing);
    }

    Object myLock = new Object();
    Object activeLock = bucketAssignmentLocks.putIfAbsent(bucketPath, myLock);

    if (activeLock != null) {
      synchronized (activeLock) {
        try {
          activeLock.wait(30000);
          UUID assigned = bucketRaftGroups.get(bucketPath);
          if (assigned != null) {
            return RaftGroupId.valueOf(assigned);
          }
          throw new RuntimeException("Timeout waiting for raft group assignment for " + bucketPath);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }

    try {
      awaitBucketRaftGroupsInitialization();
      while (!acquireBucketRaftGroupAssignmentWriteLockByRaft()) {
        Thread.sleep(100);
      }

      // Re-check after RAFT lock acquisition. The lock request went through
      // the main RAFT group, so by the time it is committed, all previous
      // RAFT entries (including any pending BucketRaftGroupAssign for this
      // bucket from another OM) are guaranteed to be applied on this node.
      UUID existingAfterBarrier = bucketRaftGroups.get(bucketPath);
      if (existingAfterBarrier != null) {
        releaseBucketRaftGroupAssignmentWriteLockByRaft();
        return RaftGroupId.valueOf(existingAfterBarrier);
      }

      UUID selected = selectLessLoadedRaftGroup();
      assignRaftGroupToBucket(bucketPath, selected);
      // Wait for the assignment raft entry to be applied locally before
      // releasing the lock.  This ensures the next lock holder (possibly
      // on a different OM) sees the updated bucketsPerRaftGroupCounter
      // and picks a different raft group.  Without this wait, the lock
      // release races ahead of the apply, and concurrent assignments
      // all land on the same group.
      waitForAssignmentApplied(bucketPath);
      releaseBucketRaftGroupAssignmentWriteLockByRaft();

      return RaftGroupId.valueOf(selected);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      synchronized (myLock) {
        bucketAssignmentLocks.remove(bucketPath);
        myLock.notifyAll();
      }
    }
  }

  private void releaseBucketRaftGroupAssignmentWriteLockByRaft() throws IOException {
    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();

    RaftPeerId mainRaftGroupLeaderPeerId = omRatisServer.getServerDivision(omRatisServer.getCurrentRaftGroupId())
        .getInfo().getLeaderId();

    
    OMNodeDetails omNode = OmUtils.getAllOMHAAddresses(ozoneManager.getConfiguration(), omServiceId, true).stream()
        .filter(omNodeDetails -> omNodeDetails.getNodeId().equals(mainRaftGroupLeaderPeerId.toString()))
        .findFirst().get();

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.ReleaseBucketRaftGroupAssignmentWriteLock)
            .setClientId(ClientId.randomId().toString())
            .build();
    getOrCreateOmTransport(omNode.getNodeId()).submitRequest(omRequest, omNode.getNodeId());
  }

  private boolean acquireBucketRaftGroupAssignmentWriteLockByRaft() throws IOException {
    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();

    RaftPeerId mainRaftGroupLeaderPeerId = omRatisServer.getServerDivision(omRatisServer.getCurrentRaftGroupId())
        .getInfo().getLeaderId();

    
    OMNodeDetails omNode = OmUtils.getAllOMHAAddresses(ozoneManager.getConfiguration(), omServiceId, true).stream()
        .filter(omNodeDetails -> omNodeDetails.getNodeId().equals(mainRaftGroupLeaderPeerId.toString()))
        .findFirst().get();

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.AcquireBucketRaftGroupAssignmentWriteLock)
            .setClientId(ClientId.randomId().toString())
            .build();
    OzoneManagerProtocolProtos.OMResponse omResponse = getOrCreateOmTransport(omNode.getNodeId())
        .submitRequest(omRequest, omNode.getNodeId());
    return omResponse.getSuccess();
  }

  private OmTransport getOrCreateOmTransport(String omNodeId) throws IOException {
    OmTransport transport = omTransportCache.get(omNodeId);
    if (transport != null) {
      return transport;
    }

    Object initLock = omTransportInitLocks.computeIfAbsent(omNodeId, k -> new Object());
    synchronized (initLock) {
      // double-check
      transport = omTransportCache.get(omNodeId);
      if (transport != null) {
        return transport;
      }

      try {
        // Submit transport creation to executor to avoid blocking IPC handler thread
        CompletableFuture<OmTransport> futureTransport = CompletableFuture.supplyAsync(() -> {
          try {
            return UserGroupInformation.getLoginUser().doAs(
                (PrivilegedExceptionAction<OmTransport>) () -> {

                  // Configure CA certificates for gRPC TLS if security is enabled
//                  if (securityConfig.isSecurityEnabled() &&
//                      securityConfig.isGrpcTlsEnabled()) {
                    try {
                      // Set CA certificates for GrpcOmTransport
                      if (ozoneManager.getCertificateClient() != null) {
                        GrpcOmTransport.setCaCerts(
                            ozoneManager.getCertificateClient().getTrustChain());
                      }
                    } catch (Exception e) {
                      LOG.warn("Failed to set CA certificates for gRPC transport", e);
                    }
//                  }

                  return new GrpcOmTransport(ozoneManager.getConfiguration(),
                        UserGroupInformation.getLoginUser(),
                        omServiceId);
                }
            );
          } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to create gRPC transport", e);
          }
        }, transportCreationExecutor);

        // Wait for transport creation with timeout
        transport = futureTransport.get(30, java.util.concurrent.TimeUnit.SECONDS);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while creating OmTransport", e);
      } catch (java.util.concurrent.TimeoutException e) {
        throw new IOException("Timeout while creating OmTransport for " + omNodeId);
      } catch (java.util.concurrent.ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        throw new IOException("Failed to create OmTransport for " + omNodeId);
      }

      omTransportCache.put(omNodeId, transport);
      return transport;
    }
  }

  /**
   * Assign raft group to bucket through Ratis consensus.
   * This ensures all OM instances agree on the mapping.
   */
  public void assignRaftGroupToBucket(String bucketPath, UUID raftGroupUUID)
      throws IOException, ServiceException {
    LOG.info("Create request to assign raft group to bucket");
    // Create request proto
    HddsProtos.UUID raftGroupProto = HddsProtos.UUID.newBuilder()
        .setMostSigBits(raftGroupUUID.getMostSignificantBits())
        .setLeastSigBits(raftGroupUUID.getLeastSignificantBits())
        .build();

    OzoneManagerProtocolProtos.BucketRaftGroupAssignRequest.Builder assignRequest =
        OzoneManagerProtocolProtos.BucketRaftGroupAssignRequest.newBuilder()
            .setBucketPath(bucketPath)
            .setRaftGroupId(raftGroupProto);

    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();

    RaftPeerId mainRaftGroupLeaderPeerId = omRatisServer.getServerDivision(omRatisServer.getCurrentRaftGroupId())
        .getInfo().getLeaderId();

    OMNodeDetails omNode = OmUtils.getAllOMHAAddresses(ozoneManager.getConfiguration(), omServiceId, true).stream()
        .filter(omNodeDetails -> omNodeDetails.getNodeId().equals(mainRaftGroupLeaderPeerId.toString()))
        .findFirst().get();

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.BucketRaftGroupAssign)
            .setBucketRaftGroupAssignRequest(assignRequest)
            .setClientId(omRatisServer.getCurrentClientId().toString())
            .build();

    LOG.info("Finish to create request to assign raft group to bucket.");
    OmTransport orCreateOmTransport = getOrCreateOmTransport(omNode.getNodeId());
    LOG.info("gRPC transport created. Send the request through gRPC");
    orCreateOmTransport.submitRequest(omRequest, omNode.getNodeId());
  }

  private void awaitBucketRaftGroupsInitialization() {
    while (bucketsPerRaftGroupCounter.size() < omRaftGroupCount) {
      try {
        LOG.info("Waiting for group initiating {}-{}. {}", bucketsPerRaftGroupCounter.size(), omRaftGroupCount,
            bucketsPerRaftGroupCounter);
        // Plain sleep rather than Object.wait() — there is no synchronized
        // block around this call, and no other code calls notify() on this
        // monitor anyway. Object.wait() without holding the monitor throws
        // IllegalMonitorStateException, which previously surfaced on RPC
        // handler threads when the bucket-group registry hadn't fully
        // initialized at the time of an early write request.
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Wait until the bucket-to-raft-group assignment is applied locally
   * (i.e. visible in {@link #bucketRaftGroups}).  The raft entry was
   * already committed by {@link #assignRaftGroupToBucket}, but the
   * local state machine may not have applied it yet.
   */
  private void waitForAssignmentApplied(String bucketPath) {
    long deadline = System.currentTimeMillis() + 30_000;
    while (!bucketRaftGroups.containsKey(bucketPath)) {
      if (System.currentTimeMillis() >= deadline) {
        LOG.warn("Timed out waiting for raft group assignment"
            + " to be applied for {}", bucketPath);
        return;
      }
      try {
        OzoneManagerRatisServer ratisServer = ozoneManager.getOmRatisServer();
        if (ratisServer != null) {
          ratisServer.getOmStateMachine().awaitDoubleBufferFlush();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private UUID selectLessLoadedRaftGroup() {
    return bucketsPerRaftGroupCounter.entrySet().stream()
        .min(Comparator.comparingInt(Map.Entry::getValue))
        .map(Map.Entry::getKey)
        .get();
  }

  public Map<String, UUID> getBucketRaftGroups() {
    return bucketRaftGroups;
  }

  public int getOmRaftGroupCount() {
    return omRaftGroupCount;
  }

  public static List<RaftGroupId> generateRaftGroups(long currentTerm, int count) {
    List<RaftGroupId> result = new ArrayList<>(count);
    long startFrom = currentTerm * 100;
    for (long i = startFrom; i < startFrom + count; i++) {
      UUID raftGroupIdUUID = OmRaftGroupManager.toUuid(String.valueOf(i));
      RaftGroupId groupId = RaftGroupId.valueOf(raftGroupIdUUID);
      result.add(groupId);
    }
    return result;
  }

  public static UUID toUuid(String groupId) {
    return UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public RaftGroupId addGroupIdToRaftGroupCounter(UUID groupUuid) {
    bucketsPerRaftGroupCounter.put(groupUuid, 0);
    return RaftGroupId.valueOf(groupUuid);
  }

  /**
   * Called from {@code BucketStateMachine.notifyGroupRemove} when Ratis is
   * tearing down this bucket raft group on the local OM.
   *
   * <p>Crucially, this does <strong>not</strong> clear the {@code bucketRaftGroups}
   * map. {@code OmBucketInfo.getRaftGroup()} is the persisted source of truth
   * for which raft group owns each bucket; silently wiping the in-memory map
   * here causes the next write to that bucket to be re-routed to a freshly
   * picked group, which then writes to the shared keyTable with a low
   * per-group log index. Meanwhile the old group's still-pending log entries
   * (replayed after a leader change or follower catch-up) try to write to the
   * same keys with even lower indices, blowing up
   * {@code WithObjectID.validate}'s monotonic-{@code updateID} assertion and
   * terminating the BucketStateMachine.
   *
   * <p>By keeping the assignment, a write to a bucket whose group is genuinely
   * gone fails loudly at the raft routing layer (no leader / group not found)
   * rather than silently corrupting state. Reassignment must be deliberate —
   * via a fresh {@code OMBucketRaftGroupAssignRequest} which overwrites the
   * entry — and ideally drained first.
   *
   * <p>The per-group usage counter <em>is</em> cleared, since it's only used
   * to load-balance future <em>new</em> bucket assignments and a removed
   * group must not be considered.
   */
  public void removeGroup(RaftGroupId raftGroupId) {
    bucketsPerRaftGroupCounter.remove(raftGroupId.getUuid());
  }

  public boolean acquireBucketRaftGroupAssignmentWriteLock() {
    return bucketRaftGroupAssignmentInProgress.compareAndSet(false, true);
  }

  public void releaseBucketRaftGroupAssignmentWriteLock() throws InterruptedException {
    LOG.info("Bucket raft group assignment write lock released");
    bucketRaftGroupAssignmentInProgress.set(false);
  }

  public void acquireBucketRaftGroupsReconstructionLock() {
    raftGroupsReconstructionLock.writeLock().lock();
  }

  public void releaseBucketRaftGroupsReconstructionLock() {
    raftGroupsReconstructionLock.writeLock().unlock();
  }

}
