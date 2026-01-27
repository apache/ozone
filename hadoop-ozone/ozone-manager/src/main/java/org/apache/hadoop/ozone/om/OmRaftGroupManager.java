package org.apache.hadoop.ozone.om;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ServiceException;
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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

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
  private final ReentrantReadWriteLock.ReadLock bucketRaftGroupReadLock = bucketRaftGroupLock.readLock();
  private final ReentrantReadWriteLock.WriteLock bucketRaftGroupWriteLock = bucketRaftGroupLock.writeLock();

  private final AtomicBoolean bucketRaftGroupAssignmentInProgress = new AtomicBoolean(false);

  private final ConcurrentMap<String, org.apache.hadoop.ozone.client.OzoneClient> omClientCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Object> omClientInitLocks = new ConcurrentHashMap<>();

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
      bucketRaftGroups.putIfAbsent(bucketPath, raftGroupUUID);
      incrRaftGroupUsageCounter(RaftGroupId.valueOf(raftGroupUUID));
    } finally {
      bucketRaftGroupWriteLock.unlock();
    }
  }

  private void initBucketMap() {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator =
            metadataManager.getBucketIterator();
    while (bucketIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = bucketIterator.next();
      OmBucketInfo bucketInfo = entry.getValue().getCacheValue();
      if (bucketInfo != null) {
        UUID raftGroup = bucketInfo.getRaftGroup();
        if (raftGroup != null) {
          String key = metadataManager.getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
          bucketRaftGroups.put(key, raftGroup);
          bucketsPerRaftGroupCounter.compute(raftGroup, (k, v) -> v == null ? 1 : v + 1);
        }
      }
    }
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
    if (existing != null) return RaftGroupId.valueOf(existing);

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

    String omServiceId = OmUtils.getOzoneManagerServiceId(ozoneManager.getConfiguration());
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

    String omServiceId = OmUtils.getOzoneManagerServiceId(ozoneManager.getConfiguration());
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

    String omServiceId = OmUtils.getOzoneManagerServiceId(ozoneManager.getConfiguration());
    OMNodeDetails omNode = OmUtils.getAllOMHAAddresses(ozoneManager.getConfiguration(), omServiceId, true).stream()
        .filter(omNodeDetails -> omNodeDetails.getNodeId().equals(mainRaftGroupLeaderPeerId.toString()))
        .findFirst().get();

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder().setCmdType(OzoneManagerProtocolProtos.Type.BucketRaftGroupAssign)
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
        wait(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
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

  public void removeGroup(RaftGroupId raftGroupId) {
    bucketRaftGroups.entrySet().stream()
            .filter(e -> e.getValue().equals(raftGroupId.getUuid()))
            .map(Map.Entry::getKey)
            .forEach(bucketRaftGroups::remove);
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
