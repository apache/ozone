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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.HddsUtils.getHostName;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.OzoneConsts.DOUBLE_SLASH_OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_LISTENER_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless helper functions for the server and client side of OM
 * communication.
 */
public final class OmUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OmUtils.class);
  private static final SecureRandom SRAND = new SecureRandom();
  private static byte[] randomBytes = new byte[32];

  private static final long TRANSACTION_ID_SHIFT = 8;
  // from the 64 bits of ObjectID (long variable), 2 bits are reserved for
  // epoch and 8 bits for recursive directory creation, if required. This
  // leaves 54 bits for the transaction ID. Also, the last transaction ID is
  // reserved for creating S3G volume on OM start {@link
  // OzoneManager#addS3GVolumeToDB()}.
  public static final long EPOCH_ID_SHIFT = 62; // 64 - 2
  public static final long REVERSE_EPOCH_ID_SHIFT = 2; // 64 - EPOCH_ID_SHIFT
  public static final long MAX_TRXN_ID = (1L << 54) - 2;
  public static final int EPOCH_WHEN_RATIS_ENABLED = 2;

  private OmUtils() {
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getOmAddress(ConfigurationSource conf) {
    return NetUtils.createSocketAddr(getOmRpcAddress(conf));
  }

  /**
   * Return list of OM addresses by service ids - when HA is enabled.
   *
   * @param conf {@link ConfigurationSource}
   * @return {service.id -&gt; [{@link InetSocketAddress}]}
   */
  public static Map<String, List<InetSocketAddress>> getOmHAAddressesById(
      ConfigurationSource conf) {
    Map<String, List<InetSocketAddress>> result = new HashMap<>();
    for (String serviceId : conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY)) {
      result.computeIfAbsent(serviceId, x -> new ArrayList<>());
      for (String nodeId : getActiveOMNodeIds(conf, serviceId)) {
        String rpcAddr = getOmRpcAddress(conf,
            ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, serviceId, nodeId));
        if (rpcAddr != null) {
          result.get(serviceId).add(NetUtils.createSocketAddr(rpcAddr));
        } else {
          LOG.warn("Address undefined for nodeId: {} for service {}", nodeId,
              serviceId);
        }
      }
    }
    return result;
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static String getOmRpcAddress(ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    return host.orElse(OZONE_OM_BIND_HOST_DEFAULT) + ":" +
        getOmRpcPort(conf);
  }

  /**
   * Retrieve the socket address that is used by OM as specified by the confKey.
   * Return null if the specified conf key is not set.
   * @param conf configuration
   * @param confKey configuration key to lookup address from
   * @return Target InetSocketAddress for the OM RPC server.
   */
  public static String getOmRpcAddress(ConfigurationSource conf,
      String confKey) {
    final Optional<String> host = getHostNameFromConfigKeys(conf, confKey);

    if (host.isPresent()) {
      return host.get() + ":" + getPortNumberFromConfigKeys(conf, confKey)
              .orElse(OZONE_OM_PORT_DEFAULT);
    } else {
      // The specified confKey is not set
      return null;
    }
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to OM.
   * @param conf
   * @return Target InetSocketAddress for the OM service endpoint.
   */
  public static InetSocketAddress getOmAddressForClients(
      ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          OZONE_OM_ADDRESS_KEY + " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration for" +
              " details on configuring Ozone.");
    }

    return NetUtils.createSocketAddr(
        host.get() + ":" + getOmRpcPort(conf));
  }

  /**
   * Returns true if OZONE_OM_SERVICE_IDS_KEY is defined and not empty.
   * @param conf Configuration
   * @return true if OZONE_OM_SERVICE_IDS_KEY is defined and not empty;
   * else false.
   */
  public static boolean isServiceIdsDefined(ConfigurationSource conf) {
    String val = conf.get(OZONE_OM_SERVICE_IDS_KEY);
    return val != null && !val.isEmpty();
  }

  /**
   * Returns true if HA for OzoneManager is configured for the given service id.
   * @param conf Configuration
   * @param serviceId OM HA cluster service ID
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isOmHAServiceId(ConfigurationSource conf,
      String serviceId) {
    Collection<String> omServiceIds = conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY);
    return omServiceIds.contains(serviceId);
  }

  public static int getOmRpcPort(ConfigurationSource conf) {
    return getPortNumberFromConfigKeys(conf, OZONE_OM_ADDRESS_KEY)
        .orElse(OZONE_OM_PORT_DEFAULT);
  }

  /**
   * Checks if the OM request is read only or not.
   * @param omRequest OMRequest proto
   * @return True if its readOnly, false otherwise.
   */
  public static boolean isReadOnly(OMRequest omRequest) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CheckVolumeAccess:
    case InfoVolume:
    case ListVolume:
    case InfoBucket:
    case ListBuckets:
    case LookupKey:
    case ListKeys:
    case ListKeysLight:
    case ListTrash:
      // ListTrash is deprecated by HDDS-11251. Keeping this in here
      // As protobuf currently doesn't support deprecating enum fields
      // TODO: Remove once migrated to proto3 and mark fields in proto
      // as deprecated
    case ServiceList:
    case ListOpenFiles:
    case ListMultiPartUploadParts:
    case GetFileStatus:
    case LookupFile:
    case ListStatus:
    case ListStatusLight:
    case GetAcl:
    case DBUpdates:
    case ListMultipartUploads:
    case FinalizeUpgradeProgress:
    case PrepareStatus:
    case GetS3VolumeContext:
    case ListTenant:
    case TenantGetUserInfo:
    case TenantListUser:
    case ListSnapshot:
    case RefetchSecretKey:
    case RangerBGSync:
      // RangerBGSync is a read operation in the sense that it doesn't directly
      // write to OM DB. And therefore it doesn't need a OMClientRequest.
      // Although indirectly the Ranger sync service task could invoke write
      // operation SetRangerServiceVersion.
    case GetKeyInfo:
    case SnapshotDiff:
    case CancelSnapshotDiff:
    case ListSnapshotDiffJobs:
    case TransferLeadership:
    case SetSafeMode:
    case PrintCompactionLogDag:
      // printCompactionLogDag is deprecated by HDDS-12053,
      // keeping it here for compatibility
    case GetSnapshotInfo:
    case GetObjectTagging:
    case GetQuotaRepairStatus:
    case StartQuotaRepair:
    case Prepare:
    case CancelPrepare:
      return true;
    case CreateVolume:
    case SetVolumeProperty:
    case DeleteVolume:
    case CreateBucket:
    case SetBucketProperty:
    case DeleteBucket:
    case CreateKey:
    case RenameKey:
    case RenameKeys:
    case DeleteKey:
    case DeleteKeys:
    case CommitKey:
    case AllocateBlock:
    case InitiateMultiPartUpload:
    case CommitMultiPartUpload:
    case CompleteMultiPartUpload:
    case AbortMultiPartUpload:
    case GetS3Secret:
    case GetDelegationToken:
    case RenewDelegationToken:
    case CancelDelegationToken:
    case CreateDirectory:
    case CreateFile:
    case RemoveAcl:
    case SetAcl:
    case AddAcl:
    case PurgeKeys:
    case RecoverTrash:
      // RecoverTrash is deprecated by HDDS-11251. Keeping this in here
      // As protobuf currently doesn't support deprecating enum fields
      // TODO: Remove once migrated to proto3 and mark fields in proto
      // as deprecated
    case FinalizeUpgrade:
    case DeleteOpenKeys:
    case SetS3Secret:
    case RevokeS3Secret:
    case PurgeDirectories:
    case PurgePaths:
    case CreateTenant:
    case DeleteTenant:
    case TenantAssignUserAccessId:
    case TenantRevokeUserAccessId:
    case TenantAssignAdmin:
    case TenantRevokeAdmin:
    case SetRangerServiceVersion:
    case CreateSnapshot:
    case DeleteSnapshot:
    case RenameSnapshot:
    case SnapshotMoveDeletedKeys:
    case SnapshotMoveTableKeys:
    case SnapshotPurge:
    case RecoverLease:
    case SetTimes:
    case AbortExpiredMultiPartUploads:
    case SetSnapshotProperty:
    case QuotaRepair:
    case PutObjectTagging:
    case DeleteObjectTagging:
    case UnknownCommand:
      return false;
    case EchoRPC:
      return omRequest.getEchoRPCRequest().getReadOnly();
    default:
      LOG.error("CmdType {} is not categorized as readOnly or not.", cmdType);
      return false;
    }
  }

  /**
   * Checks if the OM request should be sent to the follower or leader.
   * <p>
   * Note that this method is not equivalent to {@link OmUtils#isReadOnly(OMRequest)}
   * since there are cases that a "read" requests (ones that do not go through Ratis) requires
   * to be sent to the leader.
   * @param omRequest OMRequest proto
   * @return True if the request should be sent to the follower.
   */
  public static boolean shouldSendToFollower(OMRequest omRequest) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CheckVolumeAccess:
    case InfoVolume:
    case ListVolume:
    case InfoBucket:
    case ListBuckets:
    case LookupKey:
    case ListKeys:
    case ListKeysLight:
    case ListTrash:
      // ListTrash is deprecated by HDDS-11251. Keeping this in here
      // As protobuf currently doesn't support deprecating enum fields
      // TODO: Remove once migrated to proto3 and mark fields in proto
      // as deprecated
    case ListOpenFiles:
    case ListMultiPartUploadParts:
    case GetFileStatus:
    case LookupFile:
    case ListStatus:
    case ListStatusLight:
    case GetAcl:
    case ListMultipartUploads:
    case FinalizeUpgradeProgress:
    case PrepareStatus:
    case GetS3VolumeContext:
    case ListTenant:
    case TenantGetUserInfo:
    case TenantListUser:
    case ListSnapshot:
    case RefetchSecretKey:
    case GetKeyInfo:
    case GetSnapshotInfo:
    case GetObjectTagging:
      return true;
    case CreateVolume:
    case SetVolumeProperty:
    case DeleteVolume:
    case CreateBucket:
    case SetBucketProperty:
    case DeleteBucket:
    case CreateKey:
    case RenameKey:
    case RenameKeys:
    case DeleteKey:
    case DeleteKeys:
    case CommitKey:
    case AllocateBlock:
    case InitiateMultiPartUpload:
    case CommitMultiPartUpload:
    case CompleteMultiPartUpload:
    case AbortMultiPartUpload:
    case GetS3Secret:
    case GetDelegationToken:
    case RenewDelegationToken:
    case CancelDelegationToken:
    case CreateDirectory:
    case CreateFile:
    case RemoveAcl:
    case SetAcl:
    case AddAcl:
    case PurgeKeys:
    case RecoverTrash:
      // RecoverTrash is deprecated by HDDS-11251. Keeping this in here
      // As protobuf currently doesn't support deprecating enum fields
      // TODO: Remove once migrated to proto3 and mark fields in proto
      // as deprecated
    case FinalizeUpgrade:
    case DeleteOpenKeys:
    case SetS3Secret:
    case RevokeS3Secret:
    case PurgeDirectories:
    case PurgePaths:
    case CreateTenant:
    case DeleteTenant:
    case TenantAssignUserAccessId:
    case TenantRevokeUserAccessId:
    case TenantAssignAdmin:
    case TenantRevokeAdmin:
    case SetRangerServiceVersion:
    case CreateSnapshot:
    case DeleteSnapshot:
    case RenameSnapshot:
    case SnapshotMoveDeletedKeys:
    case SnapshotMoveTableKeys:
    case SnapshotPurge:
    case RecoverLease:
    case SetTimes:
    case AbortExpiredMultiPartUploads:
    case SetSnapshotProperty:
    case QuotaRepair:
    case PutObjectTagging:
    case DeleteObjectTagging:
    case ServiceList: // OM leader should have the most up-to-date OM service list info
    case RangerBGSync: // Ranger Background Sync task is only run on leader
    case SnapshotDiff:
    case CancelSnapshotDiff:
    case ListSnapshotDiffJobs:
    case PrintCompactionLogDag:
      // Snapshot diff is a local to a single OM node so we should not send it arbitrarily
      // to any OM nodes
    case TransferLeadership: // Transfer leadership should be initiated by the leader
    case SetSafeMode: // SafeMode should be initiated by the leader
    case StartQuotaRepair:
    case GetQuotaRepairStatus:
      // Quota repair lifecycle request should be initiated by the leader
    case DBUpdates: // We are currently only interested on the leader DB info
    case UnknownCommand:
      return false;
    case EchoRPC:
      return omRequest.getEchoRPCRequest().getReadOnly();
    default:
      LOG.error("CmdType {} is not categorized to be sent to follower.", cmdType);
      return false;
    }
  }

  public static byte[] getSHADigest() throws IOException {
    try {
      SRAND.nextBytes(randomBytes);
      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      return sha.digest(randomBytes);
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("Error creating an instance of SHA-256 digest.\n" +
          "This could possibly indicate a faulty JRE");
    }
  }

  /**
   * Get a collection of all active omNodeIds (excluding decommissioned nodes)
   * for the given omServiceId.
   */
  public static Collection<String> getActiveOMNodeIds(ConfigurationSource conf,
      String omServiceId) {
    String nodeIdsKey = ConfUtils.addSuffix(OZONE_OM_NODES_KEY, omServiceId);
    Collection<String> nodeIds = conf.getTrimmedStringCollection(nodeIdsKey);
    String decommissionNodesKeyWithServiceIdSuffix =
        ConfUtils.addKeySuffixes(OZONE_OM_DECOMMISSIONED_NODES_KEY,
            omServiceId);
    Collection<String> decommissionedNodeIds =
        getDecommissionedNodeIds(conf, decommissionNodesKeyWithServiceIdSuffix);
    nodeIds.removeAll(decommissionedNodeIds);

    return nodeIds;
  }

  /**
   * Returns active OM node IDs that are not listener nodes for the given service
   * ID.
   *
   * @param conf        Configuration source
   * @param omServiceId OM service ID
   * @return Collection of active non-listener node IDs
   */
  public static Collection<String> getActiveNonListenerOMNodeIds(
      ConfigurationSource conf, String omServiceId) {
    Collection<String> nodeIds = getActiveOMNodeIds(conf, omServiceId);
    Collection<String> listenerNodeIds = getListenerOMNodeIds(conf, omServiceId);
    nodeIds.removeAll(listenerNodeIds);
    return nodeIds;
  }

  /**
   * Returns a collection of configured nodeId's that are to be decommissioned.
   * Aggregate results from both config keys - with and without serviceId
   * suffix. If ozone.om.service.ids contains a single service ID, then a config
   * key without suffix defaults to nodeID associated with that serviceID.
   */
  public static Collection<String> getDecommissionedNodeIds(
      ConfigurationSource conf,
      String decommissionedNodesKeyWithServiceIdSuffix) {
    HashSet<String> serviceIds = new HashSet<>(
        conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY));
    HashSet<String> decommissionedNodeIds = new HashSet<>(
        conf.getTrimmedStringCollection(
            decommissionedNodesKeyWithServiceIdSuffix));
    // If only one serviceID is configured, also check property without prefix
    if (decommissionedNodeIds.isEmpty() && serviceIds.size() == 1) {
      decommissionedNodeIds.addAll(
          conf.getTrimmedStringCollection(OZONE_OM_DECOMMISSIONED_NODES_KEY));
    }
    return decommissionedNodeIds;
  }

  /**
   * Get a collection of listener omNodeIds for the given omServiceId.
   */
  public static Collection<String> getListenerOMNodeIds(ConfigurationSource conf,
      String omServiceId) {
    String listenerNodesKey = ConfUtils.addKeySuffixes(
        OZONE_OM_LISTENER_NODES_KEY, omServiceId);
    return conf.getTrimmedStringCollection(
        listenerNodesKey);
  }

  /**
   * Get a collection of all omNodeIds (active and decommissioned) for a
   * gived omServiceId.
   */
  private static Collection<String> getAllOMNodeIds(ConfigurationSource conf,
      String omServiceId) {
    Set<String> nodeIds = new HashSet<>();
    String nodeIdsKey = ConfUtils.addSuffix(OZONE_OM_NODES_KEY, omServiceId);
    String decommNodesKey = ConfUtils.addKeySuffixes(
        OZONE_OM_DECOMMISSIONED_NODES_KEY, omServiceId);

    nodeIds.addAll(conf.getTrimmedStringCollection(nodeIdsKey));
    nodeIds.addAll(conf.getTrimmedStringCollection(decommNodesKey));
    nodeIds.addAll(conf.getTrimmedStringCollection(OZONE_OM_DECOMMISSIONED_NODES_KEY));

    return nodeIds;
  }

  /**
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  public static Collection<String> emptyAsSingletonNull(Collection<String>
      coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }

  /**
   * Returns the http address of peer OM node.
   * @param conf Configuration
   * @param omNodeId peer OM node ID
   * @param omNodeHostAddr peer OM node host address
   * @return http address of peer OM node in the format <hostName>:<port>
   */
  public static String getHttpAddressForOMPeerNode(ConfigurationSource conf,
      String omServiceId, String omNodeId, String omNodeHostAddr) {
    final Optional<String> bindHost = getHostNameFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(
            OZONE_OM_HTTP_BIND_HOST_KEY, omServiceId, omNodeId));

    final OptionalInt addressPort = getPortNumberFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(
            OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId));

    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(
            OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId));

    String hostName = bindHost.orElse(addressHost.orElse(omNodeHostAddr));

    return hostName + ":" + addressPort.orElse(OZONE_OM_HTTP_BIND_PORT_DEFAULT);
  }

  /**
   * Returns the https address of peer OM node.
   * @param conf Configuration
   * @param omNodeId peer OM node ID
   * @param omNodeHostAddr peer OM node host address
   * @return https address of peer OM node in the format <hostName>:<port>
   */
  public static String getHttpsAddressForOMPeerNode(ConfigurationSource conf,
      String omServiceId, String omNodeId, String omNodeHostAddr) {
    final Optional<String> bindHost = getHostNameFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(
            OZONE_OM_HTTPS_BIND_HOST_KEY, omServiceId, omNodeId));

    final OptionalInt addressPort = getPortNumberFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(
            OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId));

    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        ConfUtils.addKeySuffixes(
            OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId));

    String hostName = bindHost.orElse(addressHost.orElse(omNodeHostAddr));

    return hostName + ":" +
        addressPort.orElse(OZONE_OM_HTTPS_BIND_PORT_DEFAULT);
  }

  public static File createOMDir(String dirPath) {
    File dirFile = new File(dirPath);
    if (!dirFile.mkdirs() && !dirFile.exists()) {
      throw new IllegalArgumentException("Unable to create path: " + dirFile);
    }
    return dirFile;
  }

  /**
   * Prepares key info to be moved to deletedTable.
   * 1. It strips GDPR metadata from key info
   * 2. For given object key, if the repeatedOmKeyInfo instance is null, it
   * implies that no entry for the object key exists in deletedTable so we
   * create a new instance to include this key, else we update the existing
   * repeatedOmKeyInfo instance.
   * 3. Set the updateID to the transactionLogIndex.
   * @param keyInfo args supplied by client
   * @param bucketId bucket id
   * @param trxnLogIndex For Multipart keys, this is the transactionLogIndex
   *                     of the MultipartUploadAbort request which needs to
   *                     be set as the updateID of the partKeyInfos.
   *                     For regular Key deletes, this value should be set to
   *                     the same updateID as is in keyInfo.
   * @return {@link RepeatedOmKeyInfo}
   */
  public static RepeatedOmKeyInfo prepareKeyForDelete(long bucketId, OmKeyInfo keyInfo,
      long trxnLogIndex) {
    OmKeyInfo.Builder builder = keyInfo.toBuilder();
    // If this key is in a GDPR enforced bucket, then before moving
    // KeyInfo to deletedTable, remove the GDPR related metadata and
    // FileEncryptionInfo from KeyInfo.
    if (Boolean.parseBoolean(
            keyInfo.getMetadata().get(OzoneConsts.GDPR_FLAG))
    ) {
      builder.metadata().remove(OzoneConsts.GDPR_FLAG);
      builder.metadata().remove(OzoneConsts.GDPR_ALGORITHM);
      builder.metadata().remove(OzoneConsts.GDPR_SECRET);
    
      builder.setFileEncryptionInfo(null);
    }

    // Set the updateID
    builder.setUpdateID(trxnLogIndex);

    //The key doesn't exist in deletedTable, so create a new instance.
    return new RepeatedOmKeyInfo(builder.build(), bucketId);
  }

  /**
   * Verify volume name is a valid DNS name.
   */
  public static void validateVolumeName(String volumeName, boolean isStrictS3)
      throws OMException {
    try {
      HddsClientUtils.verifyResourceName(volumeName, "volume", isStrictS3);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }
  }

  /**
   * Verify bucket name is a valid DNS name.
   */
  public static void validateBucketName(String bucketName, boolean isStrictS3)
      throws OMException {
    try {
      HddsClientUtils.verifyResourceName(bucketName, "bucket", isStrictS3);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_BUCKET_NAME);
    }
  }

  /**
   * Verify bucket layout is a valid.
   *
   * @return The {@link BucketLayout} corresponding to the string.
   * @throws ConfigurationException If the bucket layout is not valid.
   */
  public static BucketLayout validateBucketLayout(String bucketLayoutString) {
    boolean bucketLayoutValid = Arrays.stream(BucketLayout.values())
        .anyMatch(layout -> layout.name().equals(bucketLayoutString));
    if (bucketLayoutValid) {
      return BucketLayout.fromString(bucketLayoutString);
    } else {
      throw new ConfigurationException(bucketLayoutString +
          " is not a valid default bucket layout. Supported values are " +
          Arrays.stream(BucketLayout.values())
              .map(Enum::toString).collect(Collectors.joining(", ")));
    }
  }

  /**
   * Verify snapshot name is a valid DNS name.
   */
  public static void validateSnapshotName(String snapshotName)
      throws OMException {
    // allow null name, for when user wants generated name
    if (snapshotName == null) {
      return;
    }
    try {
      HddsClientUtils.verifyResourceName(snapshotName, "snapshot");
    } catch (IllegalArgumentException e) {
      throw new OMException("Invalid snapshot name: " + snapshotName + "\n" + e.getMessage(),
          OMException.ResultCodes.INVALID_SNAPSHOT_ERROR);
    }
  }

  /**
   * Return OM Client Rpc Time out.
   */
  public static long getOMClientRpcTimeOut(ConfigurationSource configuration) {
    return configuration.getObject(OMClientConfig.class).getRpcTimeOut();
  }

  public static int getOMEpoch() {
    return EPOCH_WHEN_RATIS_ENABLED;
  }

  /**
   * Get the valid base object id given the transaction id.
   * @param epoch a 2 bit epoch number. The 2 most significant bits of the
   *              object will be set to this epoch.
   * @param txId of the transaction. This value cannot exceed 2^54 - 1 as
   *           out of the 64 bits for a long, 2 are reserved for the epoch
   *           and 8 for recursive directory creation.
   * @return base object id allocated against the transaction
   */
  public static long getObjectIdFromTxId(long epoch, long txId) {
    Preconditions.checkArgument(txId <= MAX_TRXN_ID, "TransactionID " +
        "exceeds max limit of " + MAX_TRXN_ID);
    return addEpochToTxId(epoch, txId);
  }

  /**
   * Note - This function should not be called directly. It is directly called
   * only from OzoneManager#addS3GVolumeToDB() which is a one time operation
   * when OM is started first time to add S3G volume. In call other cases,
   * getObjectIdFromTxId() should be called to append epoch to objectID.
   */
  public static long addEpochToTxId(long epoch, long txId) {
    long lsb54 = txId << TRANSACTION_ID_SHIFT;
    long msb2 = epoch << EPOCH_ID_SHIFT;

    return msb2 | lsb54;
  }

  /**
   * Given an objectId, unset the 2 most significant bits to get the
   * corresponding transaction index.
   */
  @VisibleForTesting
  public static long getTxIdFromObjectId(long objectId) {
    return ((Long.MAX_VALUE >> REVERSE_EPOCH_ID_SHIFT) & objectId)
        >> TRANSACTION_ID_SHIFT;
  }

  /**
   * Verify key name is a valid name.
   */
  public static void validateKeyName(String keyName)
          throws OMException {
    try {
      HddsClientUtils.verifyKeyName(keyName);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
              OMException.ResultCodes.INVALID_KEY_NAME);
    }
  }

  /**
   * Verify if key name contains snapshot reserved word.
   * This verification will run even when
   * ozone.om.keyname.character.check.enabled sets to false
   */
  public static void verifyKeyNameWithSnapshotReservedWord(String keyName)
      throws OMException {
    if (keyName != null &&
        keyName.startsWith(OM_SNAPSHOT_INDICATOR)) {
      if (keyName.length() > OM_SNAPSHOT_INDICATOR.length()) {
        if (keyName.substring(OM_SNAPSHOT_INDICATOR.length())
            .startsWith(OM_KEY_PREFIX)) {
          throw new OMException(
              "Cannot create key under path reserved for snapshot: " + OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX,
              OMException.ResultCodes.INVALID_KEY_NAME);
        }
      } else {
        // We checked for startsWith OM_SNAPSHOT_INDICATOR, and the length is
        // the same, so it must be equal OM_SNAPSHOT_INDICATOR.
        throw new OMException("Cannot create key with reserved name: " + OM_SNAPSHOT_INDICATOR,
            OMException.ResultCodes.INVALID_KEY_NAME);
      }
    }
  }

  /**
   * Verify if key name contains snapshot reserved word.
   * This is similar to verifyKeyNameWithSnapshotReservedWord. The only difference is exception message.
   */
  public static void verifyKeyNameWithSnapshotReservedWordForDeletion(String keyName)  throws OMException {
    if (keyName != null &&
        keyName.startsWith(OM_SNAPSHOT_INDICATOR)) {
      if (keyName.length() > OM_SNAPSHOT_INDICATOR.length()) {
        if (keyName.substring(OM_SNAPSHOT_INDICATOR.length())
            .startsWith(OM_KEY_PREFIX)) {
          throw new OMException(
              "Cannot delete key under path reserved for snapshot: " + OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX,
              OMException.ResultCodes.INVALID_KEY_NAME);
        }
      } else {
        // We checked for startsWith OM_SNAPSHOT_INDICATOR, and the length is
        // the same, so it must be equal OM_SNAPSHOT_INDICATOR.
        throw new OMException("Cannot delete key with reserved name: " + OM_SNAPSHOT_INDICATOR,
            OMException.ResultCodes.INVALID_KEY_NAME);
      }
    }
  }

  /**
   * Return configured OzoneManager service id based on the following logic.
   * Look at 'ozone.om.internal.service.id' first. If configured, return that.
   * If the above is not configured, look at 'ozone.om.service.ids'.
   * If count(ozone.om.service.ids) == 1, return that id.
   * If count(ozone.om.service.ids) &gt; 1 throw exception
   * If 'ozone.om.service.ids' is not configured, return null. (Non HA)
   * @param conf configuration
   * @return OM service ID.
   * @throws IOException on error.
   */
  public static String getOzoneManagerServiceId(OzoneConfiguration conf)
      throws IOException {
    String localOMServiceId = conf.get(OZONE_OM_INTERNAL_SERVICE_ID);
    Collection<String> omServiceIds = conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY);
    if (localOMServiceId == null) {
      LOG.info("{} is not defined, falling back to {} to find serviceID for "
              + "OzoneManager if it is HA enabled cluster",
          OZONE_OM_INTERNAL_SERVICE_ID, OZONE_OM_SERVICE_IDS_KEY);
      if (omServiceIds.size() > 1) {
        throw new IOException(String.format(
            "More than 1 OzoneManager ServiceID (%s) " +
                "configured : %s, but %s is not " +
                "configured.", OZONE_OM_SERVICE_IDS_KEY,
            omServiceIds.toString(), OZONE_OM_INTERNAL_SERVICE_ID));
      }
    } else if (!omServiceIds.contains(localOMServiceId)) {
      throw new IOException(String.format(
          "Cannot find the internal service id %s in %s",
          localOMServiceId, omServiceIds.toString()));
    } else {
      omServiceIds = Collections.singletonList(localOMServiceId);
    }

    if (omServiceIds.isEmpty()) {
      LOG.info("No OzoneManager ServiceID configured.");
      return null;
    } else {
      String serviceId = omServiceIds.iterator().next();
      LOG.info("Using OzoneManager ServiceID '{}'.", serviceId);
      return serviceId;
    }
  }

  /**
   * Normalize the key name. This method used {@link Path} to
   * normalize the key name.
   * @param keyName
   * @param preserveTrailingSlash - if True preserves trailing slash, else
   * does not preserve.
   * @return normalized key name.
   */
  public static String normalizeKey(String keyName,
      boolean preserveTrailingSlash) {
    // For empty strings do nothing, just return the same.
    // Reason to check here is the Paths method fail with NPE.
    if (!StringUtils.isBlank(keyName)) {
      String normalizedKeyName;
      if (keyName.startsWith(OM_KEY_PREFIX)) {
        normalizedKeyName = new Path(normalizeLeadingSlashes(keyName)).toUri().getPath();
      } else {
        normalizedKeyName = new Path(OM_KEY_PREFIX + keyName)
            .toUri().getPath();
      }
      if (LOG.isDebugEnabled() && !keyName.equals(normalizedKeyName)) {
        LOG.debug("Normalized key {} to {} ", keyName,
            normalizedKeyName.substring(1));
      }
      if (preserveTrailingSlash && keyName.endsWith(OZONE_URI_DELIMITER)) {
        return normalizedKeyName.substring(1) + OZONE_URI_DELIMITER;
      }
      return normalizedKeyName.substring(1);
    }

    return keyName;
  }

  /**
   * Normalizes paths by replacing multiple leading slashes with a single slash.
   */
  private static String normalizeLeadingSlashes(String keyName) {
    if (keyName.startsWith(DOUBLE_SLASH_OM_KEY_PREFIX)) {
      int index = 0;
      while (index < keyName.length() && keyName.charAt(index) == OM_KEY_PREFIX.charAt(0)) {
        index++;
      }
      return OM_KEY_PREFIX + keyName.substring(index);
    }
    return keyName;
  }

  /**
   * Normalizes a given path up to the bucket level.
   *
   * This method takes a path as input and normalises uptil the bucket level.
   * It handles empty, removes leading slashes, and splits the path into
   * segments. It then extracts the volume and bucket names, forming a
   * normalized path with a single slash. Finally, any remaining segments are
   * joined as the key name, returning the complete standardized path.
   *
   * @param path The path string to be normalized.
   * @return The normalized path string.
   */
  public static String normalizePathUptoBucket(String path) {
    if (path == null || path.isEmpty()) {
      return OM_KEY_PREFIX; // Handle empty path
    }

    // Remove leading slashes
    path = path.replaceAll("^/*", "");

    String[] segments = path.split(OM_KEY_PREFIX, -1);

    String volumeName = segments[0];
    String bucketName = segments.length > 1 ? segments[1] : "";

    // Combine volume and bucket.
    StringBuilder normalizedPath = new StringBuilder(volumeName);
    if (!bucketName.isEmpty()) {
      normalizedPath.append(OM_KEY_PREFIX).append(bucketName);
    }

    // Add remaining segments as the key
    if (segments.length > 2) {
      normalizedPath.append(OM_KEY_PREFIX).append(
          String.join(OM_KEY_PREFIX,
              Arrays.copyOfRange(segments, 2, segments.length)));
    }

    return normalizedPath.toString();
  }

  /**
   * For a given service ID, return list of configured OM hosts.
   * @param conf configuration
   * @param omServiceId service id
   * @return Set of hosts.
   */
  public static Set<String> getOmHostsFromConfig(OzoneConfiguration conf,
                                                 String omServiceId) {
    Collection<String> omNodeIds = OmUtils.getActiveOMNodeIds(conf,
        omServiceId);
    Set<String> omHosts = new HashSet<>();
    for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {
      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omServiceId, nodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
      Optional<String> hostName = getHostName(rpcAddrStr);
      hostName.ifPresent(omHosts::add);
    }
    return omHosts;
  }

  /**
   * Get a list of all OM details (address and ports) from the specified config.
   */
  public static List<OMNodeDetails> getAllOMHAAddresses(OzoneConfiguration conf,
      String omServiceId, boolean includeDecommissionedNodes) {

    List<OMNodeDetails> omNodesList = new ArrayList<>();
    Collection<String> omNodeIds;
    if (includeDecommissionedNodes) {
      omNodeIds = OmUtils.getAllOMNodeIds(conf, omServiceId);
    } else {
      omNodeIds = OmUtils.getActiveOMNodeIds(conf, omServiceId);
    }
    Collection<String> decommissionedNodeIds = getDecommissionedNodeIds(conf,
            ConfUtils.addKeySuffixes(OZONE_OM_DECOMMISSIONED_NODES_KEY,
                    omServiceId));
    Collection<String> listenerNodeIds = conf.getTrimmedStringCollection(
        ConfUtils.addKeySuffixes(OZONE_OM_LISTENER_NODES_KEY,
            omServiceId));
    if (omNodeIds.isEmpty()) {
      // If there are no nodeIds present, return empty list
      return Collections.emptyList();
    }

    for (String nodeId : omNodeIds) {
      try {
        OMNodeDetails omNodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(
            conf, omServiceId, nodeId);
        if (omNodeDetails == null) {
          LOG.error(
              "There is no OM configuration for node ID {} in ozone-site.xml.",
              nodeId);
          continue;
        }
        if (decommissionedNodeIds.contains(omNodeDetails.getNodeId())) {
          omNodeDetails.setDecommissioningState();
        }
        if (listenerNodeIds.contains(omNodeDetails.getNodeId())) {
          omNodeDetails.setRatisListener();
        }
        omNodesList.add(omNodeDetails);
      } catch (IOException e) {
        String omRpcAddressStr = OMNodeDetails.getOMNodeAddressFromConf(conf,
            omServiceId, nodeId);
        LOG.error("OM {} is present in config file but it's address {} could " +
            "not be resolved. Hence, OM {} is not added to list of peer nodes.",
            nodeId, omRpcAddressStr, nodeId);
      }
    }

    return omNodesList;
  }

  /**
   * Return a comma separated list of OM node details
   * (NodeID[HostAddress:RpcPort]).
   */
  public static String getOMAddressListPrintString(List<OMNodeDetails> omList) {
    if (omList.isEmpty()) {
      return null;
    }
    StringBuilder printString = new StringBuilder();
    printString.append("OM");
    if (omList.size() == 1) {
      printString.append(" [");
    } else {
      printString.append("(s) [");
    }
    printString.append(omList.get(0).getOMPrintInfo());
    for (int i = 1; i < omList.size(); i++) {
      printString.append(',')
          .append(omList.get(i).getOMPrintInfo());
    }
    printString.append(']');
    return printString.toString();
  }

  // Key points to entire bucket's snapshot
  public static boolean isBucketSnapshotIndicator(String key) {
    return key.startsWith(OM_SNAPSHOT_INDICATOR) && key.split("/").length == 2;
  }
  
  public static List<List<String>> format(
          List<ServiceInfo> nodes, int port, String leaderId, String leaderReadiness) {
    List<List<String>> omInfoList = new ArrayList<>();
    // Ensuring OM's are printed in correct order
    List<ServiceInfo> omNodes = nodes.stream()
        .filter(node -> node.getNodeType() == HddsProtos.NodeType.OM)
        .sorted(Comparator.comparing(ServiceInfo::getHostname))
        .collect(Collectors.toList());
    for (ServiceInfo info : omNodes) {
      // Printing only the OM's running
      if (info.getNodeType() == HddsProtos.NodeType.OM) {
        String role = info.getOmRoleInfo().getNodeId().equals(leaderId)
                      ? "LEADER" : "FOLLOWER";
        List<String> omInfo = new ArrayList<>();
        omInfo.add(info.getHostname());
        omInfo.add(info.getOmRoleInfo().getNodeId());
        omInfo.add(String.valueOf(port));
        omInfo.add(role);
        omInfo.add(leaderReadiness);
        omInfoList.add(omInfo);
      }
    }
    return omInfoList;
  }

  /**
   * @param omHost
   * @param omPort
   * If the authority in the URI is not one of the service ID's,
   * it is treated as a hostname. Check if this hostname can be resolved
   * and if it's reachable.
   */
  public static void resolveOmHost(String omHost, int omPort)
      throws IOException {
    InetSocketAddress omHostAddress = NetUtils.createSocketAddr(omHost, omPort);
    if (omHostAddress.isUnresolved()) {
      throw new IOException(
          "Cannot resolve OM host " + omHost + " in the URI",
          new UnknownHostException());
    }
    try {
      if (!omHostAddress.getAddress().isReachable(5000)) {
        throw new IOException(
            "OM host " + omHost + " unreachable in the URI");
      }
    } catch (IOException e) {
      LOG.error("Failure in resolving OM host address", e);
      throw e;
    }
  }
}
