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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager;

import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless helper functions for the server and client side of OM
 * communication.
 */
public final class OmUtils {
  public static final Logger LOG = LoggerFactory.getLogger(OmUtils.class);
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
  public static final int EPOCH_WHEN_RATIS_NOT_ENABLED = 1;
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
   * @return {service.id -> [{@link InetSocketAddress}]}
   */
  public static Map<String, List<InetSocketAddress>> getOmHAAddressesById(
      ConfigurationSource conf) {
    Map<String, List<InetSocketAddress>> result = new HashMap<>();
    for (String serviceId : conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY)) {
      if (!result.containsKey(serviceId)) {
        result.put(serviceId, new ArrayList<>());
      }
      for (String nodeId : getOMNodeIds(conf, serviceId)) {
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
      return host.get() + ":" + getOmRpcPort(conf, confKey);
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
    return val != null && val.length() > 0;
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
   * Retrieve the port that is used by OM as specified by the confKey.
   * Return default port if port is not specified in the confKey.
   * @param conf configuration
   * @param confKey configuration key to lookup address from
   * @return Port on which OM RPC server will listen on
   */
  public static int getOmRpcPort(ConfigurationSource conf, String confKey) {
    return getPortNumberFromConfigKeys(conf, confKey)
        .orElse(OZONE_OM_PORT_DEFAULT);
  }

  public static int getOmRestPort(ConfigurationSource conf) {
    return getPortNumberFromConfigKeys(conf, OZONE_OM_HTTP_ADDRESS_KEY)
        .orElse(OZONE_OM_HTTP_BIND_PORT_DEFAULT);
  }


  /**
   * Checks if the OM request is read only or not.
   * @param omRequest OMRequest proto
   * @return True if its readOnly, false otherwise.
   */
  public static boolean isReadOnly(
      OzoneManagerProtocolProtos.OMRequest omRequest) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CheckVolumeAccess:
    case InfoVolume:
    case ListVolume:
    case InfoBucket:
    case ListBuckets:
    case LookupKey:
    case ListKeys:
    case ListTrash:
    case ServiceList:
    case ListMultiPartUploadParts:
    case GetFileStatus:
    case LookupFile:
    case ListStatus:
    case GetAcl:
    case DBUpdates:
    case ListMultipartUploads:
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
    case DeleteOpenKeys:
    case RevokeS3Secret:
      return false;
    default:
      LOG.error("CmdType {} is not categorized as readOnly or not.", cmdType);
      return false;
    }
  }

  public static byte[] getMD5Digest(String input) throws IOException {
    try {
      MessageDigest md = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
      return md.digest(input.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("Error creating an instance of MD5 digest.\n" +
          "This could possibly indicate a faulty JRE");
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
   * Get a collection of all omNodeIds for the given omServiceId.
   */
  public static Collection<String> getOMNodeIds(ConfigurationSource conf,
      String omServiceId) {
    String key = ConfUtils.addSuffix(OZONE_OM_NODES_KEY, omServiceId);
    return conf.getTrimmedStringCollection(key);
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
   * If a OM conf is only set with key suffixed with OM Node ID, return the
   * set value.
   * @return if the value is set for key suffixed with OM Node ID, return the
   * value, else return null.
   */
  public static String getConfSuffixedWithOMNodeId(ConfigurationSource conf,
      String confKey, String omServiceID, String omNodeId) {
    String suffixedConfKey = ConfUtils.addKeySuffixes(
        confKey, omServiceID, omNodeId);
    String confValue = conf.getTrimmed(suffixedConfKey);
    if (StringUtils.isNotEmpty(confValue)) {
      return confValue;
    }
    return null;
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
   * @param repeatedOmKeyInfo key details from deletedTable
   * @param trxnLogIndex For Multipart keys, this is the transactionLogIndex
   *                     of the MultipartUploadAbort request which needs to
   *                     be set as the updateID of the partKeyInfos.
   *                     For regular Key deletes, this value should be set to
   *                     the same updaeID as is in keyInfo.
   * @return {@link RepeatedOmKeyInfo}
   */
  public static RepeatedOmKeyInfo prepareKeyForDelete(OmKeyInfo keyInfo,
      RepeatedOmKeyInfo repeatedOmKeyInfo, long trxnLogIndex,
      boolean isRatisEnabled) {
    // If this key is in a GDPR enforced bucket, then before moving
    // KeyInfo to deletedTable, remove the GDPR related metadata and
    // FileEncryptionInfo from KeyInfo.
    if(Boolean.valueOf(keyInfo.getMetadata().get(OzoneConsts.GDPR_FLAG))) {
      keyInfo.getMetadata().remove(OzoneConsts.GDPR_FLAG);
      keyInfo.getMetadata().remove(OzoneConsts.GDPR_ALGORITHM);
      keyInfo.getMetadata().remove(OzoneConsts.GDPR_SECRET);
      keyInfo.clearFileEncryptionInfo();
    }

    // Set the updateID
    keyInfo.setUpdateID(trxnLogIndex, isRatisEnabled);

    if(repeatedOmKeyInfo == null) {
      //The key doesn't exist in deletedTable, so create a new instance.
      repeatedOmKeyInfo = new RepeatedOmKeyInfo(keyInfo);
    } else {
      //The key exists in deletedTable, so update existing instance.
      repeatedOmKeyInfo.addOmKeyInfo(keyInfo);
    }

    return repeatedOmKeyInfo;
  }

  /**
   * Verify volume name is a valid DNS name.
   */
  public static void validateVolumeName(String volumeName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(volumeName);
    } catch (IllegalArgumentException e) {
      throw new OMException("Invalid volume name: " + volumeName,
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }
  }

  /**
   * Verify bucket name is a valid DNS name.
   */
  public static void validateBucketName(String bucketName)
      throws OMException {
    try {
      HddsClientUtils.verifyResourceName(bucketName);
    } catch (IllegalArgumentException e) {
      throw new OMException("Invalid bucket name: " + bucketName,
          OMException.ResultCodes.INVALID_BUCKET_NAME);
    }
  }

  /**
   * Return OM Client Rpc Time out.
   */
  public static long getOMClientRpcTimeOut(ConfigurationSource configuration) {
    return configuration.getObject(OMClientConfig.class).getRpcTimeOut();
  }

  /**
   * Return OmKeyInfo that would be recovered.
   */
  public static OmKeyInfo prepareKeyForRecover(OmKeyInfo keyInfo,
      RepeatedOmKeyInfo repeatedOmKeyInfo) {

    /* TODO: HDDS-2425. HDDS-2426.*/
    if (repeatedOmKeyInfo.getOmKeyInfoList().contains(keyInfo)) {
      return keyInfo;
    } else {
      return null;
    }
  }

  public static int getOMEpoch(boolean isRatisEnabled) {
    return isRatisEnabled ? EPOCH_WHEN_RATIS_ENABLED :
        EPOCH_WHEN_RATIS_NOT_ENABLED;
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
   * Return configured OzoneManager service id based on the following logic.
   * Look at 'ozone.om.internal.service.id' first. If configured, return that.
   * If the above is not configured, look at 'ozone.om.service.ids'.
   * If count(ozone.om.service.ids) == 1, return that id.
   * If count(ozone.om.service.ids) > 1 throw exception
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
   * Unwrap exception to check if it is some kind of access control problem
   * ({@link AccessControlException} or {@link SecretManager.InvalidToken}).
   */
  public static boolean isAccessControlException(Exception ex) {
    if (ex instanceof ServiceException) {
      Throwable t = ex.getCause();
      if (t instanceof RemoteException) {
        t = ((RemoteException) t).unwrapRemoteException();
      }
      while (t != null) {
        if (t instanceof AccessControlException ||
            t instanceof SecretManager.InvalidToken) {
          return true;
        }
        t = t.getCause();
      }
    }
    return false;
  }

  /**
   * Normalize the key name. This method used {@link Path} to
   * normalize the key name.
   * @param keyName
   * @param preserveTrailingSlash - if True preserves trailing slash, else
   * does not preserve.
   * @return normalized key name.
   */
  @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
  public static String normalizeKey(String keyName,
      boolean preserveTrailingSlash) {
    // For empty strings do nothing, just return the same.
    // Reason to check here is the Paths method fail with NPE.
    if (!StringUtils.isBlank(keyName)) {
      String normalizedKeyName;
      if (keyName.startsWith(OM_KEY_PREFIX)) {
        normalizedKeyName = new Path(keyName).toUri().getPath();
      } else {
        normalizedKeyName = new Path(OM_KEY_PREFIX + keyName)
            .toUri().getPath();
      }
      if (!keyName.equals(normalizedKeyName)) {
        LOG.debug("Normalized key {} to {} ", keyName,
            normalizedKeyName.substring(1));
      }
      if (preserveTrailingSlash) {
        if (keyName.endsWith("/")) {
          return normalizedKeyName.substring(1) + "/";
        }
      }
      return normalizedKeyName.substring(1);
    }

    return keyName;
  }
}
