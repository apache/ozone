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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
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
            addKeySuffixes(OZONE_OM_ADDRESS_KEY, serviceId, nodeId));
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
    case DeleteKey:
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
   * Add non empty and non null suffix to a key.
   */
  private static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
        "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Concatenate list of suffix strings '.' separated.
   */
  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN.
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }

  /**
   * Match input address to local address.
   * Return true if it matches, false otherwsie.
   */
  public static boolean isAddressLocal(InetSocketAddress addr) {
    return NetUtils.isLocalAddress(addr.getAddress());
  }

  /**
   * Get a collection of all omNodeIds for the given omServiceId.
   */
  public static Collection<String> getOMNodeIds(ConfigurationSource conf,
      String omServiceId) {
    String key = addSuffix(OZONE_OM_NODES_KEY, omServiceId);
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
    String suffixedConfKey = OmUtils.addKeySuffixes(
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
        addKeySuffixes(OZONE_OM_HTTP_BIND_HOST_KEY, omServiceId, omNodeId));

    final OptionalInt addressPort = getPortNumberFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId));

    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId));

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
        addKeySuffixes(OZONE_OM_HTTPS_BIND_HOST_KEY, omServiceId, omNodeId));

    final OptionalInt addressPort = getPortNumberFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId));

    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId));

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
  public static long getOMClientRpcTimeOut(Configuration configuration) {
    return OzoneConfiguration.of(configuration)
        .getObject(OMClientConfig.class).getRpcTimeOut();
  }
}
