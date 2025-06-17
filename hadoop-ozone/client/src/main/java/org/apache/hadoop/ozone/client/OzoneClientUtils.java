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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.client.checksum.BaseFileChecksumHelper;
import org.apache.hadoop.ozone.client.checksum.ChecksumHelperFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared Utilities for Ozone FS and related classes.
 */
public final class OzoneClientUtils {
  static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientUtils.class);

  private OzoneClientUtils() {
    // Not used.
  }

  public static BucketLayout resolveLinkBucketLayout(OzoneBucket bucket,
                                                     ObjectStore objectStore,
                                                     Set<Pair<String,
                                                         String>> visited)
      throws IOException {
    if (bucket.isLink()) {
      if (!visited.add(Pair.of(bucket.getVolumeName(),
          bucket.getName()))) {
        throw new OMException("Detected loop in bucket links. Bucket name: " +
            bucket.getName() + ", Volume name: " + bucket.getVolumeName(),
            DETECTED_LOOP_IN_BUCKET_LINKS);
      }

      OzoneBucket sourceBucket;
      try {
        sourceBucket =
            objectStore.getVolume(bucket.getSourceVolume())
                .getBucket(bucket.getSourceBucket());
      } catch (OMException ex) {
        if (ex.getResult().equals(VOLUME_NOT_FOUND)
            || ex.getResult().equals(BUCKET_NOT_FOUND)) {
          // for orphan link bucket, return layout as link bucket
          bucket.setSourcePathExist(false);
          LOG.error("Source Bucket is not found, its orphan bucket and " +
              "used link bucket {} layout {}", bucket.getName(),
              bucket.getBucketLayout());
          return bucket.getBucketLayout();
        }
        // other case throw exception
        throw ex;
      }

      /** If the source bucket is again a link, we recursively resolve the
       * link bucket.
       *
       * For example:
       * buck-link1 -> buck-link2 -> buck-link3 -> buck-link1 -> buck-src
       * buck-src has the actual BucketLayout that will be used by the links.
       */
      if (sourceBucket.isLink()) {
        return resolveLinkBucketLayout(sourceBucket, objectStore, visited);
      }
    }
    return bucket.getBucketLayout();
  }

  /**
   * This API used to resolve the client side configuration preference for file
   * system layer implementations.
   *
   * @param replication                - replication value passed from FS API.
   * @param clientConfiguredReplConfig - Client side configured replication
   *                                   config.
   * @param bucketReplConfig           - server side bucket default replication
   *                                  config.
   * @param config                     - Ozone configuration object.
   * @return client resolved replication config.
   */
  public static ReplicationConfig resolveClientSideReplicationConfig(
      short replication, ReplicationConfig clientConfiguredReplConfig,
      ReplicationConfig bucketReplConfig, ConfigurationSource config) {
    ReplicationConfig clientDeterminedReplConfig = null;

    boolean isECBucket = bucketReplConfig != null && bucketReplConfig
        .getReplicationType() == HddsProtos.ReplicationType.EC;

    // if bucket replication config configured with EC, we will give high
    // preference to server side bucket defaults.
    // Why we give high prefernce to EC is, there is no way for file system
    // interfaces to pass EC replication. So, if one configures EC at bucket,
    // we consider EC to take preference. in short, keys created from file
    // system under EC bucket will always be EC'd.
    if (isECBucket) {
      // if bucket is EC, don't bother client provided configs, let's pass
      // bucket config.
      clientDeterminedReplConfig = bucketReplConfig;
    } else {
      // Let's validate the client side available replication configs.
      boolean isReplicationInSupportedList =
          (replication == ReplicationFactor.ONE
              .getValue() || replication == ReplicationFactor.THREE.getValue());
      if (isReplicationInSupportedList) {
        if (clientConfiguredReplConfig != null) {
          // Uses the replication(short value) passed from file system API and
          // construct replication config object.
          // In case if client explicitely configured EC in configurations, we
          // always take EC as priority as EC replication can't be expressed in
          // filesystem API.
          clientDeterminedReplConfig = ReplicationConfig
              .adjustReplication(clientConfiguredReplConfig, replication,
                  config);
        } else {
          // In file system layers, replication parameter always passed.
          // so, to respect the API provided replication value, we take RATIS as
          // default type.
          clientDeterminedReplConfig = ReplicationConfig
              .parse(ReplicationType.RATIS, Short.toString(replication),
                  config);
        }
      } else {
        // API passed replication number is not in supported replication list.
        // So, let's use whatever available in client side configured.
        // By default it will be null, so server will use server defaults.
        clientDeterminedReplConfig = clientConfiguredReplConfig;
      }
    }
    return clientDeterminedReplConfig;
  }

  public static ReplicationConfig getClientConfiguredReplicationConfig(
      ConfigurationSource config) {
    String replication = config.get(OZONE_REPLICATION);
    if (replication == null) {
      return null;
    }
    return ReplicationConfig.parse(ReplicationType.valueOf(
        config.get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT)),
        replication, config);
  }

  /**
   * Gets the client side replication config by checking user passed values vs
   * client configured values.
   * @param userPassedType - User provided replication type.
   * @param userPassedReplication - User provided replication.
   * @param clientSideConfig - Client side configuration.
   * @return ReplicationConfig.
   */
  public static ReplicationConfig validateAndGetClientReplicationConfig(
      ReplicationType userPassedType, String userPassedReplication,
      ConfigurationSource clientSideConfig) {
    // Priority 1: User passed replication config values.
    // Priority 2: Client side configured replication config values.
    /* if above two are not available, we should just return null and clients
     can pass null replication config to server. Now server will take the
     decision of finding the replication config( either from bucket defaults
     or server defaults). */
    ReplicationType clientReplicationType = userPassedType;
    String clientReplication = userPassedReplication;
    String clientConfiguredDefaultType =
        clientSideConfig.get(OZONE_REPLICATION_TYPE);
    if (userPassedType == null && clientConfiguredDefaultType != null) {
      clientReplicationType =
          ReplicationType.valueOf(clientConfiguredDefaultType);
    }

    String clientConfiguredDefaultReplication =
        clientSideConfig.get(OZONE_REPLICATION);
    if (userPassedReplication == null
        && clientConfiguredDefaultReplication != null) {
      clientReplication = clientConfiguredDefaultReplication;
    }

    // if clientReplicationType or clientReplication is null, then we just pass
    // replication config as null, so that server will take decision.
    if (clientReplicationType == null || clientReplication == null) {
      return null;
    }
    return ReplicationConfig
        .parse(clientReplicationType, clientReplication, clientSideConfig);
  }

  public static FileChecksum getFileChecksumWithCombineMode(OzoneVolume volume,
      OzoneBucket bucket, String keyName, long length,
      OzoneClientConfig.ChecksumCombineMode combineMode,
      ClientProtocol rpcClient) throws IOException {
    Preconditions.checkArgument(length >= 0);

    if (keyName.isEmpty()) {
      return null;
    }
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volume.getName())
        .setBucketName(bucket.getName()).setKeyName(keyName)
        .setSortDatanodesInPipeline(true)
        .setLatestVersionLocation(true).build();
    OmKeyInfo keyInfo = rpcClient.getOzoneManagerClient().lookupKey(keyArgs);
    BaseFileChecksumHelper helper = ChecksumHelperFactory
        .getChecksumHelper(keyInfo.getReplicationConfig().getReplicationType(),
            volume, bucket, keyName, length, combineMode, rpcClient, keyInfo);
    helper.compute();
    return helper.getFileChecksum();
  }

  public static boolean isKeyErasureCode(OmKeyInfo keyInfo) {
    return keyInfo.getReplicationConfig().getReplicationType() ==
            HddsProtos.ReplicationType.EC;
  }

  public static boolean isKeyErasureCode(BasicOmKeyInfo keyInfo) {
    return keyInfo.getReplicationConfig().getReplicationType() ==
        HddsProtos.ReplicationType.EC;
  }

  public static boolean isKeyEncrypted(OmKeyInfo keyInfo) {
    return !Objects.isNull(keyInfo.getFileEncryptionInfo());
  }

  public static int limitValue(int confValue, String confName, int maxLimit) {
    int limitVal = confValue;
    if (confValue > maxLimit) {
      LOG.warn("{} config value is greater than max value : {}, " +
          "limiting the config value to max value..", confName, maxLimit);
      limitVal = maxLimit;
    }
    // Below logic of limiting min page size as 2 is due to behavior of
    // startKey for getting file status where startKey once reached at
    // leaf/key level, then startKey itself being returned when page size is
    // set as 1 and non-recursive listStatus API at client side will go into
    // infinite loop.
    if (limitVal <= 1) {
      limitVal = 2;
    }
    return limitVal;
  }

  public static void deleteSnapshot(ObjectStore objectStore,
      String snapshot, String volumeName, String bucketName) {
    try {
      objectStore.deleteSnapshot(volumeName,
          bucketName, snapshot);
    } catch (IOException exception) {
      LOG.warn("Failed to delete the temp snapshot with name {} in bucket"
              + " {} and volume {} after snapDiff op. Exception : {}", snapshot,
          bucketName, volumeName,
          exception.getMessage());
    }
  }
}
