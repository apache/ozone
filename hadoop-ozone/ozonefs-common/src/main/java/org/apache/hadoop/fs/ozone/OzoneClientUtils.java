/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.fs.ozone;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;

/**
 * Shared Utilities for Ozone FS and related classes.
 */
public final class OzoneClientUtils {
  private OzoneClientUtils(){
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

      OzoneBucket sourceBucket =
          objectStore.getVolume(bucket.getSourceVolume())
              .getBucket(bucket.getSourceBucket());

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
      ReplicationConfig bucketReplConfig, OzoneConfiguration config) {
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

  static ReplicationConfig getClientConfiguredReplicationConfig(
      ConfigurationSource config) {
    String replication = config.get(OZONE_REPLICATION);
    if (replication == null) {
      return null;
    }
    return ReplicationConfig.parse(ReplicationType.valueOf(
        config.get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT)),
        replication, config);
  }
}
