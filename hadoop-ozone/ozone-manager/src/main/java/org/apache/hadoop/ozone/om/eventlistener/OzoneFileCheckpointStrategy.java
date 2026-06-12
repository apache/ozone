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

package org.apache.hadoop.ozone.om.eventlistener;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of NotificationCheckpointStrategy which loads/saves
 * the the last known notification sent by an event notification plugin
 * directly as metadata on the checkpoint bucket itself.
 *
 * This allows another OM to pick up from the appropriate place in the
 * event of a leadership change.
 *
 * NOTE: The current approach is to store the current checkpoint as a
 * bucket metadata property. This has the virtue of requiring only a
 * single request (the first implementation of this used a two-phase
 * CreateKeyRequest / CommitKeyRequest approach with the checkpoint
 * value being stored as a KeyArgs metadata property, but the two-phase
 * nature increased complexity).
 */
public class OzoneFileCheckpointStrategy implements NotificationCheckpointStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(OzoneFileCheckpointStrategy.class);

  public static final String OZONE_OM_PLUGIN_CHECKPOINT_VOLUME = "ozone.om.plugin.kafka.checkpoint.volume";
  public static final String OZONE_OM_PLUGIN_CHECKPOINT_VOLUME_DEFAULT = "notifications";

  public static final String OZONE_OM_PLUGIN_CHECKPOINT_BUCKET = "ozone.om.plugin.kafka.checkpoint.bucket";
  public static final String OZONE_OM_PLUGIN_CHECKPOINT_BUCKET_DEFAULT = "checkpoint";

  public static final String OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL =
      "ozone.om.plugin.kafka.checkpoint.save.interval";
  public static final int OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL_DEFAULT = 100;

  private static final String METDATA_KEY = "notification-checkpoint";

  private final AtomicLong callId = new AtomicLong(0);
  private final ClientId clientId = ClientId.randomId();
  private final OzoneManager ozoneManager;
  private final AtomicLong saveCount = new AtomicLong(0);

  private final String volume;
  private final String bucket;
  private final int saveInterval;

  public OzoneFileCheckpointStrategy(OzoneManager ozoneManager, final IOmMetadataReader omMetadataReader,
      OzoneConfiguration conf) {
    this.ozoneManager = ozoneManager;
    this.volume = conf.get(OZONE_OM_PLUGIN_CHECKPOINT_VOLUME, OZONE_OM_PLUGIN_CHECKPOINT_VOLUME_DEFAULT);
    this.bucket = conf.get(OZONE_OM_PLUGIN_CHECKPOINT_BUCKET, OZONE_OM_PLUGIN_CHECKPOINT_BUCKET_DEFAULT);

    int interval = conf.getInt(OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL,
        OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL_DEFAULT);
    if (interval < 1) {
      LOG.warn("Configured save interval {} is invalid. Defaulting to 100.", interval);
      interval = OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL_DEFAULT;
    }
    this.saveInterval = interval;
  }

  @Override
  public String load() throws IOException {
    try {
      OmBucketInfo bucketInfo = ozoneManager.getBucketInfo(volume, bucket);
      if (bucketInfo != null && bucketInfo.getMetadata() != null) {
        return bucketInfo.getMetadata().get(METDATA_KEY);
      }
    } catch (IOException ex) {
      LOG.info("Error loading notification checkpoint from bucket /{}/{} - {}", volume, bucket, ex.getMessage());
    }
    return null;
  }

  @Override
  public void save(String val) throws IOException {
    if (StringUtils.isBlank(val)) {
      return;
    }
    long previousSaveCount = saveCount.getAndIncrement();
    // Throttle database commits: persist checkpoint based on configured interval to avoid write storms
    if (previousSaveCount == 0 || previousSaveCount % saveInterval == 0) {
      saveImpl(val);
    }
  }

  private void saveImpl(String val) {
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .addMetadata(HddsProtos.KeyValue.newBuilder()
            .setKey(METDATA_KEY)
            .setValue(val)
            .build())
        .build();

    SetBucketPropertyRequest setBucketPropertyRequest = SetBucketPropertyRequest.newBuilder()
        .setBucketArgs(bucketArgs)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.SetBucketProperty)
        .setClientId(clientId.toString())
        .setSetBucketPropertyRequest(setBucketPropertyRequest)
        .setUserInfo(getUserInfo())
        .build();

    submitRequest(omRequest);
    LOG.info("Persisted {} = {} directly as metadata on bucket /{}/{}", METDATA_KEY, val, volume, bucket);
  }

  private UserInfo getUserInfo() {
    UserInfo.Builder userInfo = UserInfo.newBuilder();
    try {
      userInfo.setUserName(UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      LOG.warn("Failed to get current login user name", e);
      userInfo.setUserName("om");
    }

    if (ozoneManager.getOmRpcServerAddr() != null) {
      InetAddress remoteAddress = ozoneManager.getOmRpcServerAddr().getAddress();
      if (remoteAddress != null) {
        userInfo.setHostName(remoteAddress.getHostName());
        userInfo.setRemoteAddress(remoteAddress.getHostAddress());
      }
    }
    return userInfo.build();
  }

  private OMResponse submitRequest(OMRequest omRequest) {
    try {
      return OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, callId.incrementAndGet());
    } catch (ServiceException e) {
      LOG.error("Set bucket metadata " + omRequest.getCmdType() + " request failed. Will retry at next run.", e);
    }
    return null;
  }
}
