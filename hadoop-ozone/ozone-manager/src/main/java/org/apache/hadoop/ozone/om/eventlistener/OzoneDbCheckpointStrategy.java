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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetEventNotificationCheckpointRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of NotificationCheckpointStrategy which loads/saves
 * the last known transaction progress directly in the OM DB's metaTable.
 *
 * This allows lightweight, HA-consistent, and isolated checkpointing
 * without filesystem volumes, buckets, or client-contended locks.
 */
public class OzoneDbCheckpointStrategy implements NotificationCheckpointStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(OzoneDbCheckpointStrategy.class);

  public static final String OZONE_OM_PLUGIN_CHECKPOINT_NAME = "ozone.om.plugin.kafka.checkpoint.name";
  public static final String OZONE_OM_PLUGIN_CHECKPOINT_NAME_DEFAULT = "kafka-completed-ops";

  public static final String OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL =
      "ozone.om.plugin.kafka.checkpoint.save.interval";
  public static final int OZONE_OM_PLUGIN_CHECKPOINT_SAVE_INTERVAL_DEFAULT = 100;

  private final AtomicLong callId = new AtomicLong(0);
  private final ClientId clientId = ClientId.randomId();
  private final OzoneManager ozoneManager;
  private final AtomicLong saveCount = new AtomicLong(0);

  private final String checkpointName;
  private final String dbKey;
  private final int saveInterval;

  public OzoneDbCheckpointStrategy(OzoneManager ozoneManager,
                                   OzoneConfiguration conf) {
    this.ozoneManager = ozoneManager;
    this.checkpointName = conf.get(OZONE_OM_PLUGIN_CHECKPOINT_NAME, OZONE_OM_PLUGIN_CHECKPOINT_NAME_DEFAULT);
    this.dbKey = OzoneConsts.EVENT_NOTIFICATION_CHECKPOINT_PREFIX + checkpointName;

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
    return ozoneManager.getMetadataManager().getMetaTable().get(dbKey);
  }

  @Override
  public void save(String val) throws IOException {
    if (StringUtils.isBlank(val)) {
      return;
    }
    long currentSaveCount = saveCount.get();
    // Throttle database commits: persist checkpoint based on configured interval to avoid write storms
    if (currentSaveCount == 0 || currentSaveCount % saveInterval == 0) {
      try {
        saveImpl(val);
        // Successful save, so increment count
        saveCount.incrementAndGet();
      } catch (IOException e) {
        // If save fails, do not increment saveCount so we retry next time
        throw e;
      }
    } else {
      // If we are throttled, we still increment saveCount to keep track of updates
      saveCount.incrementAndGet();
    }
  }

  @Override
  public void reset() throws IOException {
    try {
      saveImpl("");
      // Reset count so next normal save starts fresh
      saveCount.set(0);
    } catch (IOException e) {
      throw e;
    }
  }

  @Override
  public Long getMinimumCheckpoint() throws IOException {
    Table<String, String> metaTable = ozoneManager.getMetadataManager().getMetaTable();
    long minCheckpoint = Long.MAX_VALUE;
    boolean hasCheckpoint = false;

    try (TableIterator<String, ? extends Table.KeyValue<String, String>> iterator =
             metaTable.iterator()) {
      iterator.seek(OzoneConsts.EVENT_NOTIFICATION_CHECKPOINT_PREFIX);
      while (iterator.hasNext()) {
        Table.KeyValue<String, String> entry = iterator.next();
        String key = entry.getKey();
        if (!key.startsWith(OzoneConsts.EVENT_NOTIFICATION_CHECKPOINT_PREFIX)) {
          break;
        }
        String valStr = entry.getValue();
        if (StringUtils.isNotBlank(valStr)) {
          try {
            long val = Long.parseLong(valStr);
            minCheckpoint = Math.min(minCheckpoint, val);
            hasCheckpoint = true;
          } catch (NumberFormatException e) {
            LOG.warn("Invalid checkpoint value {} found under key {}", valStr, key);
          }
        }
      }
    }
    return hasCheckpoint ? minCheckpoint : null;
  }

  private void saveImpl(String val) throws IOException {
    SetEventNotificationCheckpointRequest setCheckpointRequest = SetEventNotificationCheckpointRequest.newBuilder()
        .setCheckpointKey(checkpointName)
        .setCheckpointValue(val)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.SetEventNotificationCheckpoint)
        .setClientId(clientId.toString())
        .setSetEventNotificationCheckpointRequest(setCheckpointRequest)
        .setUserInfo(getUserInfo())
        .build();

    submitRequest(omRequest);
    LOG.info("Persisted {} = {} directly as metadata inside metaTable under key {}",
        checkpointName, val, dbKey);
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

  private OMResponse submitRequest(OMRequest omRequest) throws IOException {
    try {
      OMResponse response = OzoneManagerRatisUtils.submitRequest(
          ozoneManager, omRequest, clientId, callId.incrementAndGet());
      if (response != null && response.getStatus() != Status.OK) {
        throw new IOException("Failed to persist checkpoint: " + response.getStatus() +
            (StringUtils.isNotBlank(response.getMessage()) ? " - " + response.getMessage() : ""));
      }
      if (response == null) {
        throw new IOException("Failed to persist checkpoint: empty response");
      }
      return response;
    } catch (ServiceException e) {
      LOG.error("Set event notification checkpoint " + omRequest.getCmdType() + " request failed.", e);
      throw new IOException(e);
    }
  }
}
