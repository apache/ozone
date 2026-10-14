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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_PROGRESS_KEY;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.upgrade.OMNfsObjectIndexBootstrapRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexRequest.Phase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Backfills existing FSO identities with bounded, resumable Ratis batches on the ready leader. */
public class NfsObjectIndexBootstrapService extends BackgroundService {
  private static final Logger LOG = LoggerFactory.getLogger(NfsObjectIndexBootstrapService.class);
  private final OzoneManager ozoneManager;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();
  private Map<String, OmBucketInfo> buckets;

  public NfsObjectIndexBootstrapService(long interval, TimeUnit unit, OzoneManager ozoneManager) {
    super("NfsObjectIndexBootstrapService", interval, unit, 1, interval, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue tasks = new BackgroundTaskQueue();
    tasks.add(new BootstrapTask());
    return tasks;
  }

  private void bootstrapBatch() throws IOException, ServiceException {
    if (!ozoneManager.isLeaderReady()) {
      buckets = null;
      return;
    }
    OMMetadataManager manager = ozoneManager.getMetadataManager();
    if (!manager.isNfsObjectIndexEnabled() || manager.isNfsObjectIndexReady()) {
      buckets = null;
      return;
    }
    String progress = manager.getMetaTable().getSkipCache(NFS_OBJECT_INDEX_PROGRESS_KEY);
    if (!Objects.equals(progress, manager.getMetaTable().get(NFS_OBJECT_INDEX_PROGRESS_KEY))) {
      // Wait for the prior bootstrap batch, including its index rows, to reach RocksDB.
      return;
    }
    BootstrapNfsObjectIndexRequest request;
    if (progress == null) {
      request = BootstrapNfsObjectIndexRequest.newBuilder().setPhase(Phase.DIRECTORIES)
          .setExpectedCursor("").setNextCursor("").setFinished(false).setVolumeID(0).setBucketID(0)
          .setInitialize(true).build();
    } else {
      if (progress.length() < 2 || progress.charAt(1) != ':'
          || (progress.charAt(0) != '1' && progress.charAt(0) != '2')) {
        throw new IOException("Invalid persisted NFS bootstrap progress: " + progress);
      }
      if (buckets == null) {
        buckets = loadBuckets(manager);
      }
      Phase phase = progress.charAt(0) == '1' ? Phase.DIRECTORIES : Phase.FILES;
      String cursor = progress.substring(2);
      request = phase == Phase.DIRECTORIES ? scan(manager.getDirectoryTable(), phase, cursor)
          : scan(manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED), phase, cursor);
    }
    OMRequest command = OMRequest.newBuilder().setCmdType(Type.BootstrapNfsObjectIndex)
        .setClientId(clientId.toString()).setBootstrapNfsObjectIndexRequest(request).build();
    OMResponse response = OzoneManagerRatisUtils.submitRequest(
        ozoneManager, command, clientId, callId.incrementAndGet());
    if (!response.getSuccess() || response.getStatus() != Status.OK) {
      throw new IOException("NFS bootstrap batch rejected: " + response.getStatus() + " " + response.getMessage());
    }
  }

  private Map<String, OmBucketInfo> loadBuckets(OMMetadataManager manager) throws IOException {
    Map<String, OmBucketInfo> result = new HashMap<>();
    // The persisted initialization batch is a barrier behind all pre-finalization mutations.
    // Buckets created afterward already maintain their index; absent/deleted bucket IDs need no backfill.
    try (Table.KeyValueIterator<String, OmBucketInfo> iterator = manager.getBucketTable().iterator()) {
      while (iterator.hasNext()) {
        OmBucketInfo bucket = iterator.next().getValue();
        if (bucket.getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
          OmVolumeArgs volume = manager.getVolumeTable().getSkipCache(manager.getVolumeKey(bucket.getVolumeName()));
          if (volume != null) {
            result.put("/" + volume.getObjectID() + "/" + bucket.getObjectID() + "/", bucket);
          }
        }
      }
    }
    return result;
  }

  private <T extends WithObjectID> BootstrapNfsObjectIndexRequest scan(Table<String, T> table, Phase phase,
      String cursor) throws IOException {
    BootstrapNfsObjectIndexRequest.Builder request = BootstrapNfsObjectIndexRequest.newBuilder().setPhase(phase)
        .setExpectedCursor(cursor).setNextCursor(cursor).setFinished(false).setVolumeID(0).setBucketID(0);
    try (Table.KeyValueIterator<String, T> iterator = table.iterator()) {
      if (!cursor.isEmpty()) {
        Table.KeyValue<String, T> positioned = iterator.seek(cursor);
        if (positioned != null && positioned.getKey().equals(cursor)) {
          iterator.next();
        }
      }
      String prefix = null;
      OmBucketInfo bucket = null;
      int scanned = 0;
      while (iterator.hasNext()) {
        Table.KeyValue<String, T> entry = iterator.next();
        String[] parts = entry.getKey().split("/", 5);
        if (parts.length != 5 || !parts[0].isEmpty() || parts[4].isEmpty() || parts[4].contains("/")) {
          throw new IOException("Invalid canonical FSO key during NFS bootstrap: " + entry.getKey());
        }
        String entryPrefix = "/" + parts[1] + "/" + parts[2] + "/";
        if (prefix == null) {
          prefix = entryPrefix;
          try {
            request.setVolumeID(Long.parseLong(parts[1])).setBucketID(Long.parseLong(parts[2]));
          } catch (NumberFormatException ex) {
            throw new IOException("Invalid canonical FSO IDs during NFS bootstrap", ex);
          }
          bucket = buckets.get(prefix);
          if (bucket != null) {
            request.setVolumeName(bucket.getVolumeName()).setBucketName(bucket.getBucketName());
          }
        } else if (!prefix.equals(entryPrefix)) {
          // The next batch processes this bucket; the cursor remains at the last consumed key.
          return request.build();
        }
        request.setNextCursor(entry.getKey());
        if (bucket != null) {
          request.addObjects(BootstrapNfsObjectIndexRequest.Object.newBuilder().setDbKey(entry.getKey())
              .setObjectID(entry.getValue().getObjectID()));
        }
        if (++scanned == OMNfsObjectIndexBootstrapRequest.MAX_OBJECTS) {
          return request.build();
        }
      }
      return request.setFinished(true).build();
    }
  }

  private class BootstrapTask implements BackgroundTask {
    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      try {
        bootstrapBatch();
      } catch (IOException | ServiceException ex) {
        LOG.warn("NFS object index bootstrap will retry in the next interval", ex);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
