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
package org.apache.hadoop.ozone.ksm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException.ResultCodes;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.ksm.helpers.OpenKeySession;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BatchOperation;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone
    .OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_KEY_PREALLOCATION_MAXSIZE;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_IN_MB;
import org.apache.hadoop.hdds.protocol
    .proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol
    .proto.HddsProtos.ReplicationFactor;


/**
 * Implementation of keyManager.
 */
public class KeyManagerImpl implements KeyManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyManagerImpl.class);

  /**
   * A SCM block client, used to talk to SCM to allocate block during putKey.
   */
  private final ScmBlockLocationProtocol scmBlockClient;
  private final KSMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final boolean useRatis;
  private final BackgroundService keyDeletingService;
  private final BackgroundService openKeyCleanupService;

  private final long preallocateMax;
  private final Random random;
  private final String ksmId;

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      KSMMetadataManager metadataManager, OzoneConfiguration conf,
      String ksmId) {
    this.scmBlockClient = scmBlockClient;
    this.metadataManager = metadataManager;
    this.scmBlockSize = conf.getLong(OZONE_SCM_BLOCK_SIZE_IN_MB,
        OZONE_SCM_BLOCK_SIZE_DEFAULT) * OzoneConsts.MB;
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    long  blockDeleteInterval = conf.getTimeDuration(
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    long serviceTimeout = conf.getTimeDuration(
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.preallocateMax = conf.getLong(
        OZONE_KEY_PREALLOCATION_MAXSIZE,
        OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT);
    keyDeletingService = new KeyDeletingService(
        scmBlockClient, this, blockDeleteInterval, serviceTimeout, conf);
    int openkeyCheckInterval = conf.getInt(
        OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_SECONDS,
        OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_SECONDS_DEFAULT);
    openKeyCleanupService = new OpenKeyCleanupService(
        scmBlockClient, this, openkeyCheckInterval, serviceTimeout);
    random = new Random();
    this.ksmId = ksmId;
  }

  @VisibleForTesting
  public BackgroundService getOpenKeyCleanupService() {
    return openKeyCleanupService;
  }

  @Override
  public void start() {
    keyDeletingService.start();
    openKeyCleanupService.start();
  }

  @Override
  public void stop() throws IOException {
    keyDeletingService.shutdown();
    openKeyCleanupService.shutdown();
  }

  private void validateBucket(String volumeName, String bucketName)
      throws IOException {
    byte[] volumeKey = metadataManager.getVolumeKey(volumeName);
    byte[] bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

    //Check if the volume exists
    if(metadataManager.get(volumeKey) == null) {
      LOG.error("volume not found: {}", volumeName);
      throw new KSMException("Volume not found",
          KSMException.ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }
    //Check if bucket already exists
    if(metadataManager.get(bucketKey) == null) {
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new KSMException("Bucket not found",
          KSMException.ResultCodes.FAILED_BUCKET_NOT_FOUND);
    }
  }

  @Override
  public KsmKeyLocationInfo allocateBlock(KsmKeyArgs args, int clientID)
      throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    ReplicationFactor factor = args.getFactor();
    ReplicationType type = args.getType();

    // If user does not specify a replication strategy or
    // replication factor, KSM will use defaults.
    if(factor == null) {
      factor = useRatis ? ReplicationFactor.THREE: ReplicationFactor.ONE;
    }

    if(type == null) {
      type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
    }

    try {
      validateBucket(volumeName, bucketName);
      String objectKey = metadataManager.getKeyWithDBPrefix(
          volumeName, bucketName, keyName);
      byte[] openKey = metadataManager.getOpenKeyNameBytes(objectKey, clientID);
      byte[] keyData = metadataManager.get(openKey);
      if (keyData == null) {
        LOG.error("Allocate block for a key not in open status in meta store " +
            objectKey + " with ID " + clientID);
        throw new KSMException("Open Key not found",
            KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      AllocatedBlock allocatedBlock =
          scmBlockClient.allocateBlock(scmBlockSize, type, factor, ksmId);
      KsmKeyInfo keyInfo =
          KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(keyData));
      KsmKeyLocationInfo info = new KsmKeyLocationInfo.Builder()
          .setBlockID(allocatedBlock.getBlockID())
          .setShouldCreateContainer(allocatedBlock.getCreateContainer())
          .setLength(scmBlockSize)
          .setOffset(0)
          .build();
      // current version not committed, so new blocks coming now are added to
      // the same version
      keyInfo.appendNewBlocks(Collections.singletonList(info));
      keyInfo.updateModifcationTime();
      metadataManager.put(openKey, keyInfo.getProtobuf().toByteArray());
      return info;
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public OpenKeySession openKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    ReplicationFactor factor = args.getFactor();
    ReplicationType type = args.getType();

    // If user does not specify a replication strategy or
    // replication factor, KSM will use defaults.
    if(factor == null) {
      factor = useRatis ? ReplicationFactor.THREE: ReplicationFactor.ONE;
    }

    if(type == null) {
      type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
    }

    try {
      validateBucket(volumeName, bucketName);
      long requestedSize = Math.min(preallocateMax, args.getDataSize());
      List<KsmKeyLocationInfo> locations = new ArrayList<>();
      String objectKey = metadataManager.getKeyWithDBPrefix(
          volumeName, bucketName, keyName);
      // requested size is not required but more like a optimization:
      // SCM looks at the requested, if it 0, no block will be allocated at
      // the point, if client needs more blocks, client can always call
      // allocateBlock. But if requested size is not 0, KSM will preallocate
      // some blocks and piggyback to client, to save RPC calls.
      while (requestedSize > 0) {
        long allocateSize = Math.min(scmBlockSize, requestedSize);
        AllocatedBlock allocatedBlock =
            scmBlockClient.allocateBlock(allocateSize, type, factor, ksmId);
        KsmKeyLocationInfo subKeyInfo = new KsmKeyLocationInfo.Builder()
            .setBlockID(allocatedBlock.getBlockID())
            .setShouldCreateContainer(allocatedBlock.getCreateContainer())
            .setLength(allocateSize)
            .setOffset(0)
            .build();
        locations.add(subKeyInfo);
        requestedSize -= allocateSize;
      }
      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets a
      // value, then this value is used, otherwise, we allocate a single block
      // which is the current size, if read by the client.
      long size = args.getDataSize() >= 0 ? args.getDataSize() : scmBlockSize;
      byte[] keyKey = metadataManager.getDBKeyBytes(
          volumeName, bucketName, keyName);
      byte[] value = metadataManager.get(keyKey);
      KsmKeyInfo keyInfo;
      long openVersion;
      if (value != null) {
        // the key already exist, the new blocks will be added as new version
        keyInfo = KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(value));
        // when locations.size = 0, the new version will have identical blocks
        // as its previous version
        openVersion = keyInfo.addNewVersion(locations);
        keyInfo.setDataSize(size + keyInfo.getDataSize());
      } else {
        // the key does not exist, create a new object, the new blocks are the
        // version 0
        long currentTime = Time.now();
        keyInfo = new KsmKeyInfo.Builder()
            .setVolumeName(args.getVolumeName())
            .setBucketName(args.getBucketName())
            .setKeyName(args.getKeyName())
            .setKsmKeyLocationInfos(Collections.singletonList(
                new KsmKeyLocationInfoGroup(0, locations)))
            .setCreationTime(currentTime)
            .setModificationTime(currentTime)
            .setDataSize(size)
            .build();
        openVersion = 0;
      }
      // Generate a random ID which is not already in meta db.
      int id = -1;
      // in general this should finish in a couple times at most. putting some
      // arbitrary large number here to avoid dead loop.
      for (int j = 0; j < 10000; j++) {
        id = random.nextInt();
        byte[] openKey = metadataManager.getOpenKeyNameBytes(objectKey, id);
        if (metadataManager.get(openKey) == null) {
          metadataManager.put(openKey, keyInfo.getProtobuf().toByteArray());
          break;
        }
      }
      if (id == -1) {
        throw new IOException("Failed to find a usable id for " + objectKey);
      }
      LOG.debug("Key {} allocated in volume {} bucket {}",
          keyName, volumeName, bucketName);
      return new OpenKeySession(id, keyInfo, openVersion);
    } catch (KSMException e) {
      throw e;
    } catch (IOException ex) {
      if (!(ex instanceof KSMException)) {
        LOG.error("Key open failed for volume:{} bucket:{} key:{}",
            volumeName, bucketName, keyName, ex);
      }
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_KEY_ALLOCATION);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public void commitKey(KsmKeyArgs args, int clientID) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      validateBucket(volumeName, bucketName);
      String objectKey = metadataManager.getKeyWithDBPrefix(
          volumeName, bucketName, keyName);
      byte[] objectKeyBytes = metadataManager.getDBKeyBytes(volumeName,
          bucketName, keyName);
      byte[] openKey = metadataManager.getOpenKeyNameBytes(objectKey, clientID);
      byte[] openKeyData = metadataManager.get(openKey);
      if (openKeyData == null) {
        throw new KSMException("Commit a key without corresponding entry " +
            DFSUtil.bytes2String(openKey), ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      KsmKeyInfo keyInfo =
          KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(openKeyData));
      keyInfo.setDataSize(args.getDataSize());
      keyInfo.setModificationTime(Time.now());
      BatchOperation batch = new BatchOperation();
      batch.delete(openKey);
      batch.put(objectKeyBytes, keyInfo.getProtobuf().toByteArray());
      metadataManager.writeBatch(batch);
    } catch (KSMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Key commit failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_KEY_ALLOCATION);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public KsmKeyInfo lookupKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      byte[] keyKey = metadataManager.getDBKeyBytes(
          volumeName, bucketName, keyName);
      byte[] value = metadataManager.get(keyKey);
      if (value == null) {
        LOG.debug("volume:{} bucket:{} Key:{} not found",
            volumeName, bucketName, keyName);
        throw new KSMException("Key not found",
            KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      return KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(value));
    } catch (DBException ex) {
      LOG.error("Get key failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public void renameKey(KsmKeyArgs args, String toKeyName) throws IOException {
    Preconditions.checkNotNull(args);
    Preconditions.checkNotNull(toKeyName);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String fromKeyName = args.getKeyName();
    if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}.",
          volumeName, bucketName, fromKeyName, toKeyName);
      throw new KSMException("Key name is empty",
          ResultCodes.FAILED_INVALID_KEY_NAME);
    }

    metadataManager.writeLock().lock();
    try {
      // fromKeyName should exist
      byte[] fromKey = metadataManager.getDBKeyBytes(
          volumeName, bucketName, fromKeyName);
      byte[] fromKeyValue = metadataManager.get(fromKey);
      if (fromKeyValue == null) {
        // TODO: Add support for renaming open key
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} not found.", volumeName, bucketName, fromKeyName,
            toKeyName, fromKeyName);
        throw new KSMException("Key not found",
            KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }

      // toKeyName should not exist
      byte[] toKey =
          metadataManager.getDBKeyBytes(volumeName, bucketName, toKeyName);
      byte[] toKeyValue = metadataManager.get(toKey);
      if (toKeyValue != null) {
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} already exists.", volumeName, bucketName,
            fromKeyName, toKeyName, toKeyName);
        throw new KSMException("Key not found",
            KSMException.ResultCodes.FAILED_KEY_ALREADY_EXISTS);
      }

      if (fromKeyName.equals(toKeyName)) {
        return;
      }

      KsmKeyInfo newKeyInfo =
          KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(fromKeyValue));
      newKeyInfo.setKeyName(toKeyName);
      newKeyInfo.updateModifcationTime();
      BatchOperation batch = new BatchOperation();
      batch.delete(fromKey);
      batch.put(toKey, newKeyInfo.getProtobuf().toByteArray());
      metadataManager.writeBatch(batch);
    } catch (DBException ex) {
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}.",
          volumeName, bucketName, fromKeyName, toKeyName, ex);
      throw new KSMException(ex.getMessage(),
          ResultCodes.FAILED_KEY_RENAME);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public void deleteKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      byte[] objectKey = metadataManager.getDBKeyBytes(
          volumeName, bucketName, keyName);
      byte[] objectValue = metadataManager.get(objectKey);
      if (objectValue == null) {
        throw new KSMException("Key not found",
            KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      byte[] deletingKey = metadataManager.getDeletedKeyName(objectKey);
      BatchOperation batch = new BatchOperation();
      batch.put(deletingKey, objectValue);
      batch.delete(objectKey);
      metadataManager.writeBatch(batch);
    } catch (DBException ex) {
      LOG.error(String.format("Delete key failed for volume:%s "
          + "bucket:%s key:%s", volumeName, bucketName, keyName), ex);
      throw new KSMException(ex.getMessage(), ex,
          ResultCodes.FAILED_KEY_DELETION);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public List<KsmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);

    metadataManager.readLock().lock();
    try {
      return metadataManager.listKeys(volumeName, bucketName,
          startKey, keyPrefix, maxKeys);
    } finally {
      metadataManager.readLock().unlock();
    }
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int count)
      throws IOException {
    metadataManager.readLock().lock();
    try {
      return metadataManager.getPendingDeletionKeys(count);
    } finally {
      metadataManager.readLock().unlock();
    }
  }

  @Override
  public void deletePendingDeletionKey(String objectKeyName)
      throws IOException{
    Preconditions.checkNotNull(objectKeyName);
    if (!objectKeyName.startsWith(OzoneConsts.DELETING_KEY_PREFIX)) {
      throw new IllegalArgumentException("Invalid key name,"
          + " the name should be the key name with deleting prefix");
    }

    // Simply removes the entry from KSM DB.
    metadataManager.writeLock().lock();
    try {
      byte[] pendingDelKey = DFSUtil.string2Bytes(objectKeyName);
      byte[] delKeyValue = metadataManager.get(pendingDelKey);
      if (delKeyValue == null) {
        throw new IOException("Failed to delete key " + objectKeyName
            + " because it is not found in DB");
      }
      metadataManager.delete(pendingDelKey);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public List<BlockGroup> getExpiredOpenKeys() throws IOException {
    metadataManager.readLock().lock();
    try {
      return metadataManager.getExpiredOpenKeys();
    } finally {
      metadataManager.readLock().unlock();
    }
  }

  @Override
  public void deleteExpiredOpenKey(String objectKeyName) throws IOException {
    Preconditions.checkNotNull(objectKeyName);
    if (!objectKeyName.startsWith(OzoneConsts.OPEN_KEY_PREFIX)) {
      throw new IllegalArgumentException("Invalid key name,"
          + " the name should be the key name with open key prefix");
    }

    // Simply removes the entry from KSM DB.
    metadataManager.writeLock().lock();
    try {
      byte[] openKey = DFSUtil.string2Bytes(objectKeyName);
      byte[] delKeyValue = metadataManager.get(openKey);
      if (delKeyValue == null) {
        throw new IOException("Failed to delete key " + objectKeyName
            + " because it is not found in DB");
      }
      metadataManager.delete(openKey);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }
}
