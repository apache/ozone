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

package org.apache.hadoop.ozone.om.lock;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for OBSKeyPathLockStrategy covering single-key, multi-key,
 * and bucket-read-only lock operations.
 */
class TestOBSKeyPathLockStrategy {

  private OBSKeyPathLockStrategy strategy;
  private OMMetadataManager omMetadataManager;
  private String volumeName;
  private String bucketName;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setup() throws Exception {
    strategy = new OBSKeyPathLockStrategy();
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();

    omMetadataManager = mock(OMMetadataManager.class);
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    when(omMetadataManager.getLock()).thenReturn(lock);

    when(omMetadataManager.getBucketKey(anyString(), anyString()))
        .thenReturn(volumeName + "/" + bucketName);
    when(omMetadataManager.getVolumeKey(anyString()))
        .thenReturn(volumeName);

    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    when(bucketTable.get(anyString()))
        .thenReturn(OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .build());
    when(omMetadataManager.getBucketTable()).thenReturn(bucketTable);

    Table<String, OmVolumeArgs> volumeTable = mock(Table.class);
    when(volumeTable.get(anyString()))
        .thenReturn(OmVolumeArgs.newBuilder()
            .setVolume(volumeName)
            .setAdminName("admin")
            .setOwnerName("owner")
            .build());
    when(omMetadataManager.getVolumeTable()).thenReturn(volumeTable);
  }

  @Test
  void testSingleKeyAcquireRelease() throws IOException {
    String keyName = "key1";
    OMLockDetails lockDetails = strategy.acquireWriteLock(
        omMetadataManager, volumeName, bucketName, keyName);
    assertTrue(lockDetails.isLockAcquired());

    strategy.releaseWriteLock(omMetadataManager, volumeName, bucketName,
        keyName);
  }

  @Test
  void testMultiKeyAcquireRelease() throws IOException {
    OMLockDetails lockDetails = strategy.acquireWriteLock(
        omMetadataManager, volumeName, bucketName,
        Arrays.asList("key1", "key2", "key3"));
    assertTrue(lockDetails.isLockAcquired());

    strategy.releaseWriteLock(omMetadataManager, volumeName, bucketName,
        Arrays.asList("key1", "key2", "key3"));
  }

  @Test
  void testBucketReadLockAcquireRelease() throws IOException {
    OMLockDetails lockDetails = strategy.acquireBucketReadLock(
        omMetadataManager, volumeName, bucketName);
    assertTrue(lockDetails.isLockAcquired());

    strategy.releaseBucketReadLock(omMetadataManager, volumeName, bucketName);
  }

  @Test
  void testDifferentKeysCanBeLockdConcurrently() throws Exception {
    String key1 = "keyA";
    String key2 = "keyB";

    CountDownLatch bothLocked = new CountDownLatch(2);
    CountDownLatch done = new CountDownLatch(2);
    AtomicBoolean concurrent = new AtomicBoolean(false);

    Thread t1 = new Thread(() -> {
      try {
        strategy.acquireWriteLock(omMetadataManager, volumeName,
            bucketName, key1);
        bothLocked.countDown();
        bothLocked.await();
        concurrent.set(true);
        strategy.releaseWriteLock(omMetadataManager, volumeName,
            bucketName, key1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        done.countDown();
      }
    });

    Thread t2 = new Thread(() -> {
      try {
        strategy.acquireWriteLock(omMetadataManager, volumeName,
            bucketName, key2);
        bothLocked.countDown();
        bothLocked.await();
        concurrent.set(true);
        strategy.releaseWriteLock(omMetadataManager, volumeName,
            bucketName, key2);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        done.countDown();
      }
    });

    t1.start();
    t2.start();
    done.await();

    assertTrue(concurrent.get(),
        "Different keys should be lockable concurrently");
  }

  @Test
  void testMultiKeyOrderedLocking() throws IOException {
    OMLockDetails lockDetails = strategy.acquireWriteLock(
        omMetadataManager, volumeName, bucketName,
        Arrays.asList("zebra", "apple", "mango"));
    assertTrue(lockDetails.isLockAcquired());

    strategy.releaseWriteLock(omMetadataManager, volumeName, bucketName,
        Arrays.asList("zebra", "apple", "mango"));
  }

  @Test
  void testSingleKeyReadLock() throws IOException {
    String keyName = "key1";
    OMLockDetails lockDetails = strategy.acquireReadLock(
        omMetadataManager, volumeName, bucketName, keyName);
    assertTrue(lockDetails.isLockAcquired());

    strategy.releaseReadLock(omMetadataManager, volumeName, bucketName,
        keyName);
  }

  @Test
  void testEmptyKeyCollectionFallsBack() throws IOException {
    OMLockDetails lockDetails = strategy.acquireWriteLock(
        omMetadataManager, volumeName, bucketName,
        Collections.emptyList());
    assertTrue(lockDetails.isLockAcquired());

    strategy.releaseWriteLock(omMetadataManager, volumeName, bucketName,
        Collections.emptyList());
  }

  @Test
  void testBucketReadLockAllowsConcurrentReaders() throws Exception {
    CountDownLatch bothLocked = new CountDownLatch(2);
    CountDownLatch done = new CountDownLatch(2);
    AtomicBoolean concurrent = new AtomicBoolean(false);

    Thread t1 = new Thread(() -> {
      try {
        strategy.acquireBucketReadLock(omMetadataManager, volumeName,
            bucketName);
        bothLocked.countDown();
        bothLocked.await();
        concurrent.set(true);
        strategy.releaseBucketReadLock(omMetadataManager, volumeName,
            bucketName);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        done.countDown();
      }
    });

    Thread t2 = new Thread(() -> {
      try {
        strategy.acquireBucketReadLock(omMetadataManager, volumeName,
            bucketName);
        bothLocked.countDown();
        bothLocked.await();
        concurrent.set(true);
        strategy.releaseBucketReadLock(omMetadataManager, volumeName,
            bucketName);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        done.countDown();
      }
    });

    t1.start();
    t2.start();
    done.await();

    assertTrue(concurrent.get(),
        "Bucket read locks should allow concurrent readers");
  }
}
