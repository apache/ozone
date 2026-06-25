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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_GC_LOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Class to test class MultiLocks.
 */
@ExtendWith(MockitoExtension.class)
public class TestMultiSnapshotLocks {
  @Mock
  private IOzoneManagerLock mockLock;

  @Mock
  private OzoneManagerLock.LeveledResource mockResource;

  private MultiSnapshotLocks multiSnapshotLocks;
  private UUID obj1 = UUID.randomUUID();
  private UUID obj2 = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    // Initialize MultiLocks with mock dependencies
    multiSnapshotLocks = new MultiSnapshotLocks(mockLock, mockResource, true);
  }

  @Test
  public void testMultiSnapshotLocksWithMultipleResourceLocksMultipleTimes() throws OMException {
    OzoneManagerLock omLock = new OzoneManagerLock(new OzoneConfiguration());
    MultiSnapshotLocks multiSnapshotLocks1 = new MultiSnapshotLocks(omLock, SNAPSHOT_GC_LOCK, true);
    MultiSnapshotLocks multiSnapshotLocks2 = new MultiSnapshotLocks(omLock, SNAPSHOT_GC_LOCK, true);
    Collection<UUID> uuid1 = Collections.singleton(UUID.randomUUID());
    Collection<UUID> uuid2 = Collections.singleton(UUID.randomUUID());
    for (int i = 0; i < 10; i++) {
      assertTrue(multiSnapshotLocks1.acquireLock(uuid1).isLockAcquired());
      assertTrue(multiSnapshotLocks2.acquireLock(uuid2).isLockAcquired());
      multiSnapshotLocks1.releaseLock();
      multiSnapshotLocks2.releaseLock();
    }
  }

  @Test
  void testAcquireLockSuccess() throws Exception {
    List<UUID> objects = Arrays.asList(obj1, obj2);
    OMLockDetails mockLockDetails = mock(OMLockDetails.class);
    when(mockLockDetails.isLockAcquired()).thenReturn(true);

    // Simulate successful lock acquisition for each object
    when(mockLock.acquireWriteLocks(eq(mockResource), anyList())).thenReturn(mockLockDetails);

    OMLockDetails result = multiSnapshotLocks.acquireLock(objects);

    assertEquals(mockLockDetails, result);
    verify(mockLock, times(1)).acquireWriteLocks(ArgumentMatchers.eq(mockResource), any());
  }

  @Test
  void testAcquireLockFailureReleasesAll() throws Exception {

    List<UUID> objects = Arrays.asList(obj1, obj2);
    OMLockDetails failedLockDetails = mock(OMLockDetails.class);
    when(failedLockDetails.isLockAcquired()).thenReturn(false);

    // Simulate failure during lock acquisition
    when(mockLock.acquireWriteLocks(eq(mockResource), anyCollection())).thenReturn(failedLockDetails);

    OMLockDetails result = multiSnapshotLocks.acquireLock(objects);

    assertEquals(failedLockDetails, result);
    assertTrue(multiSnapshotLocks.getObjectLocks().isEmpty());
  }

  @Test
  void testReleaseLock() throws Exception {
    List<UUID> objects = Arrays.asList(obj1, obj2);
    OMLockDetails mockLockDetails = mock(OMLockDetails.class);
    when(mockLockDetails.isLockAcquired()).thenReturn(true);

    // Acquire locks first
    when(mockLock.acquireWriteLocks(eq(mockResource), anyCollection())).thenReturn(mockLockDetails);
    multiSnapshotLocks.acquireLock(objects);
    assertFalse(multiSnapshotLocks.getObjectLocks().isEmpty());
    when(mockLockDetails.isLockAcquired()).thenReturn(false);
    when(mockLock.releaseWriteLocks(eq(mockResource), anyCollection())).thenReturn(mockLockDetails);
    // Now release locks
    multiSnapshotLocks.releaseLock();

    // Verify that locks are released in order
    verify(mockLock).releaseWriteLocks(eq(mockResource), any());
    assertTrue(multiSnapshotLocks.getObjectLocks().isEmpty());
  }

  @Test
  void testAcquireLockWhenLockIsAlreadyAcquired() throws Exception {
    List<UUID> objects = Collections.singletonList(obj1);
    OMLockDetails mockLockDetails = mock(OMLockDetails.class);
    when(mockLockDetails.isLockAcquired()).thenReturn(true);

    // Acquire a lock first
    when(mockLock.acquireWriteLocks(any(), anyList())).thenReturn(mockLockDetails);
    multiSnapshotLocks.acquireLock(objects);

    // Try acquiring locks again without releasing
    OMException exception = assertThrows(OMException.class, () -> multiSnapshotLocks.acquireLock(objects));

    assertEquals(
        String.format("More locks cannot be acquired when locks have been already acquired. Locks acquired : [[%s]]",
            obj1.toString()), exception.getMessage());
  }
}
