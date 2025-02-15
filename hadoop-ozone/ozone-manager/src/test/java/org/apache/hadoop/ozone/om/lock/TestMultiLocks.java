package org.apache.hadoop.ozone.om.lock;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Class to test class MultiLocks.
 */
@ExtendWith(MockitoExtension.class)
public class TestMultiLocks {
  @Mock
  private IOzoneManagerLock mockLock;

  @Mock
  private OzoneManagerLock.Resource mockResource;

  private MultiLocks<String> multiLocks;

  @BeforeEach
  void setUp() {
    // Initialize MultiLocks with mock dependencies
    multiLocks = new MultiLocks<>(mockLock, mockResource, true);
  }

  @Test
  void testAcquireLockSuccess() throws Exception {
    List<String> objects = Arrays.asList("obj1", "obj2");
    OMLockDetails mockLockDetails = mock(OMLockDetails.class);
    when(mockLockDetails.isLockAcquired()).thenReturn(true);

    // Simulate successful lock acquisition for each object
    when(mockLock.acquireWriteLock(any(), anyString())).thenReturn(mockLockDetails);

    OMLockDetails result = multiLocks.acquireLock(objects);

    assertEquals(mockLockDetails, result);
    verify(mockLock, times(2)).acquireWriteLock(ArgumentMatchers.eq(mockResource), anyString());
  }

  @Test
  void testAcquireLockFailureReleasesAll() throws Exception {
    List<String> objects = Arrays.asList("obj1", "obj2");
    OMLockDetails failedLockDetails = mock(OMLockDetails.class);
    when(failedLockDetails.isLockAcquired()).thenReturn(false);

    // Simulate failure during lock acquisition
    when(mockLock.acquireWriteLock(mockResource, "obj1")).thenReturn(failedLockDetails);

    OMLockDetails result = multiLocks.acquireLock(objects);

    assertEquals(failedLockDetails, result);
    verify(mockLock).acquireWriteLock(mockResource, "obj1");
    verify(mockLock, never()).acquireWriteLock(mockResource, "obj2"); // No further lock attempt

    // Verify releaseLock() behavior
    verify(mockLock).releaseWriteLock(mockResource, "obj1");
  }

  @Test
  void testReleaseLock() throws Exception {
    List<String> objects = Arrays.asList("obj1", "obj2");
    OMLockDetails mockLockDetails = mock(OMLockDetails.class);
    when(mockLockDetails.isLockAcquired()).thenReturn(true);

    // Acquire locks first
    when(mockLock.acquireWriteLock(any(), anyString())).thenReturn(mockLockDetails);
    multiLocks.acquireLock(objects);

    // Now release locks
    multiLocks.releaseLock();

    // Verify that locks are released in order
    verify(mockLock).releaseWriteLock(mockResource, "obj1");
    verify(mockLock).releaseWriteLock(mockResource, "obj2");
  }

  @Test
  void testAcquireLockWhenAlreadyAcquiredThrowsException() throws Exception {
    List<String> objects = Collections.singletonList("obj1");
    OMLockDetails mockLockDetails = mock(OMLockDetails.class);
    when(mockLockDetails.isLockAcquired()).thenReturn(true);

    // Acquire a lock first
    when(mockLock.acquireWriteLock(any(), anyString())).thenReturn(mockLockDetails);
    multiLocks.acquireLock(objects);

    // Try acquiring locks again without releasing
    OMException exception = assertThrows(OMException.class, () -> multiLocks.acquireLock(objects));

    assertEquals("More locks cannot be acquired when locks have been already acquired. Locks acquired : [obj1]",
        exception.getMessage());
  }
}
