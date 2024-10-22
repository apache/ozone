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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Class to test class MultiLocks
 */
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
    verify(mockLock, times(2)).acquireWriteLock(mockResource, anyString());
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
    OMException exception = assertThrows(OMException.class, new Executable() {
      @Override
      public void execute() throws Throwable {
        multiLocks.acquireLock(objects);
      }
    });

    assertEquals("More locks cannot be acquired when locks have been already acquired. Locks acquired : [obj1]",
        exception.getMessage());
  }
}
