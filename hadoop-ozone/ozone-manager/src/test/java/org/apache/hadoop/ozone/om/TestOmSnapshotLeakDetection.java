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

package org.apache.hadoop.ozone.om;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;

/**
 * Test {@link OmSnapshot}'s leak detection: the snapshot registers with a shared LeakDetector
 * and warns if it is garbage collected without being closed.
 */
class TestOmSnapshotLeakDetection {

  /**
   * Drive an actual {@link OmSnapshot} instance through GC without closing it and verify the leak
   * is detected. Collaborators (including the {@link DBStore}) are mocked, so no metadata store is
   * opened; this exercises the constructor's LeakDetector registration and the GC-triggered report.
   */
  @Test
  void leakDetectedForUnclosedSnapshot() throws Exception {
    try (LogCapturer logs = LogCapturer.captureLogs(OmSnapshot.class)) {
      OmSnapshot snapshot = newSnapshotWithMockedStore();
      assertThat(snapshot).isNotNull();

      // Drop the only strong reference; the reporter captures no reference back to the snapshot,
      // so it becomes collectible. The report runs asynchronously on the LeakDetector thread.
      snapshot = null;
      for (int i = 0; i < 50 && !logs.getOutput().contains("is not closed properly"); i++) {
        System.gc();
        Thread.sleep(100);
      }
      assertThat(logs.getOutput()).contains("is not closed properly. snapshotName: snap-1");
    }
  }

  /** Closing the snapshot stops the leak tracker, so a GC afterwards must not report a leak. */
  @Test
  void closedSnapshotDoesNotReportLeak() throws Exception {
    try (LogCapturer logs = LogCapturer.captureLogs(OmSnapshot.class)) {
      OmSnapshot snapshot = newSnapshotWithMockedStore();
      snapshot.close();

      snapshot = null;
      System.gc();
      Thread.sleep(100);
      assertThat(logs.getOutput()).doesNotContain("is not closed properly");
    }
  }

  private static OmSnapshot newSnapshotWithMockedStore() {
    DBStore store = mock(DBStore.class);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    when(metadataManager.getStore()).thenReturn(store);
    KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.getMetadataManager()).thenReturn(metadataManager);
    OzoneManager ozoneManager = mock(OzoneManager.class);
    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);
    when(authorizer.isNative()).thenReturn(false);
    PrefixManager prefixManager = mock(PrefixManager.class);

    return new OmSnapshot(keyManager, prefixManager, ozoneManager,
        "vol", "bucket", "snap-1", UUID.randomUUID());
  }
}
