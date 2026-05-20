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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServletRequest;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class provides utilities to test the behavior of methods in the
 * OMDBCheckpointUtils class related to ozone database checkpoint tarball
 * logging and snapshot data inclusion.
 */
public class TestOMDBCheckpointUtils {

  @TempDir
  private Path dbDir;

  /**
   * Writes a given number of fake sst files with a given size to a given directory.
   */
  private void writeSstFilesToDirectory(Path dir, int numFiles, int fileSize) throws IOException {
    for (int i = 0; i < numFiles; i++) {
      byte[] data = new byte[fileSize]; // 10KB
      Random random = new Random();
      random.nextBytes(data);
      Files.write(dir.resolve(i + ".sst"), data);
    }
  }

  @Test
  public void testlogEstimatedTarballSize() throws IOException, InterruptedException, TimeoutException {
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(OMDBCheckpointUtils.class);
    writeSstFilesToDirectory(dbDir, 10, 10 * 1024);
    Set<Path> snapshotDirs = new HashSet<>();
    // without snapshots
    OMDBCheckpointUtils.logEstimatedTarballSize(dbDir, snapshotDirs);
    // 100KB checkpoint
    // 10 in checkpoint
    String expectedLogLine = getExpectedLogLine("100 KB", 10, 0);
    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(expectedLogLine), 1000, 5000);
    // with snapshots
    snapshotDirs.add(dbDir);
    OMDBCheckpointUtils.logEstimatedTarballSize(dbDir, snapshotDirs);
    // 100KB checkpoint + 100KB snapshot
    // 10 in checkpoint + 10 in snapshot
    String expectedLogLineWithSnapshots = getExpectedLogLine("200 KB", 20, 1);
    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(expectedLogLineWithSnapshots),
        1000, 5000);
  }

  private static String getExpectedLogLine(String expectedDataSize, int expectedSSTFiles,
      int expectedSnapshots) {
    String baseMessage = String.format("Estimates for Checkpoint Tarball Stream - Data size: %s, SST files: ",
        expectedDataSize);
    if (expectedSnapshots <= 0) {
      return baseMessage;
    }
    return String.format("%s%d, snapshots: %d", baseMessage, expectedSSTFiles, expectedSnapshots);
  }

  @Test
  public void testIncludeSnapshotData() {
    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA)).thenReturn("true");
    assertTrue(OMDBCheckpointUtils.includeSnapshotData(httpServletRequest));
    when(httpServletRequest.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA)).thenReturn("false");
    assertFalse(OMDBCheckpointUtils.includeSnapshotData(httpServletRequest));
  }
}
