/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.eventlistener;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link LocalFileCheckpointStrategy}.
 */
public class TestLocalFileCheckpointStrategy {

  @TempDir
  private Path tempDir;

  @Test
  public void testSeekPositionIsInitiallyNull() throws InterruptedException, IOException {
    Path filePath = Paths.get(tempDir.toString(), UUID.randomUUID().toString());

    OMEventListenerLedgerPollerSeekPosition seekPosition = new OMEventListenerLedgerPollerSeekPosition(
        new LocalFileCheckpointStrategy(filePath));

    assertThat(seekPosition.get()).isNull();
  }

  @Test
  public void testSeekPositionCanBeUpdatedAndReadBack() throws InterruptedException, IOException {
    Path filePath = Paths.get(tempDir.toString(), UUID.randomUUID().toString());

    OMEventListenerLedgerPollerSeekPosition seekPosition = new OMEventListenerLedgerPollerSeekPosition(
        new LocalFileCheckpointStrategy(filePath));

    String value1 = "00000000000000000017";
    seekPosition.set(value1);
    assertThat(seekPosition.get()).isEqualTo(value1);

    String value2 = "00000000000000000020";
    seekPosition.set(value2);
    assertThat(seekPosition.get()).isEqualTo(value2);
  }

  @Test
  public void testSeekPositionCanBeUpdatedAndReloaded() throws InterruptedException, IOException {
    Path filePath = Paths.get(tempDir.toString(), UUID.randomUUID().toString());

    OMEventListenerLedgerPollerSeekPosition seekPosition = new OMEventListenerLedgerPollerSeekPosition(
        new LocalFileCheckpointStrategy(filePath));

    String value = "00000000000000000025";
    seekPosition.set(value);
    assertThat(seekPosition.get()).isEqualTo(value);

    // reload
    seekPosition = new OMEventListenerLedgerPollerSeekPosition(
        new LocalFileCheckpointStrategy(filePath));

    assertThat(seekPosition.get()).isEqualTo(value);
  }
}
