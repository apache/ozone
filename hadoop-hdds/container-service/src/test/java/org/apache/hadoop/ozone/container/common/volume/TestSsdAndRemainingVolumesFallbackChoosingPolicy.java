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
package org.apache.hadoop.ozone.container.common.volume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.util.DiskChecker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test choosing of SSD volumes and fallback to other available volumes
 * in a round-robin manner.
 */
public class TestSsdAndRemainingVolumesFallbackChoosingPolicy {

  private static List<HddsVolume> volumes = new ArrayList<>();
  private static HddsVolume hddsVolume1;
  private static HddsVolume hddsVolume2;
  private static HddsVolume hddsVolume3;

  private SsdVolumeChoosingPolicy volumeChoosingPolicy =
      new SsdAndRemainingVolumesFallbackChoosingPolicy();

  @BeforeAll
  public static void setup() {
    hddsVolume1 = mock(HddsVolume.class);
    when(hddsVolume1.getAvailable()).thenReturn(100L);
    when(hddsVolume1.getCommittedBytes()).thenReturn(2L);
    when(hddsVolume1.getCapacity()).thenReturn(80L);
    when(hddsVolume1.getStorageType()).thenReturn(StorageType.SSD);

    hddsVolume2 = mock(HddsVolume.class);
    when(hddsVolume2.getAvailable()).thenReturn(100L);
    when(hddsVolume2.getCommittedBytes()).thenReturn(2L);
    when(hddsVolume2.getCapacity()).thenReturn(80L);
    when(hddsVolume2.getStorageType()).thenReturn(StorageType.SSD);

    hddsVolume3 = mock(HddsVolume.class);
    when(hddsVolume3.getAvailable()).thenReturn(300L);
    when(hddsVolume3.getCommittedBytes()).thenReturn(2L);
    when(hddsVolume3.getCapacity()).thenReturn(90L);
    when(hddsVolume3.getStorageType()).thenReturn(StorageType.DISK);

    ConfigurationSource conf = mock(ConfigurationSource.class);
    when(conf.isConfigured(anyString())).thenReturn(false);
    when(conf.getStorageSize(anyString(), anyString(), any(StorageUnit.class)))
        .thenReturn(80.0);

    when(hddsVolume1.getConf()).thenReturn(conf);
    when(hddsVolume2.getConf()).thenReturn(conf);
    when(hddsVolume3.getConf()).thenReturn(conf);

    volumes.add(hddsVolume1);
    volumes.add(hddsVolume2);
    volumes.add(hddsVolume3);
  }

  @Test
  public void testRoundRobinWayOfTheSsdStorageChoosingPolicy()
      throws IOException {
    // when
    HddsVolume chosenVolume1 = volumeChoosingPolicy.chooseVolume(volumes, 1L);
    HddsVolume chosenVolume2 = volumeChoosingPolicy.chooseVolume(volumes, 1L);
    HddsVolume chosenVolume3 = volumeChoosingPolicy.chooseVolume(volumes, 1L);
    HddsVolume chosenVolume4 = volumeChoosingPolicy.chooseVolume(volumes, 1L);

    // then
    assertEquals(hddsVolume1, chosenVolume1);
    assertEquals(hddsVolume2, chosenVolume2);
    assertEquals(hddsVolume1, chosenVolume3);
    assertEquals(hddsVolume2, chosenVolume4);
  }

  @Test
  public void testFallbackWayOfSsdStorageVolumePolicy() throws IOException {
    // when
    HddsVolume chosenVolume = volumeChoosingPolicy.chooseVolume(volumes, 100L);

    // then
    assertEquals(hddsVolume3, chosenVolume);
  }

  @Test
  public void testExceptionIfSpaceIsNotEnough() {
    // when
    Exception ex = assertThrows(DiskChecker.DiskOutOfSpaceException.class,
        () -> volumeChoosingPolicy.chooseVolume(volumes, 400L));

    // then
    assertTrue(ex.getMessage().contains(
        "No volumes have enough space for a new container.  " +
        "Most available space: 298 bytes"), ex.getMessage());
  }

}
