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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_DEFAULT_STORAGE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for storage-type-aware volume selection policies.
 */
public class TestStorageTypeVolumeChoosingPolicy {

  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @TempDir
  private File folder;

  private final String scmId = UUID.randomUUID().toString();
  private final String datanodeId = UUID.randomUUID().toString();
  private final List<HddsVolume> createdVolumes = new ArrayList<>();

  @AfterEach
  public void tearDown() {
    createdVolumes.forEach(HddsVolume::shutdown);
  }

  @Test
  public void testRoundRobinChoosesRequestedStorageType() throws Exception {
    HddsVolume diskVolume = createVolume("disk", StorageType.DISK);
    HddsVolume ssdVolume = createVolume("ssd", StorageType.SSD);

    RoundRobinVolumeChoosingPolicy policy = new RoundRobinVolumeChoosingPolicy();
    HddsVolume chosen = policy.chooseVolume(
        Arrays.asList(diskVolume, ssdVolume), 1L, StorageType.SSD);

    assertSame(ssdVolume, chosen);
    assertEquals(1L, ssdVolume.getCommittedBytes());
    assertEquals(0L, diskVolume.getCommittedBytes());
  }

  @Test
  public void testCapacityPolicyRejectsMissingStorageType() throws Exception {
    HddsVolume diskVolume = createVolume("disk", StorageType.DISK);

    CapacityVolumeChoosingPolicy policy = new CapacityVolumeChoosingPolicy();

    assertThrows(DiskOutOfSpaceException.class,
        () -> policy.chooseVolume(
            Collections.singletonList(diskVolume), 1L, StorageType.SSD));
  }

  @Test
  public void testNullStorageTypeUsesDefaultStorageType() throws Exception {
    HddsVolume diskVolume = createVolume("disk", StorageType.DISK);
    HddsVolume ssdVolume = createVolume("ssd", StorageType.SSD);

    RoundRobinVolumeChoosingPolicy policy = new RoundRobinVolumeChoosingPolicy();
    policy.init(StorageType.SSD);
    HddsVolume chosen = policy.chooseVolume(
        Arrays.asList(diskVolume, ssdVolume), 1L, null);

    assertThat(chosen).isSameAs(ssdVolume);
    assertThat(ssdVolume.getCommittedBytes()).isEqualTo(1L);
    assertThat(diskVolume.getCommittedBytes()).isZero();
  }

  @Test
  public void testRequestedStorageTypeOverridesDefaultStorageType() throws Exception {
    HddsVolume diskVolume = createVolume("disk", StorageType.DISK);
    HddsVolume ssdVolume = createVolume("ssd", StorageType.SSD);

    CapacityVolumeChoosingPolicy policy = new CapacityVolumeChoosingPolicy();
    policy.init(StorageType.SSD);
    HddsVolume chosen = policy.chooseVolume(
        Arrays.asList(diskVolume, ssdVolume), 1L, StorageType.DISK);

    assertThat(chosen).isSameAs(diskVolume);
  }

  @Test
  public void testNullStorageTypeRejectsMissingDefaultStorageType() throws Exception {
    HddsVolume ssdVolume = createVolume("ssd", StorageType.SSD);

    CapacityVolumeChoosingPolicy policy = new CapacityVolumeChoosingPolicy();
    policy.init(StorageType.DISK);

    assertThatThrownBy(() -> policy.chooseVolume(
        Collections.singletonList(ssdVolume), 1L, null))
        .isInstanceOf(DiskOutOfSpaceException.class);
    assertThat(ssdVolume.getCommittedBytes()).isZero();
  }

  @Test
  public void testFactoryUsesConfiguredDefaultStorageType() throws Exception {
    HddsVolume diskVolume = createVolume("disk", StorageType.DISK);
    HddsVolume ssdVolume = createVolume("ssd", StorageType.SSD);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DEFAULT_STORAGE_TYPE, "ssd");

    HddsVolume chosen = VolumeChoosingPolicyFactory.getPolicy(conf)
        .chooseVolume(Arrays.asList(diskVolume, ssdVolume), 1L, null);

    assertThat(chosen).isSameAs(ssdVolume);
  }

  @Test
  public void testFactoryRejectsInvalidDefaultStorageType() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DEFAULT_STORAGE_TYPE, "TAPE");

    assertThatThrownBy(() -> VolumeChoosingPolicyFactory.getPolicy(conf))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private HddsVolume createVolume(String name, StorageType storageType)
      throws Exception {
    HddsVolume volume = new HddsVolume.Builder(
        new File(folder, name).getAbsolutePath())
        .conf(CONF)
        .datanodeUuid(datanodeId)
        .storageType(storageType)
        .build();
    StorageVolumeUtil.checkVolume(volume, scmId, scmId, CONF, null, null);
    createdVolumes.add(volume);
    return volume;
  }
}
