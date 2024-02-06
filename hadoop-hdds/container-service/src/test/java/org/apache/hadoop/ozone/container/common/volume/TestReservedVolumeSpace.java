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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * To test the reserved volume space.
 */
public class TestReservedVolumeSpace {

  @TempDir
  private Path folder;
  @TempDir
  private Path temp;
  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private HddsVolume.Builder volumeBuilder;

  @BeforeEach
  public void setup() throws Exception {
    volumeBuilder = new HddsVolume.Builder(folder.toString())
        .datanodeUuid(DATANODE_UUID)
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);
  }

  /**
   * Test reserved capacity with respect to the percentage of actual capacity.
   * @throws Exception
   */
  @Test
  public void testVolumeCapacityAfterReserve() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, "0.3");
    HddsVolume hddsVolume = volumeBuilder.conf(conf).build();
    //Reserving
    float percentage = conf.getFloat(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT,
        HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT);

    long volumeCapacity = hddsVolume.getCapacity();
    //Gets the actual total capacity
    long totalCapacity = hddsVolume.getVolumeInfo().get()
        .getUsageForTesting().getCapacity();
    long reservedCapacity = hddsVolume.getVolumeInfo().get()
            .getReservedInBytes();
    //Volume Capacity with Reserved
    long volumeCapacityReserved = totalCapacity - reservedCapacity;

    long reservedFromVolume = hddsVolume.getVolumeInfo().get()
            .getReservedInBytes();
    long reservedCalculated = (long) Math.ceil(totalCapacity * percentage);

    assertEquals(reservedFromVolume, reservedCalculated);
    assertEquals(volumeCapacity, volumeCapacityReserved);
  }

  /**
   * When both configs are set, hdds.datanode.dir.du.reserved is set
   * if the volume matches with volume in config parameter.
   * @throws Exception
   */
  @Test
  public void testReservedWhenBothConfigSet() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, "0.3");
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED,
        folder.toString() + ":500B");
    HddsVolume hddsVolume = volumeBuilder.conf(conf).build();

    long reservedFromVolume = hddsVolume.getVolumeInfo().get()
            .getReservedInBytes();
    assertEquals(reservedFromVolume, 500);
  }

  @Test
  public void testReservedToZeroWhenBothConfigNotSet() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    HddsVolume hddsVolume = volumeBuilder.conf(conf).build();

    long reservedFromVolume = hddsVolume.getVolumeInfo().get()
            .getReservedInBytes();
    assertEquals(reservedFromVolume, 0);
  }

  @Test
  public void testFallbackToPercentConfig() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, "0.3");
    //Setting config for different volume, hence fallback happens
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED,
        temp.toString() + ":500B");
    HddsVolume hddsVolume = volumeBuilder.conf(conf).build();

    long reservedFromVolume = hddsVolume.getVolumeInfo().get()
            .getReservedInBytes();
    assertNotEquals(reservedFromVolume, 0);

    long totalCapacity = hddsVolume.getVolumeInfo().get()
        .getUsageForTesting().getCapacity();
    float percentage = conf.getFloat(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT,
        HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT);
    long reservedCalculated = (long) Math.ceil(totalCapacity * percentage);
    assertEquals(reservedFromVolume, reservedCalculated);
  }

  @Test
  public void testInvalidConfig() throws Exception {
    OzoneConfiguration conf1 = new OzoneConfiguration();

    // 500C doesn't match with any Storage Unit
    conf1.set(ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED,
        folder.toString() + ":500C");
    HddsVolume hddsVolume1 = volumeBuilder.conf(conf1).build();

    long reservedFromVolume1 = hddsVolume1.getVolumeInfo().get()
            .getReservedInBytes();
    assertEquals(reservedFromVolume1, 0);

    OzoneConfiguration conf2 = new OzoneConfiguration();

    //Should be between 0-1.
    conf2.set(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, "20");
    HddsVolume hddsVolume2 = volumeBuilder.conf(conf2).build();

    long reservedFromVolume2 = hddsVolume2.getVolumeInfo().get()
            .getReservedInBytes();
    assertEquals(reservedFromVolume2, 0);
  }
}
