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
package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test for {@link StorageVolumeUtil}.
 */
public class TestStorageVolumeUtil {
  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  private HddsVolume.Builder hddsVolumeBuilder;
  private DbVolume.Builder dbVolumeBuilder;

  @Before
  public void setup() throws Exception {
    hddsVolumeBuilder = new HddsVolume.Builder(folder.newFolder().getPath())
        .datanodeUuid(DATANODE_UUID)
        .conf(CONF)
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);
    dbVolumeBuilder = new DbVolume.Builder(folder.newFolder().getPath())
        .datanodeUuid(DATANODE_UUID)
        .conf(CONF)
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);
  }

  @Test
  public void testCheckVolumeNoDupDbStoreCreated() throws IOException {
    ContainerTestUtils.enableSchemaV3(CONF);

    HddsVolume hddsVolume = hddsVolumeBuilder.build();
    HddsVolume spyHddsVolume = spy(hddsVolume);
    DbVolume dbVolume = dbVolumeBuilder.build();
    MutableVolumeSet dbVolumeSet = mock(MutableVolumeSet.class);
    when(dbVolumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(dbVolume));

    // check the dbVolume first for hddsVolume to use
    boolean res = StorageVolumeUtil.checkVolume(dbVolume, CLUSTER_ID,
        CLUSTER_ID, CONF, null, null);
    assertTrue(res);

    // checkVolume for the 1st time: hddsFiles.length == 1
    res = StorageVolumeUtil.checkVolume(spyHddsVolume, CLUSTER_ID,
        CLUSTER_ID, CONF, null, dbVolumeSet);
    assertTrue(res);
    // createDbStore called as expected
    verify(spyHddsVolume, times(1)).createDbStore(dbVolumeSet);

    // checkVolume for the 2nd time: hddsFiles.length == 2
    res = StorageVolumeUtil.checkVolume(spyHddsVolume, CLUSTER_ID,
        CLUSTER_ID, CONF, null, dbVolumeSet);
    assertTrue(res);

    // should only call createDbStore once, so no dup db instance
    verify(spyHddsVolume, times(1)).createDbStore(dbVolumeSet);
  }
}
