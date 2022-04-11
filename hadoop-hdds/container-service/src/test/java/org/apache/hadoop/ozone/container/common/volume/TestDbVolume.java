/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DbVolume}.
 */
public class TestDbVolume {

  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  private DbVolume.Builder volumeBuilder;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    volumeBuilder = new DbVolume.Builder(folder.getRoot().getPath())
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE).conf(CONF);
  }

  @Test
  public void testFormat() throws IOException {
    DbVolume volume = volumeBuilder.build();

    assertEquals(StorageType.DEFAULT, volume.getStorageType());

    boolean res = volume.format(CLUSTER_ID);
    assertTrue(res);

    File volumeRootDir = new File(folder.getRoot(), DbVolume.DB_VOLUME_DIR);
    assertTrue(volumeRootDir.exists());

    File clusterIdDir = new File(volumeRootDir, CLUSTER_ID);
    assertTrue(clusterIdDir.exists());

    // duplicate format will do nothing and return true
    res = volume.format(CLUSTER_ID);
    assertTrue(res);
  }

  @Test
  public void testInitialize() throws IOException {
    DbVolume volume = volumeBuilder.build();

    // Initialize before format without clusterID should just return true
    // and nothing is created, it happens when we boot up a fresh datanode.
    boolean res = volume.initialize();
    assertTrue(res);

    File volumeRootDir = new File(folder.getRoot(), DbVolume.DB_VOLUME_DIR);
    File clusterIdDir = new File(volumeRootDir, CLUSTER_ID);

    assertFalse(volumeRootDir.exists());
    assertFalse(clusterIdDir.exists());

    // Initialize with clusterID set should do format
    // this happens for test cases.
    volumeBuilder.clusterID(CLUSTER_ID);
    volume = volumeBuilder.build();

    res = volume.initialize();
    assertTrue(res);

    assertTrue(volumeRootDir.exists());
    assertTrue(clusterIdDir.exists());

    // duplicate initialize will do nothing and return true
    res = volume.initialize();
    assertTrue(res);
  }
}
