/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.fs;

import java.io.File;
import java.time.Duration;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.apache.hadoop.hdds.fs.DedicatedDiskSpaceUsageFactory.Conf.configKeyForRefreshPeriod;
import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link DedicatedDiskSpaceUsageFactory}.
 */
public class TestDedicatedDiskSpaceUsageFactory {

  @Test
  public void testCreateViaConfig() {
    TestSpaceUsageFactory.testCreateViaConfig(
        DedicatedDiskSpaceUsageFactory.class);
  }

  @Test
  public void testParams() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(configKeyForRefreshPeriod(), "2m");
    File dir = getTestDir(getClass().getSimpleName());

    SpaceUsageCheckParams params = new DedicatedDiskSpaceUsageFactory()
        .setConfiguration(conf)
        .paramsFor(dir);

    assertSame(dir, params.getDir());
    assertEquals(Duration.ofMinutes(2), params.getRefresh());
    assertSame(DedicatedDiskSpaceUsage.class, params.getSource().getClass());
  }

}
