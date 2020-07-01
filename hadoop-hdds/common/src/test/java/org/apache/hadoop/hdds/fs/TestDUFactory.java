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
import org.junit.Test;

import static org.apache.hadoop.test.GenericTestUtils.getTestDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link DUFactory}.
 */
public class TestDUFactory {

  @Test
  public void testCreateViaConfig() {
    TestSpaceUsageFactory.testCreateViaConfig(DUFactory.class);
  }

  @Test
  public void testParams() {
    File dir = getTestDir(getClass().getSimpleName());
    Duration refresh = Duration.ofHours(1);

    OzoneConfiguration conf = new OzoneConfiguration();

    DUFactory.Conf duConf = conf.getObject(DUFactory.Conf.class);
    duConf.setRefreshPeriod(refresh);
    conf.setFromObject(duConf);

    SpaceUsageCheckParams params = new DUFactory()
        .setConfiguration(conf)
        .paramsFor(dir);

    assertSame(dir, params.getDir());
    assertEquals(refresh, params.getRefresh());
    assertSame(DU.class, params.getSource().getClass());
    assertSame(SaveSpaceUsageToFile.class, params.getPersistence().getClass());
  }

}
