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

package org.apache.hadoop.hdds.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.File;
import java.time.Duration;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DUOptimizedFactory}.
 */
class TestDUOptimizedFactory {

  @Test
  void testParamsFor(@TempDir File dir) {
    Duration refresh = Duration.ofMinutes(30);
    OzoneConfiguration conf = new OzoneConfiguration();

    DUFactory.Conf duConf = conf.getObject(DUFactory.Conf.class);
    duConf.setRefreshPeriod(refresh);
    conf.setFromObject(duConf);

    DUOptimizedFactory factory = new DUOptimizedFactory();
    factory.setConfiguration(conf);

    Supplier<File> exclusionProvider = () -> new File(dir, "exclude");
    SpaceUsageCheckParams params = factory.paramsFor(dir, exclusionProvider);

    assertSame(dir, params.getDir());
    assertEquals(refresh, params.getRefresh());
    assertSame(DUOptimized.class, params.getSource().getClass());
    assertSame(SaveSpaceUsageToFile.class, params.getPersistence().getClass());
  }
}
