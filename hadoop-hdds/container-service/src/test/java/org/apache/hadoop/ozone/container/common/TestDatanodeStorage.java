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

package org.apache.hadoop.ozone.container.common;

import static org.apache.hadoop.ozone.container.common.DatanodeStorage.TESTING_INIT_APPARENT_VERSION_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DatanodeStorage}.
 */
public class TestDatanodeStorage {

  @TempDir
  private Path tempDir;

  private OzoneConfiguration baseConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    return conf;
  }

  @Test
  public void testTestingKeyOverridesDefaultApparentVersion() throws Exception {
    OzoneConfiguration conf = baseConf();
    conf.setInt(TESTING_INIT_APPARENT_VERSION_KEY, 8);

    DatanodeStorage storage = new DatanodeStorage(conf);
    assertEquals(8, storage.getApparentVersion());
  }

  @Test
  public void testDefaultApparentVersionWithoutOverride() throws Exception {
    OzoneConfiguration conf = baseConf();

    DatanodeStorage storage = new DatanodeStorage(conf);
    // Without the testing key and without a datanode.id file present, defaults
    // to SOFTWARE_VERSION.
    assertEquals(HDDSVersion.SOFTWARE_VERSION.serialize(),
        storage.getApparentVersion());
  }
}
