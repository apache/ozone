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
package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * This class tests Ozone sh shell command with FSO.
 * Inspired by TestS3Shell
 */
public class TestOzoneShellHAWithFSO extends TestOzoneShellHA {

  /**
   * Create a MiniOzoneCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);
    startCluster(conf);
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdownCluster() {
    shutdown();
  }
}
