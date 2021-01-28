/*
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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test OzoneFileSystem Interfaces layout version V1.
 *
 * This test will test the various interfaces i.e.
 * create, read, write, getFileStatus
 */
@RunWith(Parameterized.class)
public class TestOzoneFileInterfacesV1 extends TestOzoneFileInterfaces {

  public TestOzoneFileInterfacesV1(boolean setDefaultFs,
      boolean useAbsolutePath, boolean enabledFileSystemPaths) {
    super(setDefaultFs, useAbsolutePath, enabledFileSystemPaths);
  }

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    TestOMRequestUtils.configureFSOptimizedPaths(conf,
            enableFileSystemPaths, OMConfigKeys.OZONE_OM_LAYOUT_VERSION_V1);
    return conf;
  }

  @Override
  @Test
  @Ignore("TODO:HDDS-2939")
  public void testDirectory() {

  }

  @Override
  @Test
  @Ignore("TODO:HDDS-2939")
  public void testOzFsReadWrite() {

  }
}
