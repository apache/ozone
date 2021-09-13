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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Test OzoneFileSystem Interfaces - prefix layout.
 *
 * This test will test the various interfaces i.e.
 * create, read, write, getFileStatus
 */
@RunWith(Parameterized.class)
public class TestOzoneFileInterfacesWithFSO extends TestOzoneFileInterfaces {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{false, true, true}});
  }

  public TestOzoneFileInterfacesWithFSO(boolean setDefaultFs,
      boolean useAbsolutePath, boolean enabledFileSystemPaths) {
    super(setDefaultFs, useAbsolutePath, enabledFileSystemPaths);
  }

  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    TestOMRequestUtils.configureFSOptimizedPaths(conf, enableFileSystemPaths,
        OMConfigKeys.OZONE_OM_METADATA_LAYOUT_PREFIX);
    return conf;
  }

  @Override
  @Test
  @Ignore("HDDS-2939")
  public void testReplication() throws IOException {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Ignore("HDDS-2939")
  public void testPathToKey() throws Exception {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Ignore("HDDS-2939")
  public void testFileSystemInit() throws IOException {
    // ignore as this is not relevant to PREFIX layout changes
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
