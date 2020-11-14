/**
 package org.apache.hadoop.ozone.upgrade;
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
package org.apache.hadoop.ozone.upgrade;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestStartupWithLargerMLV {
  @Rule
  public TemporaryFolder dataDir;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  // Base version file contents:
  /*
  #Tue Nov 10 19:51:26 UTC 2020
cTime=1605037869816
clusterID=CID-bf14896a-524e-46e3-951c-fbceec67a03a
omUuid=6c0f492a-876d-429a-a2d3-ff3244ca93e7
nodeType=OM
scmUuid=27ff2467-c314-43e8-9080-bc938bb673d2
   */

  private static final String VERSION_FILE_NAME = "VERSION";
  private int slv;
  private int mlv;
  private String mlvSpecifier;
  private File baseVersionFile;

  @Before
  public void setup() throws Exception {
    // Iterate features on all components to find largest slv.
    slv = Math.max(
        getLargestVersion(HDDSLayoutFeatureCatalog.HDDSLayoutFeature.values()),
        getLargestVersion(OMLayoutFeature.values())
    );
    mlv = slv + 1;
    mlvSpecifier = "layoutVersion=" + mlv;

    // Load version file from resources.
    URL baseVersionFileURL =
        getClass().getClassLoader().getResource(VERSION_FILE_NAME);
    baseVersionFile = new File(baseVersionFileURL.toURI());
  }

  @Test
  public void testOMStartup() throws Exception {
    OzoneConfiguration conf = createVersionFile(OMConfigKeys.OZONE_OM_DB_DIRS,
        "om");
    registerExpectedException();
    OzoneManager.createOm(conf);
  }

  @Test
  void testSCMStartup() {
    OzoneConfiguration conf = createVersionFile(OMConfigKeys.HDDS_SCM_DB_DIRS,
        "scm");
    registerExpectedException();
    new StorageContainerManager(conf);
  }

  @Test
  void testDatanodeStartup() {
    OzoneConfiguration conf =
        createVersionFile(OMConfigKeys.HDDS_DATANODE_DB_DIRS,
        "hdds");
    registerExpectedException();
    new DatanodeStateMachine(null, conf, null, null);
  }

  private OzoneConfiguration createVersionFile(String dirConfigKey,
      String subdirName) throws Exception {
    // Copy version file to om, scm, and hdds subdirs of dataDir.
    File subdir = dataDir.newFolder(subdirName);
    FileUtils.copyFileToDirectory(baseVersionFile, subdir);
    File versionFile = new File(subdir, baseVersionFile.getName());

    // Append layout version larger than largest slv.
    Files.write(
        Paths.get(versionFile.toURI()),
        mlvSpecifier.getBytes(),
        StandardOpenOption.APPEND);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(dirConfigKey, subdirName);

    return conf;
  }

  private void registerExpectedException() {
    thrown.expect(IOException.class);
    thrown.expectMessage(String.format(
        "Cannot initialize VersionManager. Metadata layout version " +
        "(%d) > software layout version (%d)", mlv, slv));
  }

  private int getLargestVersion(LayoutFeature[] features) {
    int maxVersion = 0;
    for (LayoutFeature f : features) {
      maxVersion = Math.max(maxVersion, f.layoutVersion());
    }

    return maxVersion;
  }
}
