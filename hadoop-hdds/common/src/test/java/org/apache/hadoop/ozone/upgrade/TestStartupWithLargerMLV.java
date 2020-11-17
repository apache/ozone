///*
// package org.apache.hadoop.ozone.upgrade;
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.hadoop.ozone.upgrade;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.hdds.conf.OzoneConfiguration;
//import org.apache.hadoop.hdds.scm.ScmConfigKeys;
//import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
//import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.ExpectedException;
//import org.junit.rules.TemporaryFolder;
//
//import java.io.File;
//import java.io.IOException;
//import java.net.URL;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.nio.file.StandardOpenOption;
//
//public class TestStartupWithLargerMLV {
//  @Rule
//  public TemporaryFolder dataDir = new TemporaryFolder();
//
//  @Rule
//  public ExpectedException thrown = ExpectedException.none();
//
//  private static final String VERSION_FILE_NAME = "VERSION";
//
//  private int slv;
//  private int mlv;
//  private String mlvSpecifier;
//  private File baseVersionFile;
//
//  @Before
//  public void setup() throws Exception {
//    // Iterate features on all components to find largest slv.
//    slv = Math.max(
//        getLargestVersion(HDDSLayoutFeatureCatalog.HDDSLayoutFeature.values()),
//        getLargestVersion(OMLayoutFeature.values())
//    );
//    mlv = slv + 1;
//    // Line added to the base version file to set the MLV.
//    mlvSpecifier = "layoutVersion=" + mlv;
//
//    // Load base version file from resources.
//    URL baseVersionFileURL =
//        getClass().getClassLoader().getResource(VERSION_FILE_NAME);
//    baseVersionFile = new File(baseVersionFileURL.toURI());
//  }
//
//  @Test
//  public void testOMStartup() throws Exception {
//    OzoneConfiguration conf = createVersionFile(OMConfigKeys.OZONE_OM_DB_DIRS,
//        "om");
//    registerExpectedException();
//    OzoneManager.createOm(conf);
//  }
//
//  @Test
//  public void testSCMStartup() throws Exception {
//    OzoneConfiguration conf = createVersionFile(ScmConfigKeys.OZONE_SCM_DB_DIRS,
//        "scm");
//    registerExpectedException();
//    new StorageContainerManager(conf);
//  }
//
//  @Test
//  public void testDatanodeStartup() throws Exception {
//    OzoneConfiguration conf =
//        createVersionFile(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
//        "hdds");
//    registerExpectedException();
//    new DatanodeStateMachine(null, conf, null, null);
//  }
//
//  /**
//   * Copies the base version file for this test from the test resources to
//   * {@code subdir} in the test's temporary folder, adds the test's metadata
//   * layout version to the file, and returns a configuration with {@code
//   * configKey} pointing to the temporary folder.
//   *
//   * @return A new {@link OzoneConfiguration} with the configuration key
//   * {@code dirConfigKey} set to the test's temporary folder.
//   * @throws Exception
//   */
//  private OzoneConfiguration createVersionFile(String dirConfigKey,
//      String subdirName) throws Exception {
//    File subdir = dataDir.newFolder(subdirName);
//    FileUtils.copyFileToDirectory(baseVersionFile, subdir);
//    File versionFile = new File(subdir, baseVersionFile.getName());
//
//    // Append layout version larger than largest slv.
//    Files.write(
//        Paths.get(versionFile.toURI()),
//        mlvSpecifier.getBytes(),
//        StandardOpenOption.APPEND);
//
//    OzoneConfiguration conf = new OzoneConfiguration();
//    conf.set(dirConfigKey, subdir.getPath());
//
//    return conf;
//  }
//
//  private void registerExpectedException() {
//    thrown.expect(IOException.class);
//    thrown.expectMessage(String.format(
//        "Cannot initialize VersionManager. Metadata layout version " +
//        "(%d) > software layout version (%d)", mlv, slv));
//  }
//
//  /**
//   * @return The largest software layout version from a list of layout features.
//   */
//  private int getLargestVersion(LayoutFeature[] features) {
//    int maxVersion = 0;
//    for (LayoutFeature f : features) {
//      maxVersion = Math.max(maxVersion, f.layoutVersion());
//    }
//
//    return maxVersion;
//  }
//}
