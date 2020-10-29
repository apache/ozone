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

package org.apache.hadoop.fs.ozone;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.test.LambdaTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.junit.Assert.assertTrue;

/**
 * Ozone file system tests that are not covered by contract tests,
 * layout version V1.
 *
 * Note: When adding new test(s), please append it in testFileSystem() to
 * avoid test run time regression.
 *
 * TODO: This class will be replaced once HDDS-4332 is committed to the branch.
 */
@RunWith(Parameterized.class)
public class TestOzoneFileSystemV1 extends TestOzoneFileSystem {

  public TestOzoneFileSystemV1(boolean setDefaultFs) {
    super(setDefaultFs);
  }

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final Logger LOG =
          LoggerFactory.getLogger(TestOzoneFileSystemV1.class);


  @Test(timeout = 300_000)
  @Override
  public void testFileSystem() throws Exception {
    setupOzoneFileSystem();

    testOzoneFsServiceLoader();
    o3fs = (OzoneFileSystem) fs;

    testRenameDir();
    tableCleanup();
  }

  protected void testRenameDir() throws Exception {
    final String dir = "/root_dir/dir1";
    final Path source = new Path(fs.getUri().toString() + dir);
    final Path dest = new Path(source.toString() + ".renamed");
    // Add a sub-dir to the directory to be moved.
    final Path subdir = new Path(source, "sub_dir1");
    fs.mkdirs(subdir);
    LOG.info("Created dir {}", subdir);
    LOG.info("Will move {} to {}", source, dest);
    fs.rename(source, dest);

    // TODO: Will modify this assertion with fs.exists once HDDS-4332 is
    //  committed to the branch.
    TableIterator<String, ? extends Table.KeyValue<String,
            OmDirectoryInfo>> dirIterator = cluster.getOzoneManager()
            .getMetadataManager().getDirectoryTable().iterator();
    String actualDestinKeyName = "/";
    boolean actualDestinPathExists = false;
    boolean actualSubDirPathExists = false;
    Path destinSubDirPath = new Path(dest, "sub_dir1");
    while (dirIterator.hasNext()) {
      Table.KeyValue<String, OmDirectoryInfo> next = dirIterator.next();
      OmDirectoryInfo dirInfo = next.getValue();
      actualDestinKeyName = actualDestinKeyName + dirInfo.getName();

      Path actualDestinKeyPath = new Path(fs.getUri().toString()
              + actualDestinKeyName);
      if (actualDestinKeyPath.equals(dest)) {
        actualDestinPathExists = true;
      }
      if (actualDestinKeyPath.equals(destinSubDirPath)) {
        actualSubDirPathExists = true;
      }
      if (dirIterator.hasNext()) {
        actualDestinKeyName = actualDestinKeyName + "/";
      }
    }
    assertTrue("Directory rename failed", actualDestinPathExists);
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // sub-directories of the renamed directory have also been renamed.
    assertTrue("Keys under the renamed directory not renamed",
            actualSubDirPathExists);

    // Test if one path belongs to other FileSystem.
    Path fakeDir = new Path(fs.getUri().toString() + "fake" + dir);
    LambdaTestUtils.intercept(IllegalArgumentException.class, "Wrong FS",
        () -> fs.rename(fakeDir, dest));
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   *
   * @throws IOException DB failure
   */
  protected void tableCleanup() throws IOException {
    OMMetadataManager metadataMgr = cluster.getOzoneManager()
            .getMetadataManager();
    TableIterator<String, ? extends
            Table.KeyValue<String, OmDirectoryInfo>> dirTableIterator =
            metadataMgr.getDirectoryTable().iterator();
    dirTableIterator.seekToFirst();
    ArrayList <String> dirList = new ArrayList<>();
    while (dirTableIterator.hasNext()) {
      String key = dirTableIterator.key();
      if (StringUtils.isNotBlank(key)) {
        dirList.add(key);
      }
      dirTableIterator.next();
    }

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>>
            cacheIterator = metadataMgr.getDirectoryTable().cacheIterator();
    while(cacheIterator.hasNext()){
      cacheIterator.next();
      cacheIterator.remove();
    }

    for (String dirKey : dirList) {
      metadataMgr.getDirectoryTable().delete(dirKey);
      Assert.assertNull("Unexpected entry!",
              metadataMgr.getDirectoryTable().get(dirKey));
    }

    Assert.assertTrue("DirTable is not empty",
            metadataMgr.getDirectoryTable().isEmpty());

    Assert.assertFalse(metadataMgr.getDirectoryTable().cacheIterator()
            .hasNext());

    TableIterator<String, ? extends
            Table.KeyValue<String, OmKeyInfo>> keyTableIterator =
            metadataMgr.getKeyTable().iterator();
    keyTableIterator.seekToFirst();
    ArrayList <String> fileList = new ArrayList<>();
    while (keyTableIterator.hasNext()) {
      String key = keyTableIterator.key();
      if (StringUtils.isNotBlank(key)) {
        fileList.add(key);
      }
      keyTableIterator.next();
    }

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>>
            keyCacheIterator = metadataMgr.getDirectoryTable().cacheIterator();
    while(keyCacheIterator.hasNext()){
      keyCacheIterator.next();
      keyCacheIterator.remove();
    }

    for (String fileKey : fileList) {
      metadataMgr.getKeyTable().delete(fileKey);
      Assert.assertNull("Unexpected entry!",
              metadataMgr.getKeyTable().get(fileKey));
    }

    Assert.assertTrue("KeyTable is not empty",
            metadataMgr.getKeyTable().isEmpty());

    rootItemCount = 0;
  }

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(FS_TRASH_INTERVAL_KEY, 1);
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
            enabledFileSystemPaths);
    conf.set(OMConfigKeys.OZONE_OM_LAYOUT_VERSION, "V1");
    return conf;
  }
}