/**
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


import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit Test for verifying directory rename operation through OzoneFS.
 */
public class TestOzoneFsRenameDir {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestOzoneFsRenameDir.class);

  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf = null;
  private static FileSystem fs;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);

    // Fetch the host and port for File System init
    DatanodeDetails datanodeDetails = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails();

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri);
    fs =  FileSystem.get(conf);
    LOG.info("fs.defaultFS=" + fs.getUri());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Tests directory rename opertion through OzoneFS.
   * @throws Exception
   */
  @Test(timeout=300_000)
  public void testRenameDir() throws Exception {
    final String dir = "/root_dir/dir1";
    final Path source = new Path(fs.getUri().toString() + dir);
    final Path dest = new Path(source.toString() + ".renamed");
    // Add a sub-dir to the directory to be moved.
    final Path subdir = new Path(source, "sub_dir1");
    fs.mkdirs(subdir);
    LOG.info("Created dir {}", subdir);
    LOG.info("Will move {} to {}", source, dest);
    fs.rename(source, dest);
    assertTrue("Directory rename failed", fs.exists(dest));
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // sub-directories of the renamed directory have also been renamed.
    assertTrue("Keys under the renamed directory not renamed",
        fs.exists(new Path(dest, "sub_dir1")));

    // Test if one path belongs to other FileSystem.
    LambdaTestUtils.intercept(IllegalArgumentException.class, "Wrong FS",
        () -> fs.rename(new Path(fs.getUri().toString() + "fake" + dir), dest));

    // Renaming to same path when src is specified with scheme.
    assertTrue("Renaming to same path should be success.",
        fs.rename(source, new Path(dir)));
  }
}
