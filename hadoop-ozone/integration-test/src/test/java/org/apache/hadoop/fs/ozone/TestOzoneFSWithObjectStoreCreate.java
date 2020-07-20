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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Arrays;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;

/**
 * Class tests create with object store and getFileStatus.
 */
public class TestOzoneFSWithObjectStoreCreate {

  @Rule
  public Timeout timeout = new Timeout(300000);

  private String rootPath;

  private MiniOzoneCluster cluster = null;

  private OzoneFileSystem o3fs;

  private String volumeName;

  private String bucketName;


  @Before
  public void init() throws Exception {
    volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster, volumeName, bucketName);

    rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucketName,
        volumeName);
    o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath), conf);
  }


  @Test
  public void test() throws Exception {

    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    String key1 = "///dir1/dir2/file1";
    String key2 = "///dir1/dir2/file2";
    int length = 10;
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key1, length);
    byte[] b = new byte[10];
    Arrays.fill(b, (byte)96);
    ozoneOutputStream.write(b);
    ozoneOutputStream.close();

    ozoneOutputStream = ozoneBucket.createKey(key2, length);
    ozoneOutputStream.write(b);
    ozoneOutputStream.close();

    // Adding "/" here otherwise Path will be considered as relative path and
    // workingDir will be added.
    key1 = "///dir1/dir2/file1";
    Path p = new Path(key1);
    Assert.assertTrue(o3fs.getFileStatus(p).isFile());

    p = p.getParent();
    checkAncestors(p);


    key2 = "///dir1/dir2/file2";
    p = new Path(key2);
    Assert.assertTrue(o3fs.getFileStatus(p).isFile());
    checkAncestors(p);

  }

  private void checkAncestors(Path p) throws Exception {
    p = p.getParent();
    while(p.getParent() != null) {
      FileStatus fileStatus = o3fs.getFileStatus(p);
      Assert.assertTrue(fileStatus.isDirectory());
      p = p.getParent();
    }
  }

}
