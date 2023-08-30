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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.junit.Assert.fail;

/**
 * Class tests create with object store and getFileStatus.
 */
public class TestOzoneFSWithObjectStoreCreate {

  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private String rootPath;

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;

  private OzoneFileSystem o3fs;

  private String volumeName;

  private String bucketName;

  @BeforeClass
  public static void initClass() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @AfterClass
  public static void teardownClass() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void init() throws Exception {
    volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    OzoneConfiguration conf = cluster.getConf();

    // create a volume and a bucket to be used by OzoneFileSystem
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName);

    rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucketName,
        volumeName);
    o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath), conf);
  }

  @After
  public void teardown() {
    IOUtils.closeQuietly(o3fs);
  }

  @Test
  public void testObjectStoreCreateWithO3fs() throws Exception {
    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);


    // Use ObjectStore API to create keys. This similates how s3 create keys.
    String parentDir = "/dir1/dir2/dir3/dir4/";


    List<String> keys = new ArrayList<>();
    keys.add("/dir1");
    keys.add("/dir1/dir2");
    keys.add("/dir1/dir2/dir3");
    keys.add("/dir1/dir2/dir3/dir4/");
    for (int i = 1; i <= 3; i++) {
      int length = 10;
      String fileName = parentDir.concat("file" + i + "/");
      keys.add(fileName);
      OzoneOutputStream ozoneOutputStream =
          ozoneBucket.createKey(fileName, length);
      byte[] b = new byte[10];
      Arrays.fill(b, (byte)96);
      ozoneOutputStream.write(b);
      ozoneOutputStream.close();
    }

    // check
    for (int i = 1; i <= 3; i++) {
      String fileName = parentDir.concat("file" + i + "/");
      Path p = new Path(fileName);
      Assert.assertTrue(o3fs.getFileStatus(p).isFile());
      checkAncestors(p);
    }

    // Delete keys with object store api delete
    for (int i = 1; i <= 3; i++) {
      String fileName = parentDir.concat("file" + i + "/");
      ozoneBucket.deleteKey(fileName);
    }


    // Delete parent dir via o3fs.
    boolean result = o3fs.delete(new Path("/dir1"), true);
    Assert.assertTrue(result);

    // No Key should exist.
    for (String key : keys) {
      checkPath(new Path(key));
    }


    for (int i = 1; i <= 3; i++) {
      int length = 10;
      String fileName = parentDir.concat("file" + i + "/");
      OzoneOutputStream ozoneOutputStream =
          ozoneBucket.createKey(fileName, length);
      byte[] b = new byte[10];
      Arrays.fill(b, (byte)96);
      ozoneOutputStream.write(b);
      ozoneOutputStream.close();
    }

    o3fs.mkdirs(new Path("/dest"));
    o3fs.rename(new Path("/dir1"), new Path("/dest"));

    // No source Key should exist.
    for (String key : keys) {
      checkPath(new Path(key));
    }

    // check dest path.
    for (int i = 1; i <= 3; i++) {
      String fileName = "/dest/".concat(parentDir.concat("/file" + i + "/"));
      Path p = new Path(fileName);
      Assert.assertTrue(o3fs.getFileStatus(p).isFile());
      checkAncestors(p);
    }

  }

  private void checkKeyList(Iterator<? extends OzoneKey > ozoneKeyIterator,
      List<String> keys) {

    LinkedList<String> outputKeys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      outputKeys.add(ozoneKey.getName());
    }

    Assert.assertEquals(keys, outputKeys);
  }

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
      byte[] input)
      throws Exception {

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createKey(key, length);

    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();

    // Read the key with given key name.
    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(key);
    byte[] read = new byte[length];
    ozoneInputStream.read(read, 0, length);
    ozoneInputStream.close();

    String inputString = new String(input, UTF_8);
    Assert.assertEquals(inputString, new String(read, UTF_8));

    // Read using filesystem.
    FSDataInputStream fsDataInputStream = o3fs.open(new Path(key));
    read = new byte[length];
    fsDataInputStream.read(read, 0, length);
    fsDataInputStream.close();

    Assert.assertEquals(inputString, new String(read, UTF_8));
  }

  private void checkPath(Path path) {
    try {
      o3fs.getFileStatus(path);
      fail("testObjectStoreCreateWithO3fs failed for Path" + path);
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof FileNotFoundException);
      Assert.assertTrue(ex.getMessage().contains("No such file or directory"));
    }
  }

  private void checkAncestors(Path p) throws Exception {
    p = p.getParent();
    while (p.getParent() != null) {
      FileStatus fileStatus = o3fs.getFileStatus(p);
      Assert.assertTrue(fileStatus.isDirectory());
      p = p.getParent();
    }
  }

}
