/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

/**
 * Test for HadoopDirTreeGenerator.
 */
public class TestHadoopDirTreeGenerator {

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster = null;
  private ObjectStore store = null;
  private static final Logger LOG =
          LoggerFactory.getLogger(TestHadoopDirTreeGenerator.class);
  private OzoneClient client;

  @Before
  public void setup() {
    path = GenericTestUtils
            .getTempPath(TestHadoopDirTreeGenerator.class.getSimpleName());
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() throws IOException {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
      FileUtils.deleteDirectory(new File(path));
    }
  }

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  private void startCluster() throws Exception {
    conf = getOzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    client = OzoneClientFactory.getRpcClient(conf);
    store = client.getObjectStore();
  }

  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @Test
  public void testNestedDirTreeGeneration() throws Exception {
    try {
      startCluster();
      FileOutputStream out = FileUtils.openOutputStream(new File(path,
              "conf"));
      cluster.getConf().writeXml(out);
      out.getFD().sync();
      out.close();

      verifyDirTree("vol1", "bucket1", 1,
              1, 1, "0");
      verifyDirTree("vol2", "bucket1", 1,
              5, 1, "5B");
      verifyDirTree("vol3", "bucket1", 2,
              5, 3, "1B");
      verifyDirTree("vol4", "bucket1", 3,
              2, 4, "2B");
      verifyDirTree("vol5", "bucket1", 5,
              4, 1, "0");
      // default page size is Constants.LISTING_PAGE_SIZE = 1024
      verifyDirTree("vol6", "bucket1", 2,
              1, 1100, "0");
    } finally {
      shutdown();
    }
  }

  private void verifyDirTree(String volumeName, String bucketName, int depth,
                             int span, int fileCount, String perFileSize)
          throws IOException {

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    String rootPath = "o3fs://" + bucketName + "." + volumeName;
    String confPath = new File(path, "conf").getAbsolutePath();
    new Freon().execute(
        new String[]{"-conf", confPath, "dtsg", "-d", depth + "", "-c",
            fileCount + "", "-s", span + "", "-n", "1", "-r", rootPath,
            "-g", perFileSize});
    // verify the directory structure
    LOG.info("Started verifying the directory structure...");
    FileSystem fileSystem = FileSystem.get(URI.create(rootPath),
            conf);
    Path rootDir = new Path(rootPath.concat("/"));
    // verify root path details
    FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
    for (FileStatus fileStatus : fileStatuses) {
      // verify the num of peer directories, expected span count is 1
      // as it has only one dir at root.
      verifyActualSpan(1, fileStatuses);
      int actualDepth =
          traverseToLeaf(fileSystem, fileStatus.getPath(), 1, depth, span,
              fileCount, StorageSize.parse(perFileSize, StorageUnit.BYTES));
      Assert.assertEquals("Mismatch depth in a path",
              depth, actualDepth);
    }
  }

  private int traverseToLeaf(FileSystem fs, Path dirPath, int depth,
                             int expectedDepth, int expectedSpanCnt,
                             int expectedFileCnt, StorageSize perFileSize)
          throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dirPath);
    // check the num of peer directories except root and leaf as both
    // has less dirs.
    if (depth < expectedDepth - 1) {
      verifyActualSpan(expectedSpanCnt, fileStatuses);
    }
    int actualNumFiles = 0;
    ArrayList <String> files = new ArrayList<>();
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        ++depth;
        return traverseToLeaf(fs, fileStatus.getPath(), depth, expectedDepth,
                expectedSpanCnt, expectedFileCnt, perFileSize);
      } else {
        Assert.assertEquals("Mismatches file len", perFileSize.toBytes(),
            fileStatus.getLen());
        String fName = fileStatus.getPath().getName();
        Assert.assertFalse("actualNumFiles:" + actualNumFiles +
                        ", fName:" + fName + ", expectedFileCnt:" +
                        expectedFileCnt + ", depth:" + depth,
                files.contains(fName));
        files.add(fName);
        actualNumFiles++;
      }
    }
    Assert.assertEquals("Mismatches files count in a directory",
            expectedFileCnt, actualNumFiles);
    return depth;
  }

  private int verifyActualSpan(int expectedSpanCnt,
                               FileStatus[] fileStatuses) {
    int actualSpan = 0;
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        ++actualSpan;
      }
    }
    Assert.assertEquals("Mismatches subdirs count in a directory",
            expectedSpanCnt, actualSpan);
    return actualSpan;
  }
}
