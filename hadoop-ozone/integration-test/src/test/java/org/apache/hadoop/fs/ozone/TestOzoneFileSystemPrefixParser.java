/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.debug.PrefixParser;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;

/**
 * Test Ozone Prefix Parser.
 */
public class TestOzoneFileSystemPrefixParser {

  private static MiniOzoneCluster cluster = null;

  private static FileSystem fs;

  private static String volumeName;

  private static String bucketName;

  private static OzoneConfiguration configuration;

  private static Path dir;
  private static Path file;

  @BeforeClass
  public static void init() throws Exception {
    volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    configuration = new OzoneConfiguration();

    cluster = MiniOzoneCluster.newBuilder(configuration)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    TestDataUtil.createVolumeAndBucket(cluster, volumeName, bucketName,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String rootPath = String
        .format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucketName,
            volumeName);
    fs = FileSystem.get(new URI(rootPath + "/test.txt"), configuration);

    dir = new Path("/a/b/c/d/e");
    fs.mkdirs(dir);
    file = new Path("/a/b/c/file1");
    FSDataOutputStream os = fs.create(file);
    os.close();
  }

  @AfterClass
  public static void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test(timeout = 120000)
  public void testPrefixParsePath() throws Exception {

    cluster.stop();

    // Directory Path
    verifyPrefixParsePath(dir.getParent(), 4, 0, 0, 1);

    // File Path
    verifyPrefixParsePath(file, 3, 1, 1, 1);

    // Verify invalid path
    testPrefixParseWithInvalidPaths();
  }

  private void assertPrefixStats(PrefixParser parser, int volumeCount,
      int bucketCount, int intermediateDirCount, int nonExistentDirCount,
      int fileCount, int dirCount) {
    Assert.assertEquals(volumeCount,
        parser.getParserStats(PrefixParser.Types.VOLUME));
    Assert.assertEquals(bucketCount,
        parser.getParserStats(PrefixParser.Types.BUCKET));
    Assert.assertEquals(intermediateDirCount,
        parser.getParserStats(PrefixParser.Types.INTERMEDIATE_DIRECTORY));
    Assert.assertEquals(nonExistentDirCount,
        parser.getParserStats(PrefixParser.Types.NON_EXISTENT_DIRECTORY));
    Assert.assertEquals(fileCount,
        parser.getParserStats(PrefixParser.Types.FILE));
    Assert.assertEquals(dirCount,
        parser.getParserStats(PrefixParser.Types.DIRECTORY));
  }

  private void testPrefixParseWithInvalidPaths() throws Exception {
    PrefixParser invalidVolumeParser = new PrefixParser();
    String invalidVolumeName =
        RandomStringUtils.randomAlphabetic(10).toLowerCase();
    invalidVolumeParser.parse(invalidVolumeName, bucketName,
        OMStorage.getOmDbDir(configuration).getPath(),
        file.toString());
    assertPrefixStats(invalidVolumeParser, 0, 0, 0, 0, 0, 0);

    PrefixParser invalidBucketParser = new PrefixParser();
    String invalidBucketName =
        RandomStringUtils.randomAlphabetic(10).toLowerCase();
    invalidBucketParser.parse(volumeName, invalidBucketName,
        OMStorage.getOmDbDir(configuration).getPath(),
        file.toString());
    assertPrefixStats(invalidBucketParser, 1, 0, 0, 0, 0, 0);


    Path invalidIntermediateDir = new Path(file.getParent(), "xyz");
    PrefixParser invalidIntermediateDirParser = new PrefixParser();
    invalidIntermediateDirParser.parse(volumeName, bucketName,
        OMStorage.getOmDbDir(configuration).getPath(),
        invalidIntermediateDir.toString());

    assertPrefixStats(invalidIntermediateDirParser, 1, 1, 3, 1, 1, 1);

  }

  private void verifyPrefixParsePath(Path parent, int intermediateDirCount,
      int nonExistentDirCount, int fileCount, int dirCount) throws Exception {
    PrefixParser parser = new PrefixParser();

    parser.parse(volumeName, bucketName,
        OMStorage.getOmDbDir(configuration).getPath(), parent.toString());

    assertPrefixStats(parser, 1, 1, intermediateDirCount, nonExistentDirCount,
        fileCount, dirCount);
  }
}
