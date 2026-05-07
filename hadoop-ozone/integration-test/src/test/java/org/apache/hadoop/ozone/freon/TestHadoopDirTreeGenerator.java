/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.OzoneFileSystemTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for HadoopDirTreeGenerator.
 */
public abstract class TestHadoopDirTreeGenerator implements NonHATests.TestCase {

  private static final int PAGE_SIZE = 10;

  private ObjectStore store = null;
  private static final Logger LOG =
          LoggerFactory.getLogger(TestHadoopDirTreeGenerator.class);
  private OzoneClient client;

  @BeforeEach
  void setup() throws Exception {
    client = cluster().newClient();
    store = client.getObjectStore();
  }

  @AfterEach
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  @ParameterizedTest
  @EnumSource(names = {"FILE_SYSTEM_OPTIMIZED", "LEGACY"})
  public void testNestedDirTreeGeneration(BucketLayout layout) throws Exception {
    String uuid = UUID.randomUUID().toString();
    verifyDirTree("vol1-" + uuid, "bucket1", 1, 1, 1, "0", layout);
    verifyDirTree("vol2-" + uuid, "bucket1", 1, 5, 1, "5B", layout);
    verifyDirTree("vol3-" + uuid, "bucket1", 2, 5, 3, "1B", layout);
    verifyDirTree("vol4-" + uuid, "bucket1", 3, 2, 4, "2B", layout);
    verifyDirTree("vol5-" + uuid, "bucket1", 5, 4, 1, "0", layout);
    verifyDirTree("vol6-" + uuid, "bucket1", 2, 1, PAGE_SIZE + PAGE_SIZE / 2, "0", layout);
  }

  private void verifyDirTree(String volumeName, String bucketName, int depth,
                             int span, int fileCount, String perFileSize, BucketLayout layout)
          throws IOException {

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName, BucketArgs.newBuilder().setBucketLayout(layout).build());
    String rootPath = OZONE_URI_SCHEME + "://" + bucketName + "." + volumeName;
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    new Freon().getCmd().execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "dtsg",
        "-c", String.valueOf(fileCount),
        "-d", String.valueOf(depth),
        "-g", perFileSize,
        "-n", "1",
        "-r", rootPath,
        "-s", String.valueOf(span)
    );
    // verify the directory structure
    LOG.info("Started verifying the directory structure...");
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    OzoneFileSystemTestUtils.setPageSize(conf, PAGE_SIZE);
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf)) {
      Path rootDir = new Path(rootPath.concat("/"));
      // verify root path details
      FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
      // verify the num of peer directories, expected span count is 1
      // as it has only one dir at root.
      verifyActualSpan(1, Arrays.asList(fileStatuses));
      for (FileStatus fileStatus : fileStatuses) {
        int actualDepth =
            traverseToLeaf(fileSystem, fileStatus.getPath(), 1, depth, span,
                fileCount, StorageSize.parse(perFileSize, StorageUnit.BYTES));
        assertEquals(depth, actualDepth, "Mismatch depth in a path");
      }
    }
  }

  private int traverseToLeaf(FileSystem fs, Path dirPath, int depth,
                             int expectedDepth, int expectedSpanCnt,
                             int expectedFileCnt, StorageSize perFileSize)
          throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dirPath);
    List<FileStatus> fileStatusList = new ArrayList<>();
    Collections.addAll(fileStatusList, fileStatuses);
    // check the num of peer directories except root and leaf as both
    // has less dirs.
    if (depth < expectedDepth - 1) {
      verifyActualSpan(expectedSpanCnt, fileStatusList);
    }
    int actualNumFiles = 0;
    ArrayList <String> files = new ArrayList<>();
    for (FileStatus fileStatus : fileStatusList) {
      if (fileStatus.isDirectory()) {
        ++depth;
        return traverseToLeaf(fs, fileStatus.getPath(), depth, expectedDepth,
                expectedSpanCnt, expectedFileCnt, perFileSize);
      } else {
        assertEquals(perFileSize.toBytes(), fileStatus.getLen(), "Mismatches file len");
        String fName = fileStatus.getPath().getName();
        assertThat(files)
            .withFailMessage(actualNumFiles + "actualNumFiles:" + fName +
                ", fName:" + expectedFileCnt + ", expectedFileCnt:" + depth + ", depth:")
            .doesNotContain(fName);
        files.add(fName);
        actualNumFiles++;
      }
    }
    assertEquals(expectedFileCnt, actualNumFiles, "Mismatches files count in a directory");
    return depth;
  }

  private int verifyActualSpan(int expectedSpanCnt,
                               List<FileStatus> fileStatuses) {
    int actualSpan = 0;
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        ++actualSpan;
      }
    }
    assertEquals(expectedSpanCnt, actualSpan, "Mismatches subdirs count in a directory");
    return actualSpan;
  }
}
