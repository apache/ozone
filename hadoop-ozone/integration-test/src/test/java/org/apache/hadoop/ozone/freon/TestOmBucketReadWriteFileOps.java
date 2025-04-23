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

import static org.apache.hadoop.ozone.freon.OmBucketTestUtils.verifyOMLockMetrics;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.freon.OmBucketTestUtils.ParameterBuilder;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for OmBucketReadWriteFileOps.
 */
public abstract class TestOmBucketReadWriteFileOps implements NonHATests.TestCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteFileOps.class);

  static List<ParameterBuilder> parameters() {
    return Arrays.asList(
        new ParameterBuilder()
            .setTotalThreadCount(10)
            .setNumOfReadOperations(10)
            .setNumOfWriteOperations(5)
            .setReadThreadPercentage(70)
            .setCountForRead(10)
            .setCountForWrite(5)
            .setDescription("default"),
        new ParameterBuilder()
            .setPrefixFilePath("/dir1/dir2/dir3")
            .setTotalThreadCount(10)
            .setNumOfReadOperations(10)
            .setNumOfWriteOperations(5)
            .setReadThreadPercentage(80)
            .setBufferSize(128)
            .setCountForRead(10)
            .setCountForWrite(5)
            .setDescription("with increased buffer and read thread %"),
        new ParameterBuilder()
            .setPrefixFilePath("/")
            .setTotalThreadCount(15)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(3)
            .setDataSize("128B")
            .setCountForRead(5)
            .setCountForWrite(3)
            .setDescription("with 128 byte object"),
        new ParameterBuilder()
            .setPrefixFilePath("/dir1/")
            .setTotalThreadCount(10)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(3)
            .setCountForRead(5)
            .setCountForWrite(3)
            .setDataSize("64B")
            .setBufferSize(16)
            .setDescription("with 64 byte object"),
        new ParameterBuilder()
            .setPrefixFilePath("/dir1/dir2/dir3")
            .setTotalThreadCount(10)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(0)
            .setCountForRead(5)
            .setDescription("pure reads"),
        new ParameterBuilder()
            .setLength(64)
            .setPrefixFilePath("/dir1/dir2/dir3/dir4")
            .setTotalThreadCount(20)
            .setNumOfReadOperations(0)
            .setNumOfWriteOperations(5)
            .setCountForRead(0)
            .setCountForWrite(5)
            .setDescription("pure writes")
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("parameters")
  void testOmBucketReadWriteFileOps(ParameterBuilder parameterBuilder) throws Exception {
    try (OzoneClient client = cluster().newClient()) {
      TestDataUtil.createVolumeAndBucket(client,
          parameterBuilder.getVolumeName(),
          parameterBuilder.getBucketName(),
          parameterBuilder.getBucketArgs().build()
      );
    }

    String rootPath = "o3fs://" + parameterBuilder.getBucketName() + "." +
            parameterBuilder.getVolumeName() + parameterBuilder.getPrefixFilePath();
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    new Freon().getCmd().execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "obrwf",
        "-P", rootPath,
        "-r", String.valueOf(parameterBuilder.getCountForRead()),
        "-w", String.valueOf(parameterBuilder.getCountForWrite()),
        "-g", parameterBuilder.getDataSize(),
        "--buffer", String.valueOf(parameterBuilder.getBufferSize()),
        "-l", String.valueOf(parameterBuilder.getLength()),
        "-c", String.valueOf(parameterBuilder.getTotalThreadCount()),
        "-T", String.valueOf(parameterBuilder.getReadThreadPercentage()),
        "-R", String.valueOf(parameterBuilder.getNumOfReadOperations()),
        "-W", String.valueOf(parameterBuilder.getNumOfWriteOperations()),
        "-n", String.valueOf(1)
    );
    LOG.info("Started verifying OM bucket read/write ops file generation...");
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), cluster().getConf())) {
      Path rootDir = new Path(rootPath.concat(OzoneConsts.OM_KEY_PREFIX));
      FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
      verifyFileCreation(2, fileStatuses, true);

      Path readDir = new Path(rootPath.concat("/readPath"));
      FileStatus[] readFileStatuses = fileSystem.listStatus(readDir);
      verifyFileCreation(parameterBuilder.getCountForRead(), readFileStatuses,
          false);

      Path writeDir = new Path(rootPath.concat("/writePath"));
      FileStatus[] writeFileStatuses = fileSystem.listStatus(writeDir);
      verifyFileCreation(parameterBuilder.getExpectedWriteCount(), writeFileStatuses, false);

      verifyOMLockMetrics(cluster().getOzoneManager().getMetadataManager().getLock()
          .getOMLockMetrics());
    }
  }

  private void verifyFileCreation(int expectedCount, FileStatus[] fileStatuses,
                          boolean checkDirectoryCount) {
    int actual = 0;
    if (checkDirectoryCount) {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          ++actual;
        }
      }
    } else {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isFile()) {
          ++actual;
        }
      }
    }
    assertEquals(expectedCount, actual, "Mismatch Count!");
  }
}
