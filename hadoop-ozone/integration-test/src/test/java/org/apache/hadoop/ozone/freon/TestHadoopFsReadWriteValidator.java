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
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import picocli.CommandLine;

/**
 * Test for HadoopFsReadWriteValidator.
 */
public abstract class TestHadoopFsReadWriteValidator implements NonHATests.TestCase {

  private ObjectStore store = null;
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
  public void testWriteReadValidate(BucketLayout layout) throws Exception {
    String volumeName = "vol-" + UUID.randomUUID();
    String bucketName = "bucket1";
    String prefix = "dfsrw";
    int fileCount = 20;
    long fileSize = 1024;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(layout).build());

    String rootPath = OZONE_URI_SCHEME + "://" + bucketName + "." + volumeName;
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    int exitCode = new Freon().getCmd().execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "dfsrw",
        "-n", String.valueOf(fileCount),
        "-t", "4",
        "-s", fileSize + "B",
        "-p", prefix,
        "-r", rootPath
    );
    assertEquals(0, exitCode, "Freon dfsrw command failed");

    // verify all files were written with the requested size
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf)) {
      FileStatus[] files =
          fileSystem.listStatus(new Path(rootPath + "/" + prefix));
      assertEquals(fileCount, files.length, "Unexpected number of files");
      Set<Long> checksums = new HashSet<>();
      for (FileStatus file : files) {
        assertEquals(fileSize, file.getLen(),
            "Unexpected file size: " + file.getPath());
        checksums.add(checksumOf(fileSystem, file.getPath()));
      }
      // distinct content across threads, otherwise reading the wrong file would
      // still validate
      assertEquals(fileCount, checksums.size(), "Files share their content");
    }
  }

  private static long checksumOf(FileSystem fileSystem, Path file)
      throws IOException {
    Checksum checksum = new CRC32();
    try (InputStream input =
        new CheckedInputStream(fileSystem.open(file), checksum)) {
      // not the hdds IOUtils imported above for closeQuietly
      org.apache.commons.io.IOUtils.copyLarge(input, NullOutputStream.INSTANCE);
    }
    return checksum.getValue();
  }

  /**
   * Once a thread has written --max-files-per-thread files its paths wrap, so
   * the run keeps writing without leaving files it has no checksum for.  The
   * wrap is layout independent, so one layout covers it.
   */
  @Test
  public void testPathsWrapAtMaxFilesPerThread() throws Exception {
    String volumeName = "vol-" + UUID.randomUUID();
    String bucketName = "bucket1";
    String prefix = "dfsrw-wrap";
    int fileCount = 20;
    int maxFilesPerThread = 5;
    long fileSize = 1024;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());

    String rootPath = OZONE_URI_SCHEME + "://" + bucketName + "." + volumeName;
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    CommandLine cmd = new Freon().getCmd();
    int exitCode = cmd.execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "dfsrw",
        "-n", String.valueOf(fileCount),
        "-t", "1",
        "-s", fileSize + "B",
        "--max-files-per-thread", String.valueOf(maxFilesPerThread),
        "-p", prefix,
        "-r", rootPath
    );
    assertEquals(0, exitCode, "Freon dfsrw command failed");

    // every write still ran and validated, but they landed on wrapped paths
    BaseFreonGenerator subject = (BaseFreonGenerator)
        cmd.getParseResult().subcommand().commandSpec().userObject();
    assertEquals(fileCount, subject.getSuccessCount());

    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf)) {
      FileStatus[] files =
          fileSystem.listStatus(new Path(rootPath + "/" + prefix));
      assertEquals(maxFilesPerThread, files.length,
          "Paths did not wrap at --max-files-per-thread");
    }
  }

  /**
   * A time-based run reuses its paths, so the read-back validates overwritten
   * files.  A count-based run uses each path once and never gets there.
   */
  @ParameterizedTest
  @EnumSource(names = {"FILE_SYSTEM_OPTIMIZED", "LEGACY"})
  public void testValidateOverwrittenPaths(BucketLayout layout) throws Exception {
    String volumeName = "vol-" + UUID.randomUUID();
    String bucketName = "bucket1";
    String prefix = "dfsrw-duration";
    int threads = 2;
    int pathsPerThread = 4;
    long fileSize = 1024;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(layout).build());

    String rootPath = OZONE_URI_SCHEME + "://" + bucketName + "." + volumeName;
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    CommandLine cmd = new Freon().getCmd();
    int exitCode = cmd.execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "dfsrw",
        "--duration", "3s",
        "-n", String.valueOf(pathsPerThread),
        "-t", String.valueOf(threads),
        "-s", fileSize + "B",
        "-p", prefix,
        "-r", rootPath
    );
    assertEquals(0, exitCode, "Freon dfsrw command failed");

    BaseFreonGenerator subject = (BaseFreonGenerator)
        cmd.getParseResult().subcommand().commandSpec().userObject();
    int maxPaths = threads * pathsPerThread;

    // more successful tasks than paths means paths were overwritten, and a
    // successful task is one whose read-back matched
    assertThat(subject.getSuccessCount()).isGreaterThan(maxPaths);

    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf)) {
      FileStatus[] files =
          fileSystem.listStatus(new Path(rootPath + "/" + prefix));
      assertThat(files.length).isLessThanOrEqualTo(maxPaths);
      for (FileStatus file : files) {
        assertEquals(fileSize, file.getLen(),
            "Unexpected file size: " + file.getPath());
      }
    }
  }
}
