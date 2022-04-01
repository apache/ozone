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
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
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

/**
 * Test for OmBucketReadWriteOps.
 */
public class TestOmBucketReadWriteOps {

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster = null;
  private ObjectStore store = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteOps.class);

  @Before
  public void setup() {
    path = GenericTestUtils
        .getTempPath(TestOmBucketReadWriteOps.class.getSimpleName());
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() throws IOException {
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

    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @Test
  public void testOmBucketReadWriteOps() throws Exception {
    try {
      startCluster();
      FileOutputStream out = FileUtils.openOutputStream(new File(path,
          "conf"));
      cluster.getConf().writeXml(out);
      out.getFD().sync();
      out.close();

      verifyFileGeneration(new ParameterBuilder());
      verifyFileGeneration(
          new ParameterBuilder().setVolumeName("vol2").setBucketName("bucket1")
              .setPrefixFilePath("/dir1/dir2/dir3"));
      verifyFileGeneration(
          new ParameterBuilder().setVolumeName("vol3").setBucketName("bucket1")
              .setPrefixFilePath("/").setFileCountForRead(1000)
              .setFileCountForWrite(100).setTotalThreadCount(505));
      verifyFileGeneration(
          new ParameterBuilder().setVolumeName("vol4").setBucketName("bucket1")
              .setPrefixFilePath("/dir1/").setFileSizeInBytes(128)
              .setBufferSize(32));
      verifyFileGeneration(
          new ParameterBuilder().setVolumeName("vol5").setBucketName("bucket1")
              .setPrefixFilePath("/dir1/dir2/dir3").setFileCountForRead(250)
              .setNumOfReadOperations(100).setNumOfWriteOperations(0));
      verifyFileGeneration(
          new ParameterBuilder().setVolumeName("vol6").setBucketName("bucket1")
              .setPrefixFilePath("/dir1/dir2/dir3").setNumOfReadOperations(0)
              .setFileCountForWrite(20).setNumOfWriteOperations(100));
    } finally {
      shutdown();
    }
  }

  private void verifyFileGeneration(ParameterBuilder parameterBuilder)
      throws IOException {
    store.createVolume(parameterBuilder.volumeName);
    OzoneVolume volume = store.getVolume(parameterBuilder.volumeName);
    volume.createBucket(parameterBuilder.bucketName);
    String rootPath = "o3fs://" + parameterBuilder.bucketName + "." +
            parameterBuilder.volumeName + parameterBuilder.prefixFilePath;
    String confPath = new File(path, "conf").getAbsolutePath();
    new Freon().execute(
        new String[]{"-conf", confPath, "obrwo", "-P", rootPath,
            "-r", String.valueOf(parameterBuilder.fileCountForRead),
            "-w", String.valueOf(parameterBuilder.fileCountForWrite),
            "-g", String.valueOf(parameterBuilder.fileSizeInBytes),
            "-b", String.valueOf(parameterBuilder.bufferSize),
            "-l", String.valueOf(parameterBuilder.length),
            "-c", String.valueOf(parameterBuilder.totalThreadCount),
            "-T", String.valueOf(parameterBuilder.readThreadPercentage),
            "-R", String.valueOf(parameterBuilder.numOfReadOperations),
            "-W", String.valueOf(parameterBuilder.numOfWriteOperations),
            "-n", String.valueOf(1)});

    LOG.info("Started verifying OM bucket read/write ops file generation...");
    FileSystem fileSystem = FileSystem.get(URI.create(rootPath),
        conf);
    Path rootDir = new Path(rootPath.concat(OzoneConsts.OM_KEY_PREFIX));
    FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
    verifyUtil(2, fileStatuses, true);

    Path readDir = new Path(rootPath.concat("/readPath"));
    FileStatus[] readFileStatuses = fileSystem.listStatus(readDir);
    verifyUtil(parameterBuilder.fileCountForRead, readFileStatuses, false);

    Path writeDir = new Path(rootPath.concat("/writePath"));
    FileStatus[] writeFileStatuses = fileSystem.listStatus(writeDir);
    verifyUtil(parameterBuilder.fileCountForWrite *
        parameterBuilder.numOfWriteOperations, writeFileStatuses, false);
  }

  private void verifyUtil(int expectedCount, FileStatus[] fileStatuses,
                          boolean checkDir) {
    int actual = 0;
    if (checkDir) {
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
    Assert.assertEquals("Mismatch Count!", expectedCount, actual);
  }

  private class ParameterBuilder {

    private String volumeName = "vol1";
    private String bucketName = "bucket1";
    private String prefixFilePath = "/dir1/dir2";
    private int fileCountForRead = 100;
    private int fileCountForWrite = 10;
    private long fileSizeInBytes = 256;
    private int bufferSize = 64;
    private int length = 10;
    private int totalThreadCount = 100;
    private int readThreadPercentage = 90;
    private int numOfReadOperations = 50;
    private int numOfWriteOperations = 10;

    private ParameterBuilder setVolumeName(String volumeNameParam) {
      volumeName = volumeNameParam;
      return this;
    }

    private ParameterBuilder setBucketName(String bucketNameParam) {
      bucketName = bucketNameParam;
      return this;
    }

    private ParameterBuilder setPrefixFilePath(String prefixFilePathParam) {
      prefixFilePath = prefixFilePathParam;
      return this;
    }

    private ParameterBuilder setFileCountForRead(int fileCountForReadParam) {
      fileCountForRead = fileCountForReadParam;
      return this;
    }

    private ParameterBuilder setFileCountForWrite(int fileCountForWriteParam) {
      fileCountForWrite = fileCountForWriteParam;
      return this;
    }

    private ParameterBuilder setFileSizeInBytes(long fileSizeInBytesParam) {
      fileSizeInBytes = fileSizeInBytesParam;
      return this;
    }

    private ParameterBuilder setBufferSize(int bufferSizeParam) {
      bufferSize = bufferSizeParam;
      return this;
    }

    private ParameterBuilder setLength(int lengthParam) {
      length = lengthParam;
      return this;
    }

    private ParameterBuilder setTotalThreadCount(int totalThreadCountParam) {
      totalThreadCount = totalThreadCountParam;
      return this;
    }

    private ParameterBuilder setReadThreadPercentage(
        int readThreadPercentageParam) {
      readThreadPercentage = readThreadPercentageParam;
      return this;
    }

    private ParameterBuilder setNumOfReadOperations(
        int numOfReadOperationsParam) {
      numOfReadOperations = numOfReadOperationsParam;
      return this;
    }

    private ParameterBuilder setNumOfWriteOperations(
        int numOfWriteOperationsParam) {
      numOfWriteOperations = numOfWriteOperationsParam;
      return this;
    }
  }
}
