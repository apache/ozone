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
package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpSync;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.ozone.test.GenericTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * Testing Distcp with -diff option that uses snapdiff.
 */
@RunWith(Parameterized.class)
public class TestDistcpWithSnapshots {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDistcpWithSnapshots.class);

  @Rule
  public Timeout globalTimeout = Timeout.seconds(300);
  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static BucketLayout bucketLayout;
  private static String rootPath;
  private static boolean enableRatis;

  private static File metaDir;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[] {true, BucketLayout.FILE_SYSTEM_OPTIMIZED},
        new Object[] {false, BucketLayout.LEGACY});
  }

  public TestDistcpWithSnapshots(boolean enableRatis,
      BucketLayout bucketLayout) {
    // do nothing
  }

  @Parameterized.BeforeParam
  public static void initParam(boolean ratisEnable, BucketLayout layout)
      throws IOException, InterruptedException, TimeoutException {
    // Initialize the cluster before EACH set of parameters
    enableRatis = ratisEnable;
    bucketLayout = layout;
    initClusterAndEnv();
  }

  @Parameterized.AfterParam
  public static void teardownParam() {
    // Tear down the cluster after EACH set of parameters
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  public static void initClusterAndEnv()
      throws IOException, InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, enableRatis);
    bucketLayout = BucketLayout.FILE_SYSTEM_OPTIMIZED;
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();

    rootPath = String.format("%s://%s/", OzoneConsts.OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    conf.setClass("distcp.sync.class", OzoneDistcpSync.class,
        DistCpSync.class);
    fs = FileSystem.get(conf);
    metaDir = OMStorage.getOmDbDir(conf);
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDistcpWithSnapDiff() throws Exception {
    Path srcBucketPath = createAndGetBucketPath();
    Path insideSrcBucket = new Path(srcBucketPath, "*");
    Path dstBucketPath = createAndGetBucketPath();
    // create 2 files on source
    createFiles(srcBucketPath, 2);
    // Create target directory/bucket
    fs.mkdirs(dstBucketPath);

    // perform normal distcp
    final DistCpOptions options =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath).build();
    options.appendToConf(conf);
    Job distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 2);

    // take Snapshot snap1 on both source and target
    createSnapshot(srcBucketPath, "snap1");
    createSnapshot(dstBucketPath, "snap1");



    // create another file on src and take snapshot snap2 on source
    createFiles(srcBucketPath, 1);
    Assert.assertEquals(3, fs.listStatus(srcBucketPath).length);
    createSnapshot(srcBucketPath, "snap2");

    // perform distcp providing snapshot snap1.snap2 as arguments to
    // copy only diff b/w them.
    final DistCpOptions options2 =
        new DistCpOptions.Builder(Collections.singletonList(srcBucketPath),
            dstBucketPath).withUseDiff("snap1", "snap2").withSyncFolder(true)
            .build();

    distcpJob = new DistCp(conf, options2).execute();
    verifyCopy(dstBucketPath, distcpJob, 1, 3);
  }

  @NotNull
  private static Path createAndGetBucketPath() throws IOException {
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster, bucketLayout);
    Path volumePath =
        new Path(OZONE_URI_DELIMITER, bucket.getVolumeName());
    Path bucketPath = new Path(volumePath, bucket.getName());
    return bucketPath;
  }

  @Test
  public void testDistcpWithSnapDiff2() throws Exception {
    Path srcBucketPath = createAndGetBucketPath();
    Path insideSrcBucket = new Path(srcBucketPath, "*");
    Path dstBucketPath = createAndGetBucketPath();
    // create 2 files on source
    createFiles(srcBucketPath, 2);
    // Create target directory/bucket
    fs.mkdirs(dstBucketPath);

    // perform normal distcp
    final DistCpOptions options =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath).build();
    options.appendToConf(conf);
    Job distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 2);

    // take Snapshot snap1 on both source and target
    createSnapshot(srcBucketPath, "snap1");
    createSnapshot(dstBucketPath, "snap1");

    // delete 1 file on source
    deleteFiles(srcBucketPath, 1);
    Assert.assertEquals(1, fs.listStatus(srcBucketPath).length);
    createSnapshot(srcBucketPath, "snap2");

    // perform distcp providing snapshot snap1.snap2 as arguments to
    // copy only diff b/w them.
    final DistCpOptions options2 =
        new DistCpOptions.Builder(Collections.singletonList(srcBucketPath),
            dstBucketPath).withUseDiff("snap1", "snap2").withSyncFolder(true)
            .build();

    distcpJob = new DistCp(conf, options2).execute();
    verifyCopy(dstBucketPath, distcpJob, 0, 1);
  }

  @Test
  public void testDistcpWithSnapDiff3() throws Exception {
    Path srcBucketPath = createAndGetBucketPath();
    Path insideSrcBucket = new Path(srcBucketPath, "*");
    Path dstBucketPath = createAndGetBucketPath();
    // create 2 files on source
    createFiles(srcBucketPath, 2);
    // Create target directory/bucket
    fs.mkdirs(dstBucketPath);

    // perform normal distcp
    final DistCpOptions options =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath).build();
    options.appendToConf(conf);
    Job distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 2);

    // take Snapshot snap1 on both source and target
    createSnapshot(srcBucketPath, "snap1");
    createSnapshot(dstBucketPath, "snap1");

    // delete 1 file on source
    renameFiles(srcBucketPath, 1);
    Assert.assertEquals(2, fs.listStatus(srcBucketPath).length);
    createSnapshot(srcBucketPath, "snap2");

    // perform distcp providing snapshot snap1.snap2 as arguments to
    // copy only diff b/w them.
    final DistCpOptions options2 =
        new DistCpOptions.Builder(Collections.singletonList(srcBucketPath),
            dstBucketPath).withUseDiff("snap1", "snap2").withSyncFolder(true)
            .build();

    distcpJob = new DistCp(conf, options2).execute();
    verifyCopy(dstBucketPath, distcpJob, 0, 2);
  }

  private void deleteFiles(Path path, int fileCount) throws IOException {
    FileStatus[] filesInPath = fs.listStatus(path);
    Assert.assertTrue("Cannot delete more files than what is existing",
        fileCount <= filesInPath.length);
    for (int i = 0; i < fileCount; i++) {
      Path toDelete = filesInPath[i].getPath();
      fs.delete(toDelete, false);
    }
  }

  private void renameFiles(Path path, int fileCount) throws IOException {
    FileStatus[] filesInPath = fs.listStatus(path);
    Assert.assertTrue("Cannot delete more files than what is existing",
        fileCount <= filesInPath.length);
    for (int i = 0; i < fileCount; i++) {
      Path from = filesInPath[i].getPath();
      Path to = new Path(from + "_renamed");
      fs.rename(from, to);
    }
  }

  private static void verifyCopy(Path dstBucketPath, Job distcpJob,
      long expectedFilesToBeCopied, long expectedTotalFilesInDest)
      throws IOException {
    long filesCopied =
        distcpJob.getCounters().findCounter(CopyMapper.Counter.COPY).getValue();
    FileStatus[] destinationFileStatus = fs.listStatus(dstBucketPath);
    Assert.assertEquals(expectedTotalFilesInDest, destinationFileStatus.length);
    Assert.assertEquals(expectedFilesToBeCopied, filesCopied);
  }

  private static void createFiles(Path srcBucketPath, int fileCount)
      throws IOException {
    for (int i = 1; i <= fileCount; i++) {
      String keyName = "key" + RandomStringUtils.randomNumeric(5);
      Path file =
          new Path(srcBucketPath, keyName);
      ContractTestUtils.touch(fs, file);
    }
  }

  private Path createSnapshot(Path path, String snapshotName)
      throws IOException, InterruptedException, TimeoutException {
    OFSPath ofsPath = new OFSPath(path, conf);
    String volume = ofsPath.getVolumeName();
    String bucket = ofsPath.getBucketName();
    Path snapPath = fs.createSnapshot(path, snapshotName);
    SnapshotInfo snapshotInfo =
        cluster.getOzoneManager().getMetadataManager().getSnapshotInfoTable()
            .get(SnapshotInfo.getTableKey(volume, bucket, snapshotName));
    String snapshotDirName =
        metaDir + OM_KEY_PREFIX + OM_SNAPSHOT_DIR + OM_KEY_PREFIX
            + OM_DB_NAME + snapshotInfo.getCheckpointDirName() + OM_KEY_PREFIX
            + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(), 1000,
        120000);
    return snapPath;
  }

}
