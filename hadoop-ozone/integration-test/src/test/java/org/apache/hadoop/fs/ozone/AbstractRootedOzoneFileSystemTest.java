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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_ACLS;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_CHECKSUMS;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertHasPathCapabilities;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.security.UserGroupInformation.createUserForTesting;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone file system tests that are not covered by contract tests.
 * TODO: Refactor this and TestOzoneFileSystem to reduce duplication.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractRootedOzoneFileSystemTest extends OzoneFileSystemTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRootedOzoneFileSystemTest.class);

  private static final float TRASH_INTERVAL = 0.05f; // 3 seconds

  private static final String USER1 = "regularuser1";
  private static final UserGroupInformation UGI_USER1 = createUserForTesting(USER1,  new String[] {"usergroup"});

  private OzoneClient client;

  private final boolean enabledFileSystemPaths;
  private final boolean isBucketFSOptimized;
  private final boolean enableAcl;

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private RootedOzoneFileSystem fs;
  private ObjectStore objectStore;
  private BasicRootedOzoneClientAdapterImpl adapter;
  private Trash trash;

  private String volumeName;
  private Path volumePath;
  private String bucketName;
  // Store path commonly used by tests that test functionality within a bucket
  private Path bucketPath;
  private String rootPath;
  private final BucketLayout bucketLayout;

  // Non-privileged OFS instance
  private RootedOzoneFileSystem userOfs;

  AbstractRootedOzoneFileSystemTest(BucketLayout bucketLayout, boolean setDefaultFs,
      boolean isAclEnabled) {
    // Initialize the cluster before EACH set of parameters
    this.bucketLayout = bucketLayout;
    enabledFileSystemPaths = setDefaultFs;
    enableAcl = isAclEnabled;
    isBucketFSOptimized = bucketLayout.isFileSystemOptimized();
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(fs, userOfs, client);
    // Tear down the cluster after EACH set of parameters
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  void createVolumeAndBucket() throws IOException {
    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    volumeName = bucket.getVolumeName();
    volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);
  }

  @AfterEach
  void cleanup() throws IOException {
    fs.delete(bucketPath, true);
    fs.delete(volumePath, false);
  }

  @Override
  public RootedOzoneFileSystem getFs() {
    return fs;
  }

  public Path getBucketPath() {
    return bucketPath;
  }

  @BeforeAll
  void initClusterAndEnv() throws IOException, InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    conf.setFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY, TRASH_INTERVAL / 2);
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    if (bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          bucketLayout.name());
    } else {
      conf.set(OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT,
          OzoneConfigKeys.OZONE_BUCKET_LAYOUT_LEGACY);
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          bucketLayout.name());
      conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
          enabledFileSystemPaths);
    }
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, enableAcl);
    // Set ACL authorizer class to OzoneNativeAuthorizer. The default
    // OzoneAccessAuthorizer always returns true for all ACL checks which
    // doesn't work for the test.
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    objectStore = client.getObjectStore();

    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    conf.setInt(OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE, 4);
    // fs.ofs.impl would be loaded from META-INF, no need to manually set it
    trash = new Trash(conf);
    fs = (RootedOzoneFileSystem) FileSystem.get(conf);
    adapter = (BasicRootedOzoneClientAdapterImpl) fs.getAdapter();

    userOfs = UGI_USER1.doAs(
        (PrivilegedExceptionAction<RootedOzoneFileSystem>)()
            -> (RootedOzoneFileSystem) FileSystem.get(conf));
  }

  protected OMMetrics getOMMetrics() {
    return cluster.getOzoneManager().getMetrics();
  }

  @Test
  void testUserHomeDirectory() {
    assertEquals(new Path(rootPath + "user/" + USER1),
        userOfs.getHomeDirectory());
  }

  @Test
  void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path(bucketPath,
        "testCreateDoesNotAddParentDirKeys");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    OzoneKeyDetails key = getKey(child, false);
    OFSPath childOFSPath = new OFSPath(child, conf);
    assertEquals(key.getName(), childOFSPath.getKeyName());

    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (OMException ome) {
      assertEquals(KEY_NOT_FOUND, ome.getResult());
    }

    // List status on the parent should show the child file
    assertEquals(1L, fs.listStatus(parent).length, "List status of parent should include the 1 child file");
    assertTrue(fs.getFileStatus(parent).isDirectory(), "Parent directory does not appear to be a directory");

    // Cleanup
    fs.delete(grandparent, true);
  }

  @Test
  public void testCreateKeyWithECReplicationConfig() throws Exception {
    createKeyWithECReplicationConfig(bucketPath, cluster.getConf());
  }

  @Test
  void testListStatusWithIntermediateDirWithECEnabled()
          throws Exception {
    String key = "object-dir/object-name1";

    // write some test data into bucket
    TestDataUtil.createKey(objectStore.getVolume(volumeName).getBucket(bucketName),
        key, new ECReplicationConfig("RS-3-2-1024k"),
        RandomUtils.secure().randomBytes(1));

    List<String> dirs = Arrays.asList(volumeName, bucketName, "object-dir",
            "object-name1");
    for (int size = 1; size <= dirs.size(); size++) {
      String path = "/" + dirs.subList(0, size).stream()
              .collect(Collectors.joining("/"));
      Path parent = new Path(path);
      // Wait until the filestatus is updated
      if (!enabledFileSystemPaths) {
        GenericTestUtils.waitFor(() -> {
          try {
            fs.getFileStatus(parent);
            return true;
          } catch (IOException e) {
            return false;
          }
        }, 1000, 120000);
      }
      FileStatus fileStatus = fs.getFileStatus(parent);
      assertEquals((size == dirs.size() - 1 &&
           !bucketLayout.isFileSystemOptimized()) || size == dirs.size(), fileStatus.isErasureCoded());
    }

  }

  @Test
  void testDeleteCreatesFakeParentDir() throws Exception {
    // TODO: Request for comment.
    //  If possible, improve this to test when FS Path is enabled.
    assumeTrue(!enabledFileSystemPaths, "FS Path is enabled. Skipping this test as it is not " +
            "tuned for FS Path yet");

    Path grandparent = new Path(bucketPath,
        "testDeleteCreatesFakeParentDir");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    // Verify that parent dir key does not exist
    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (OMException ome) {
      assertEquals(KEY_NOT_FOUND, ome.getResult());
    }


    // Delete the child key
    assertTrue(fs.delete(child, false));

    // Deleting the only child should create the parent dir key if it does
    // not exist
    OFSPath parentOFSPath = new OFSPath(parent, conf);
    String parentKey = parentOFSPath.getKeyName() + "/";
    OzoneKeyDetails parentKeyInfo = getKey(parent, true);
    assertEquals(parentKey, parentKeyInfo.getName());

    // Recursive delete with DeleteIterator
    assertTrue(fs.delete(grandparent, true));
  }

  @Test
  void testListLocatedStatusForZeroByteFile() throws Exception {
    Path parent = new Path(bucketPath, "testListLocatedStatusForZeroByteFile");
    Path path = new Path(parent, "key1");

    try {
      listLocatedStatusForZeroByteFile(fs, path);
    } finally {
      // Cleanup
      fs.delete(parent, true);
    }
  }

  @Test
  void testListStatus() throws Exception {
    Path parent = new Path(bucketPath, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");

    FileStatus[] fileStatuses = fs.listStatus(bucketPath);
    assertEquals(0, fileStatuses.length, "Should be empty");

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    fileStatuses = fs.listStatus(bucketPath);
    assertEquals(1, fileStatuses.length, "Should have created parent");
    assertEquals(fileStatuses[0].getPath().toUri().getPath(), parent.toString(), "Parent path doesn't match");

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    fileStatuses = fs.listStatus(parent);
    assertEquals(2, fileStatuses.length, "FileStatus did not return all children of the directory");

    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);
    fileStatuses = fs.listStatus(parent);
    assertEquals(3, fileStatuses.length, "FileStatus did not return all children of" +
        " the directory : Got " + Arrays.toString(
        fileStatuses));

    // Cleanup
    fs.delete(parent, true);
  }

  /**
   * Tests listStatusIterator operation on a directory.
   */
  @Test
  void testListStatusIteratorWithDir() throws Exception {
    listStatusIteratorWithDir(bucketPath);
  }

  /**
   * Test listStatusIterator operation in a bucket.
   */
  @Test
  void testListStatusIteratorInBucket() throws Exception {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    try {
      fs.mkdirs(dir12);
      fs.mkdirs(dir2);

      // ListStatus on root should return dir1 (even though /dir1 key does not
      // exist) and dir2 only. dir12 is not an immediate child of root and
      // hence should not be listed.
      RemoteIterator<FileStatus> it = fs.listStatusIterator(root);
      // Verify that dir12 is not included in the result of the listStatus on
      // root
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
        assertNotEquals(fileStatus, dir12.toString());
      }
      assertEquals(2, iCount, "FileStatus should return only the immediate children");

    } finally {
      // cleanup
      fs.delete(dir1, true);
      fs.delete(dir2, true);
    }
  }

  @Test
  void testListStatusIteratorWithPathNotFound() throws Exception {
    Path path = new Path("/test/test1/test2");
    try {
      fs.listStatusIterator(path);
      fail("Should have thrown OMException");
    } catch (OMException omEx) {
      assertEquals(VOLUME_NOT_FOUND, omEx.getResult(), "Volume test is not found");
    }
  }

  /**
   * Tests listStatusIterator operation on root directory with different
   * numbers of numDir.
   */
  @Test
  void testListStatusIteratorOnPageSize() throws Exception {
    listStatusIteratorOnPageSize(conf,
        "/" + volumeName + "/" + bucketName);
  }

  /**
   * Tests listStatusIterator on a path with subdirs.
   */
  @Test
  void testListStatusIteratorOnSubDirs() throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // listStatusIterator on /dir1 should return all its immediated subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatusIterator.
    Path dir1 = new Path(bucketPath, "dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path(bucketPath, "dir2");
    try {
      fs.mkdirs(dir111);
      fs.mkdirs(dir12);
      ContractTestUtils.touch(fs, file121);
      fs.mkdirs(dir2);

      RemoteIterator<FileStatus> it = fs.listStatusIterator(dir1);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
        assertNotEquals(fileStatus, dir12.toString());
        // Verify that the two children of /dir1
        // returned by listStatusIterator operation
        // are /dir1/dir11 and /dir1/dir12.
        assertTrue(fileStatus.getPath().toUri().getPath().
            equals(dir11.toString()) ||
            fileStatus.getPath().toUri().getPath().
                equals(dir12.toString()));
      }
      assertEquals(2, iCount, "Iterator should return only the immediate children");
    } finally {
      // Cleanup
      fs.delete(dir2, true);
      fs.delete(dir1, true);
    }
  }

  /**
   * OFS: Helper function for tests. Return a volume name that doesn't exist.
   */
  protected String getRandomNonExistVolumeName() throws IOException {
    final int numDigit = 5;
    long retriesLeft = Math.round(Math.pow(10, 5));
    String name = null;
    while (name == null && retriesLeft-- > 0) {
      name = "volume-" + RandomStringUtils.secure().nextNumeric(numDigit);
      // Check volume existence.
      Iterator<? extends OzoneVolume> iter =
          objectStore.listVolumesByUser(null, name, null);
      if (iter.hasNext()) {
        // If there is a match, try again.
        // Note that volume name prefix match doesn't equal volume existence
        //  but the check is sufficient for this test.
        name = null;
      }
    }
    if (retriesLeft <= 0) {
      fail("Failed to generate random volume name that doesn't exist already.");
    }
    return name;
  }

  /**
   * OFS: Test mkdir on volume, bucket and dir that doesn't exist.
   */
  @Test
  void testMkdirOnNonExistentVolumeBucketDir() throws Exception {
    // TODO: Request for comment.
    //  If possible, improve this to test when FS Path is enabled.
    assumeTrue(!enabledFileSystemPaths, "FS Path is enabled. Skipping this test as it is not " +
            "tuned for FS Path yet");

    String volumeNameLocal = getRandomNonExistVolumeName();
    String bucketNameLocal = "bucket-" + RandomStringUtils.secure().nextNumeric(5);
    Path root = new Path("/" + volumeNameLocal + "/" + bucketNameLocal);
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // Check volume and bucket existence, they should both be created.
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeNameLocal);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketNameLocal);
    OFSPath ofsPathDir1 = new OFSPath(dir12, conf);
    String key = ofsPathDir1.getKeyName() + "/";
    OzoneKeyDetails ozoneKeyDetails = ozoneBucket.getKey(key);
    assertEquals(key, ozoneKeyDetails.getName());

    // Verify that directories are created.
    FileStatus[] fileStatuses = fs.listStatus(root);
    assertEquals(fileStatuses[0].getPath().toUri().getPath(), dir1.toString());
    assertEquals(fileStatuses[1].getPath().toUri().getPath(), dir2.toString());

    fileStatuses = fs.listStatus(dir1);
    assertEquals(fileStatuses[0].getPath().toUri().getPath(), dir12.toString());
    fileStatuses = fs.listStatus(dir12);
    assertEquals(fileStatuses.length, 0);
    fileStatuses = fs.listStatus(dir2);
    assertEquals(fileStatuses.length, 0);

    // Cleanup
    fs.delete(dir2, true);
    fs.delete(dir1, true);
    ozoneVolume.deleteBucket(bucketNameLocal);
    objectStore.deleteVolume(volumeNameLocal);
  }

  /**
   * OFS: Test mkdir on a volume and bucket that doesn't exist.
   */
  @Test
  void testMkdirNonExistentVolumeBucket() throws Exception {
    String volumeNameLocal = getRandomNonExistVolumeName();
    String bucketNameLocal = "bucket-" + RandomStringUtils.secure().nextNumeric(5);
    Path newVolBucket = new Path(
        "/" + volumeNameLocal + "/" + bucketNameLocal);
    fs.mkdirs(newVolBucket);

    // Verify with listVolumes and listBuckets
    Iterator<? extends OzoneVolume> iterVol =
        objectStore.listVolumesByUser(null, volumeNameLocal, null);
    OzoneVolume ozoneVolume = iterVol.next();
    assertNotNull(ozoneVolume);
    assertEquals(volumeNameLocal, ozoneVolume.getName());

    Iterator<? extends OzoneBucket> iterBuc =
        ozoneVolume.listBuckets("bucket-");
    OzoneBucket ozoneBucket = iterBuc.next();
    assertNotNull(ozoneBucket);
    assertEquals(bucketNameLocal, ozoneBucket.getName());
    assertEquals(bucketLayout, ozoneBucket.getBucketLayout());
    // TODO: Use listStatus to check volume and bucket creation in HDDS-2928.

    // Cleanup
    ozoneVolume.deleteBucket(bucketNameLocal);
    objectStore.deleteVolume(volumeNameLocal);
  }

  /**
   * OFS: Test mkdir on a volume that doesn't exist.
   */
  @Test
  void testMkdirNonExistentVolume() throws Exception {
    assumeFalse(isBucketFSOptimized);

    String volumeNameLocal = getRandomNonExistVolumeName();
    Path newVolume = new Path("/" + volumeNameLocal);
    fs.mkdirs(newVolume);

    // Verify with listVolumes and listBuckets
    Iterator<? extends OzoneVolume> iterVol =
        objectStore.listVolumesByUser(null, volumeNameLocal, null);
    OzoneVolume ozoneVolume = iterVol.next();
    assertNotNull(ozoneVolume);
    assertEquals(volumeNameLocal, ozoneVolume.getName());

    // TODO: Use listStatus to check volume and bucket creation in HDDS-2928.

    // Cleanup
    objectStore.deleteVolume(volumeNameLocal);
  }

  /**
   * OFS: Test getFileStatus on root.
   */
  @Test
  void testGetFileStatusRoot() throws Exception {
    Path root = new Path("/");
    FileStatus fileStatus = fs.getFileStatus(root);
    assertNotNull(fileStatus);
    assertEquals(new Path(rootPath), fileStatus.getPath());
    assertTrue(fileStatus.isDirectory());
    assertEquals(FsPermission.getDirDefault(), fileStatus.getPermission());
  }

  /**
   * Test listStatus operation in a bucket.
   */
  @Test
  void testListStatusInBucket() throws Exception {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    try {
      fs.mkdirs(dir12);
      fs.mkdirs(dir2);

      // ListStatus on root should return dir1 (even though /dir1 key does not
      // exist) and dir2 only. dir12 is not an immediate child of root and
      // hence should not be listed.
      FileStatus[] fileStatuses = fs.listStatus(root);
      assertEquals(2, fileStatuses.length, "FileStatus should return only the immediate children");

      // Verify that dir12 is not included in the result of the listStatus on
      // root
      String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
      String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
      assertNotEquals(fileStatus1, dir12.toString());
      assertNotEquals(fileStatus2, dir12.toString());
    } finally {
      // cleanup
      fs.delete(dir1, true);
      fs.delete(dir2, true);
    }
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  void testListStatusOnLargeDirectory() throws Exception {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    try {
      for (int i = 0; i < numDirs; i++) {
        Path p = new Path(root, String.valueOf(i));
        fs.mkdirs(p);
        paths.add(p.getName());
      }

      FileStatus[] fileStatuses = fs.listStatus(root);
      assertEquals(numDirs, fileStatuses.length, "Total directories listed do not match the existing directories");

      for (int i = 0; i < numDirs; i++) {
        assertThat(paths).contains(fileStatuses[i].getPath().getName());
      }
    } finally {
      // Cleanup
      for (int i = 0; i < numDirs; i++) {
        Path p = new Path(root, String.valueOf(i));
        fs.delete(p, true);
      }
    }
  }

  /**
   * Tests listStatus on a path with subdirs.
   */
  @Test
  void testListStatusOnSubDirs() throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // ListStatus on /dir1 should return all its immediated subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatus.
    Path dir1 = new Path(bucketPath, "dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path(bucketPath, "dir2");
    fs.mkdirs(dir111);
    fs.mkdirs(dir12);
    ContractTestUtils.touch(fs, file121);
    fs.mkdirs(dir2);

    FileStatus[] fileStatuses = fs.listStatus(dir1);
    assertEquals(2, fileStatuses.length, "FileStatus should return only the immediate children");

    // Verify that the two children of /dir1 returned by listStatus operation
    // are /dir1/dir11 and /dir1/dir12.
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertTrue(fileStatus1.equals(dir11.toString()) ||
        fileStatus1.equals(dir12.toString()));
    assertTrue(fileStatus2.equals(dir11.toString()) ||
        fileStatus2.equals(dir12.toString()));

    // Cleanup
    fs.delete(dir2, true);
    fs.delete(dir1, true);
  }

  @Test
  void testNonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved()
      throws Exception {
    Path source = new Path(bucketPath, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(bucketPath, "target");
    Path leafInTarget = new Path(target, "leaf");

    fs.mkdirs(source);
    fs.mkdirs(target);
    fs.mkdirs(leafInsideInterimPath);

    assertTrue(fs.rename(leafInsideInterimPath, leafInTarget));

    // after rename listStatus for interimPath should succeed and
    // interimPath should have no children
    FileStatus[] statuses = fs.listStatus(interimPath);
    assertNotNull(statuses, "liststatus returns a null array");
    assertEquals(0, statuses.length, "Statuses array is not empty");
    FileStatus fileStatus = fs.getFileStatus(interimPath);
    assertEquals(interimPath.getName(), fileStatus.getPath().getName(), "FileStatus does not point to interimPath");

    // Cleanup
    fs.delete(target, true);
    fs.delete(source, true);
  }

  /**
   * OFS: Try to rename a key to a different bucket. The attempt should fail.
   */
  @Test
  void testRenameToDifferentBucket() throws IOException {
    Path source = new Path(bucketPath, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(bucketPath, "target");

    fs.mkdirs(source);
    fs.mkdirs(target);
    fs.mkdirs(leafInsideInterimPath);

    // Attempt to rename the key to a different bucket
    Path bucket2 = new Path(OZONE_URI_DELIMITER + volumeName +
        OZONE_URI_DELIMITER + bucketName + "test");
    Path leafInTargetInAnotherBucket = new Path(bucket2, "leaf");
    try {
      fs.rename(leafInsideInterimPath, leafInTargetInAnotherBucket);
      fail("Should have thrown exception when renaming to a different bucket");
    } catch (IOException ignored) {
      // Test passed. Exception thrown as expected.
    }

    // Cleanup
    fs.delete(target, true);
    fs.delete(source, true);
  }

  @Override
  protected OzoneKeyDetails getKey(Path keyPath, boolean isDirectory)
      throws IOException {
    String key = fs.pathToKey(keyPath);
    if (isDirectory) {
      key = key + OZONE_URI_DELIMITER;
    }
    OFSPath ofsPath = new OFSPath(key, conf);
    String keyInBucket = ofsPath.getKeyName();
    return client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).getKey(keyInBucket);
  }

  /**
   * Helper function for testListStatusRootAndVolume*.
   * Each call creates one volume, one bucket under that volume,
   * two dir under that bucket, one subdir under one of the dirs,
   * and one file under the subdir.
   */
  private Path createRandomVolumeBucketWithDirs() throws IOException {
    String volume1 = getRandomNonExistVolumeName();
    String bucket1 = "bucket-" + RandomStringUtils.secure().nextNumeric(5);
    Path bucketPath1 = new Path(OZONE_URI_DELIMITER + volume1 +
        OZONE_URI_DELIMITER + bucket1);

    Path dir1 = new Path(bucketPath1, "dir1");
    fs.mkdirs(dir1);  // Intentionally creating this "in-the-middle" dir key
    Path subdir1 = new Path(dir1, "subdir1");
    fs.mkdirs(subdir1);
    Path dir2 = new Path(bucketPath1, "dir2");
    fs.mkdirs(dir2);

    try (FSDataOutputStream stream =
        fs.create(new Path(dir2, "file1"))) {
      stream.write(1);
    }

    return bucketPath1;
  }

  private void teardownVolumeBucketWithDir(Path bucketPath1)
      throws IOException {
    fs.delete(new Path(bucketPath1, "dir1"), true);
    fs.delete(new Path(bucketPath1, "dir2"), true);
    OFSPath ofsPath = new OFSPath(bucketPath1, conf);
    OzoneVolume volume = objectStore.getVolume(ofsPath.getVolumeName());
    volume.deleteBucket(ofsPath.getBucketName());
    objectStore.deleteVolume(ofsPath.getVolumeName());
  }

  /**
   * Create a bucket with a different owner than the volume owner
   * and test the owner on listStatus.
   */
  @Test
  void testListStatusWithDifferentBucketOwner() throws IOException {
    String volName = getRandomNonExistVolumeName();
    objectStore.createVolume(volName);
    OzoneVolume ozoneVolume = objectStore.getVolume(volName);

    String buckName = "bucket-" + RandomStringUtils.secure().nextNumeric(5);
    UserGroupInformation currUgi = UserGroupInformation.getCurrentUser();
    String bucketOwner = currUgi.getUserName() + RandomStringUtils.secure().nextNumeric(5);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setOwner(bucketOwner)
        .build();
    ozoneVolume.createBucket(buckName, bucketArgs);

    Path volPath = new Path(OZONE_URI_DELIMITER + volName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(buckName);

    FileStatus[] fileStatusVolume = fs.listStatus(volPath);
    assertEquals(1, fileStatusVolume.length);
    // FileStatus owner is different from the volume owner.
    // Owner is the same as the bucket owner returned by the ObjectStore.
    assertNotEquals(ozoneVolume.getOwner(), fileStatusVolume[0].getOwner());
    assertEquals(ozoneBucket.getOwner(), fileStatusVolume[0].getOwner());

    ozoneVolume.deleteBucket(buckName);
    objectStore.deleteVolume(volName);
  }

  /**
   * OFS: Test non-recursive listStatus on root and volume.
   */
  @Test
  void testListStatusRootAndVolumeNonRecursive() throws Exception {
    // Get owner and group of the user running this test
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final String ownerShort = ugi.getShortUserName();
    final String group = ugi.getPrimaryGroupName();

    Path bucketPath1 = createRandomVolumeBucketWithDirs();
    Path bucketPath2 = createRandomVolumeBucketWithDirs();
    // listStatus("/volume/bucket")
    FileStatus[] fileStatusBucket = fs.listStatus(bucketPath1);
    assertEquals(2, fileStatusBucket.length);
    // listStatus("/volume")
    Path volume = new Path(
        OZONE_URI_DELIMITER + new OFSPath(bucketPath1, conf).getVolumeName());
    FileStatus[] fileStatusVolume = fs.listStatus(volume);
    assertEquals(1, fileStatusVolume.length);
    assertEquals(ownerShort, fileStatusVolume[0].getOwner());
    assertEquals(group, fileStatusVolume[0].getGroup());

    // listStatus("/")
    Path root = new Path(OZONE_URI_DELIMITER);
    FileStatus[] fileStatusRoot = fs.listStatus(root);

    // When ACL is enabled, listStatus root will see a 4th volume created by
    //  userXXXXX as the result of createVolumeAndBucket in initClusterAndEnv.
    // This is due to the difference in behavior in listVolumesByUser depending
    //  on whether ACL is enabled or not:
    // 1. when ACL is disabled, listVolumesByUser would only return volumes
    //  OWNED by the current user (volume owner is the current user);
    // 2. when ACL is enabled, it would return all the volumes that the current
    //  user has LIST permission to, regardless of the volume owner field.

    if (!enableAcl) {
      // When ACL is disabled, ofs.listStatus(root) will see 2+1 = 3 volumes,
      // the +1 is the default volume "s3v" created by OM during start up.
      assertEquals(2 + 1, fileStatusRoot.length);
      for (FileStatus fileStatus : fileStatusRoot) {
        assertEquals(ownerShort, fileStatus.getOwner());
        assertEquals(group, fileStatus.getGroup());
      }
    } else {
      assertEquals(2 + 1 + 1, fileStatusRoot.length);
    }

    // Cleanup
    teardownVolumeBucketWithDir(bucketPath2);
    teardownVolumeBucketWithDir(bucketPath1);
  }

  /**
   * Helper function to do FileSystem#listStatus recursively.
   * Simulate what FsShell does, using DFS.
   */
  private void listStatusRecursiveHelper(Path curPath, List<FileStatus> result)
      throws IOException {
    FileStatus[] startList = fs.listStatus(curPath);
    for (FileStatus fileStatus : startList) {
      result.add(fileStatus);
      if (fileStatus.isDirectory()) {
        Path nextPath = fileStatus.getPath();
        listStatusRecursiveHelper(nextPath, result);
      }
    }
  }

  /**
   * Helper function to call listStatus in adapter implementation.
   */
  private List<FileStatus> callAdapterListStatus(String pathStr,
      boolean recursive, String startPath, long numEntries) throws IOException {
    return adapter.listStatus(pathStr, recursive, startPath, numEntries,
        fs.getUri(), fs.getWorkingDirectory(), fs.getUsername(), false)
        .stream().map(fs::convertFileStatus).collect(Collectors.toList());
  }

  /**
   * Helper function to compare recursive listStatus results from adapter
   * and (simulated) FileSystem.
   */
  private void listStatusCheckHelper(Path path) throws IOException {
    // Get recursive listStatus result directly from adapter impl
    List<FileStatus> statusesFromAdapter = callAdapterListStatus(
        path.toString(), true, "", 1000);
    // Get recursive listStatus result with FileSystem API by simulating FsShell
    List<FileStatus> statusesFromFS = new ArrayList<>();
    listStatusRecursiveHelper(path, statusesFromFS);
    // Compare. The results would be in the same order due to assumptions:
    // 1. They are both using DFS internally;
    // 2. They both return ordered results.
    assertEquals(statusesFromAdapter.size(), statusesFromFS.size());
    final int n = statusesFromFS.size();
    for (int i = 0; i < n; i++) {
      FileStatus statusFromAdapter = statusesFromAdapter.get(i);
      FileStatus statusFromFS = statusesFromFS.get(i);
      assertEquals(statusFromAdapter.getPath(), statusFromFS.getPath());
      assertEquals(statusFromAdapter.getLen(), statusFromFS.getLen());
      assertEquals(statusFromAdapter.isDirectory(), statusFromFS.isDirectory());
      assertEquals(statusFromAdapter.getModificationTime(), statusFromFS.getModificationTime());
    }
  }

  /**
   * OFS: Test recursive listStatus on root and volume.
   */
  @Test
  void testListStatusRootAndVolumeRecursive() throws IOException {
    assumeFalse(isBucketFSOptimized);

    Path bucketPath1 = createRandomVolumeBucketWithDirs();
    Path bucketPath2 = createRandomVolumeBucketWithDirs();
    // listStatus("/volume/bucket")
    listStatusCheckHelper(bucketPath1);
    // listStatus("/volume")
    Path volume = new Path(
        OZONE_URI_DELIMITER + new OFSPath(bucketPath1, conf).getVolumeName());
    listStatusCheckHelper(volume);
    // listStatus("/")
    Path root = new Path(OZONE_URI_DELIMITER);
    listStatusCheckHelper(root);
    // Cleanup
    teardownVolumeBucketWithDir(bucketPath2);
    teardownVolumeBucketWithDir(bucketPath1);
  }

  /**
   * Helper function. FileSystem#listStatus on steroid:
   * Supports recursion, start path and custom listing page size (numEntries).
   * @param f Given path
   * @param recursive List contents inside subdirectories
   * @param startPath Starting path of the batch
   * @param numEntries Max number of entries in result
   * @return Array of the statuses of the files/directories in the given path
   * @throws IOException See specific implementation
   */
  private FileStatus[] customListStatus(Path f, boolean recursive,
      String startPath, int numEntries) throws IOException {
    assertThat(numEntries).isGreaterThan(0);
    LinkedList<FileStatus> statuses = new LinkedList<>();
    List<FileStatus> tmpStatusList;
    do {
      tmpStatusList = callAdapterListStatus(f.toString(), recursive,
          startPath, numEntries - statuses.size());
      if (!tmpStatusList.isEmpty()) {
        statuses.addAll(tmpStatusList);
        startPath = statuses.getLast().getPath().toString();
      }
    } while (tmpStatusList.size() == numEntries &&
        statuses.size() < numEntries);
    return statuses.toArray(new FileStatus[0]);
  }

  @Test
  void testListStatusRootAndVolumeContinuation() throws IOException {
    // TODO: Request for comment.
    //  If possible, improve this to test when FS Path is enabled.
    assumeTrue(!enabledFileSystemPaths, "FS Path is enabled. Skipping this test as it is not " +
            "tuned for FS Path yet");

    Path[] paths = new Path[5];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = createRandomVolumeBucketWithDirs();
    }
    // Similar to recursive option, we can't test continuation directly with
    // FileSystem because we can't change LISTING_PAGE_SIZE. Use adapter instead

    // numEntries > 5
    FileStatus[] fileStatusesOver = customListStatus(new Path("/"),
        false, "", 8);
    // There are only 5 volumes
    // Default volume "s3v" is created during startup.
    assertEquals(5 + 1, fileStatusesOver.length);

    // numEntries = 5
    FileStatus[] fileStatusesExact = customListStatus(new Path("/"),
        false, "", 5);
    assertEquals(5, fileStatusesExact.length);

    // numEntries < 5
    FileStatus[] fileStatusesLimit1 = customListStatus(new Path("/"),
        false, "", 3);
    // Should only return 3 volumes even though there are more than that due to
    // the specified limit
    assertEquals(3, fileStatusesLimit1.length);

    // Get the last entry in the list as startPath
    String nextStartPath =
        fileStatusesLimit1[fileStatusesLimit1.length - 1].getPath().toString();
    FileStatus[] fileStatusesLimit2 = customListStatus(new Path("/"),
        false, nextStartPath, 3);
    // Note: at the time of writing this test, OmMetadataManagerImpl#listVolumes
    //  excludes startVolume (startPath) from the result. Might change.
    assertEquals(fileStatusesOver.length, fileStatusesLimit1.length + fileStatusesLimit2.length);

    // Cleanup
    for (Path path : paths) {
      teardownVolumeBucketWithDir(path);
    }
  }

  @Test
  void testSharedTmpDir() throws IOException {
    // Prep
    conf.setBoolean(OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR, true);
    // Use ClientProtocol to pass in volume ACL, ObjectStore won't do it
    ClientProtocol proxy = objectStore.getClientProxy();
    // Get default acl rights for user
    OmConfig omConfig = cluster.getOzoneManager().getConfig();
    // Construct ACL for world access
    // ACL admin owner, world read+write
    EnumSet<ACLType> aclRights = EnumSet.of(READ, WRITE);
    // volume acls have all access to admin and read+write access to world

    // Construct VolumeArgs
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setAdmin("admin")
        .setOwner("admin")
        .addAcl(OzoneAcl.of(ACLIdentityType.WORLD, "", ACCESS, aclRights))
        .addAcl(OzoneAcl.of(ACLIdentityType.USER, "admin", ACCESS, omConfig.getUserDefaultRights()))
        .setQuotaInNamespace(1000)
        .setQuotaInBytes(Long.MAX_VALUE).build();
    // Sanity check
    assertEquals("admin", volumeArgs.getOwner());
    assertEquals("admin", volumeArgs.getAdmin());
    assertEquals(Long.MAX_VALUE, volumeArgs.getQuotaInBytes());
    assertEquals(1000, volumeArgs.getQuotaInNamespace());
    assertEquals(0, volumeArgs.getMetadata().size());
    assertEquals(2, volumeArgs.getAcls().size());
    // Create volume "tmp" with world access read+write to access tmp mount
    // admin has all access to tmp mount
    proxy.createVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME, volumeArgs);

    OzoneVolume vol = objectStore.getVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME);
    assertNotNull(vol);

    // Begin test
    String hashedUsername = OFSPath.getTempMountBucketNameOfCurrentUser();

    // Expect failure since temp bucket for current user is not created yet
    try {
      vol.getBucket(hashedUsername);
    } catch (OMException ex) {
      // Expect BUCKET_NOT_FOUND
      if (!ex.getResult().equals(BUCKET_NOT_FOUND)) {
        fail("Temp bucket for current user shouldn't have been created");
      }
    }

    // set acls for shared tmp mount under the tmp volume
    // bucket acls have all access to admin and read+write+list access to world
    BucketArgs bucketArgs = new BucketArgs.Builder()
        .setOwner("admin")
        .addAcl(OzoneAcl.of(ACLIdentityType.WORLD, "", ACCESS, READ, WRITE, LIST))
        .addAcl(OzoneAcl.of(ACLIdentityType.USER, "admin", ACCESS, omConfig.getUserDefaultRights()))
        .setQuotaInNamespace(1000)
        .setQuotaInBytes(Long.MAX_VALUE).build();

    // Create bucket "tmp" with world access read+write+list to tmp directory
    // admin has all access to tmp mount
    proxy.createBucket(OFSPath.OFS_MOUNT_TMP_VOLUMENAME,
        OFSPath.OFS_MOUNT_TMP_VOLUMENAME, bucketArgs);

    // Write under /tmp/
    Path dir1 = new Path("/tmp/dir1");
    userOfs.mkdirs(dir1);

    try (FSDataOutputStream stream = userOfs.create(new Path(
        "/tmp/dir1/file1"))) {
      stream.write(1);
    }

    // Verify temp bucket creation
    OzoneBucket bucket = vol.getBucket("tmp");
    assertNotNull(bucket);
    // Verify dir1 creation
    FileStatus[] fileStatuses = fs.listStatus(new Path("/tmp/"));
    assertEquals(1, fileStatuses.length);
    assertEquals("/tmp/dir1", fileStatuses[0].getPath().toUri().getPath());
    // Verify file1 creation
    FileStatus[] fileStatusesInDir1 = fs.listStatus(dir1);
    assertEquals(1, fileStatusesInDir1.length);
    assertEquals("/tmp/dir1/file1", fileStatusesInDir1[0].getPath().toUri().getPath());

    // Cleanup
    userOfs.delete(dir1, true);
    try {
      userOfs.delete(new Path("/tmp"), true);
    } catch (OMException ex) {
      // Expect PERMISSION_DENIED, User regularuser1 doesn't have DELETE
      // permission for /tmp
      if (!ex.getResult().equals(PERMISSION_DENIED)) {
        fail("Temp bucket cannot be deleted by current user");
      }
    }
    fs.delete(new Path("/tmp"), true);
    proxy.deleteVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME);
  }

  /*
   * OFS: Test /tmp mount behavior.
   */
  @Test
  void testTempMount() throws IOException {
    assumeFalse(isBucketFSOptimized);

    // Prep
    // Use ClientProtocol to pass in volume ACL, ObjectStore won't do it
    ClientProtocol proxy = objectStore.getClientProxy();
    // Get default acl rights for user
    OmConfig omConfig = cluster.getOzoneManager().getConfig();
    // Construct ACL for world access
    OzoneAcl aclWorldAccess = OzoneAcl.of(ACLIdentityType.WORLD, "",
        ACCESS, omConfig.getUserDefaultRights());
    // Construct VolumeArgs
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addAcl(aclWorldAccess)
        .setQuotaInNamespace(1000).build();
    // Sanity check
    assertNull(volumeArgs.getOwner());
    assertNull(volumeArgs.getAdmin());
    assertEquals(-1, volumeArgs.getQuotaInBytes());
    assertEquals(1000, volumeArgs.getQuotaInNamespace());
    assertEquals(0, volumeArgs.getMetadata().size());
    assertEquals(1, volumeArgs.getAcls().size());
    // Create volume "tmp" with world access. allow non-admin to create buckets
    proxy.createVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME, volumeArgs);

    OzoneVolume vol = objectStore.getVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME);
    assertNotNull(vol);

    // Begin test
    String hashedUsername = OFSPath.getTempMountBucketNameOfCurrentUser();

    // Expect failure since temp bucket for current user is not created yet
    try {
      vol.getBucket(hashedUsername);
    } catch (OMException ex) {
      // Expect BUCKET_NOT_FOUND
      if (!ex.getResult().equals(BUCKET_NOT_FOUND)) {
        fail("Temp bucket for current user shouldn't have been created");
      }
    }

    // Write under /tmp/, OFS will create the temp bucket if not exist
    Path dir1 = new Path("/tmp/dir1");
    fs.mkdirs(dir1);

    try (FSDataOutputStream stream = fs.create(new Path("/tmp/dir1/file1"))) {
      stream.write(1);
    }

    // Verify temp bucket creation
    OzoneBucket bucket = vol.getBucket(hashedUsername);
    assertNotNull(bucket);
    // Verify dir1 creation
    FileStatus[] fileStatuses = fs.listStatus(new Path("/tmp/"));
    assertEquals(1, fileStatuses.length);
    assertEquals("/tmp/dir1", fileStatuses[0].getPath().toUri().getPath());
    // Verify file1 creation
    FileStatus[] fileStatusesInDir1 = fs.listStatus(dir1);
    assertEquals(1, fileStatusesInDir1.length);
    assertEquals("/tmp/dir1/file1", fileStatusesInDir1[0].getPath().toUri().getPath());

    // Cleanup
    fs.delete(dir1, true);
    vol.deleteBucket(hashedUsername);
    proxy.deleteVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME);
  }

  /**
   * Helper function. Check Ozone volume existence.
   * @param volumeStr Name of the volume
   * @return true if volume exists, false if not
   */
  private boolean volumeExist(String volumeStr) throws IOException {
    try {
      objectStore.getVolume(volumeStr);
    } catch (OMException ex) {
      if (ex.getResult() == VOLUME_NOT_FOUND) {
        return false;
      } else {
        throw ex;
      }
    }
    return true;
  }

  /**
   * Helper function. Delete a path non-recursively and expect failure.
   * @param f Path to delete.
   */
  private void deleteNonRecursivelyAndFail(Path f) throws IOException {
    try {
      fs.delete(f, false);
      fail("Should have thrown PathIsNotEmptyDirectoryException!");
    } catch (PathIsNotEmptyDirectoryException ignored) {
    }
  }

  @Test
  void testDeleteEmptyVolume() throws IOException {
    assumeFalse(isBucketFSOptimized);

    // Create volume
    String volumeStr1 = getRandomNonExistVolumeName();
    Path volumePath1 = new Path(OZONE_URI_DELIMITER + volumeStr1);
    fs.mkdirs(volumePath1);
    // Check volume creation
    OzoneVolume volume1 = objectStore.getVolume(volumeStr1);
    assertEquals(volumeStr1, volume1.getName());
    // Delete empty volume non-recursively
    assertTrue(fs.delete(volumePath1, false));
    // Verify the volume is deleted
    assertFalse(volumeExist(volumeStr1), volumeStr1 + " should have been deleted!");
  }

  @Test
  void testDeleteVolumeAndBucket() throws IOException {
    // Create volume and bucket
    String volumeStr2 = getRandomNonExistVolumeName();
    Path volumePath2 = new Path(OZONE_URI_DELIMITER + volumeStr2);
    String bucketStr2 = "bucket2";
    Path bucketPath2 = new Path(volumePath2, bucketStr2);
    fs.mkdirs(bucketPath2);
    // Check volume and bucket creation
    OzoneVolume volume2 = objectStore.getVolume(volumeStr2);
    assertEquals(volumeStr2, volume2.getName());
    OzoneBucket bucket2 = volume2.getBucket(bucketStr2);
    assertEquals(bucketStr2, bucket2.getName());
    // Delete volume non-recursively should fail since it is not empty
    deleteNonRecursivelyAndFail(volumePath2);
    // Delete bucket first, then volume
    assertTrue(fs.delete(bucketPath2, false));
    assertTrue(fs.delete(volumePath2, false));
    // Verify the volume is deleted
    assertFalse(volumeExist(volumeStr2));
  }

  @Test
  void testDeleteVolumeBucketAndKey() throws IOException {
    // Create test volume, bucket and key
    String volumeStr3 = getRandomNonExistVolumeName();
    Path volumePath3 = new Path(OZONE_URI_DELIMITER + volumeStr3);
    String bucketStr3 = "bucket3";
    Path bucketPath3 = new Path(volumePath3, bucketStr3);
    String dirStr3 = "dir3";
    Path dirPath3 = new Path(bucketPath3, dirStr3);
    fs.mkdirs(dirPath3);
    // Delete volume or bucket non-recursively, should fail
    deleteNonRecursivelyAndFail(volumePath3);
    deleteNonRecursivelyAndFail(bucketPath3);
    // Delete key first, then bucket, then volume
    assertTrue(fs.delete(dirPath3, false));
    assertTrue(fs.delete(bucketPath3, false));
    assertTrue(fs.delete(volumePath3, false));
    // Verify the volume is deleted
    assertFalse(volumeExist(volumeStr3));
  }

  private void createSymlinkSrcDestPaths(String srcVol,
      String srcBucket, String destVol, String destBucket) throws IOException {
    // src srcVol/srcBucket
    Path volumeSrcPath = new Path(OZONE_URI_DELIMITER + srcVol);
    Path bucketSrcPath = Path.mergePaths(volumeSrcPath,
        new Path(OZONE_URI_DELIMITER + srcBucket));
    fs.mkdirs(volumeSrcPath);
    OzoneVolume volume = objectStore.getVolume(srcVol);
    assertEquals(srcVol, volume.getName());
    fs.mkdirs(bucketSrcPath);
    OzoneBucket bucket = volume.getBucket(srcBucket);
    assertEquals(srcBucket, bucket.getName());

    // dest link destVol/destBucket -> srcVol/srcBucket
    Path volumeLinkPath = new Path(OZONE_URI_DELIMITER + destVol);
    fs.mkdirs(volumeLinkPath);
    volume = objectStore.getVolume(destVol);
    assertEquals(destVol, volume.getName());
    createLinkBucket(destVol, destBucket, srcVol, srcBucket);
  }

  @Test
  void testSymlinkList() throws Exception {
    OzoneFsShell shell = new OzoneFsShell(conf);
    // setup symlink, destVol/destBucket -> srcVol/srcBucket
    String srcVolume = getRandomNonExistVolumeName();
    final String srcBucket = "bucket";
    String destVolume = getRandomNonExistVolumeName();
    createSymlinkSrcDestPaths(srcVolume, srcBucket, destVolume, srcBucket);

    try {
      // test symlink -ls -R destVol/destBucket -> srcVol/srcBucket
      // srcBucket no keys
      // run toolrunner ozone fs shell commands
      try (GenericTestUtils.SystemOutCapturer capture =
               new GenericTestUtils.SystemOutCapturer()) {
        String linkPathStr = rootPath + destVolume;
        ToolRunner.run(shell, new String[]{"-ls", linkPathStr});
        assertThat(capture.getOutput()).contains("drwxrwxrwx");
        assertThat(capture.getOutput()).contains(linkPathStr +
            OZONE_URI_DELIMITER + srcBucket);
      } finally {
        shell.close();
      }

      // add key in source bucket
      final String key = "object-dir/object-name1";
      TestDataUtil.createKey(objectStore.getVolume(srcVolume).getBucket(srcBucket),
          key, RandomUtils.secure().randomBytes(1));
      assertEquals(key, objectStore.getVolume(srcVolume)
          .getBucket(srcBucket).getKey(key).getName());

      // test ls -R /destVol/destBucket, srcBucket with key (non-empty)
      try (GenericTestUtils.SystemOutCapturer capture =
               new GenericTestUtils.SystemOutCapturer()) {
        String linkPathStr = rootPath + destVolume;
        ToolRunner.run(shell, new String[]{"-ls", "-R",
            linkPathStr + OZONE_URI_DELIMITER + srcBucket});
        assertThat(capture.getOutput()).contains("drwxrwxrwx");
        assertThat(capture.getOutput()).contains(linkPathStr +
            OZONE_URI_DELIMITER + srcBucket);
        assertThat(capture.getOutput()).contains("-rw-rw-rw-");
        assertThat(capture.getOutput()).contains(linkPathStr +
            OZONE_URI_DELIMITER + srcBucket + OZONE_URI_DELIMITER + key);
      } finally {
        shell.close();
      }
    } finally {
      // cleanup; note must delete link before link src bucket
      // due to bug - HDDS-7884
      fs.delete(new Path(OZONE_URI_DELIMITER + destVolume +
          OZONE_URI_DELIMITER + srcBucket));
      fs.delete(new Path(OZONE_URI_DELIMITER + srcVolume +
          OZONE_URI_DELIMITER + srcBucket));
      fs.delete(new Path(OZONE_URI_DELIMITER + srcVolume), false);
      fs.delete(new Path(OZONE_URI_DELIMITER + destVolume), false);
    }
  }

  @Test
  void testSymlinkPosixDelete() throws Exception {
    OzoneFsShell shell = new OzoneFsShell(conf);
    // setup symlink, destVol/destBucket -> srcVol/srcBucket
    String srcVolume = getRandomNonExistVolumeName();
    final String srcBucket = "bucket";
    String destVolume = getRandomNonExistVolumeName();
    createSymlinkSrcDestPaths(srcVolume, srcBucket, destVolume, srcBucket);

    try {
      // test symlink destVol/destBucket -> srcVol/srcBucket exists
      assertTrue(fs.exists(new Path(OZONE_URI_DELIMITER +
          destVolume + OZONE_URI_DELIMITER + srcBucket)));

      // add key to srcBucket
      final String key = "object-dir/object-name1";
      TestDataUtil.createKey(objectStore.getVolume(srcVolume).getBucket(srcBucket),
          key, RandomUtils.secure().randomBytes(1));
      assertEquals(key, objectStore.getVolume(srcVolume).
          getBucket(srcBucket).getKey(key).getName());

      // test symlink -rm destVol/destBucket -> srcVol/srcBucket
      // should delete only link, srcBucket and key unaltered
      // run toolrunner ozone fs shell commands
      try {
        String linkPathStr = rootPath + destVolume + OZONE_URI_DELIMITER +
            srcBucket;
        int res = ToolRunner.run(shell, new String[]{"-rm", "-skipTrash",
            linkPathStr});
        assertEquals(0, res);

        try {
          objectStore.getVolume(destVolume).getBucket(srcBucket);
          fail("Bucket should not exist, should throw OMException");
        } catch (OMException ex) {
          assertEquals(BUCKET_NOT_FOUND, ex.getResult());
        }

        assertEquals(srcBucket, objectStore.getVolume(srcVolume)
            .getBucket(srcBucket).getName());
        assertEquals(key, objectStore.getVolume(srcVolume)
            .getBucket(srcBucket).getKey(key).getName());

        // re-create symlink
        createLinkBucket(destVolume, srcBucket, srcVolume, srcBucket);
        assertTrue(fs.exists(new Path(OZONE_URI_DELIMITER +
            destVolume + OZONE_URI_DELIMITER + srcBucket)));

        // test symlink -rm -R -f destVol/destBucket/ -> srcVol/srcBucket
        // should delete only link contents (src bucket contents),
        // link and srcBucket unaltered
        // run toolrunner ozone fs shell commands
        linkPathStr = rootPath + destVolume + OZONE_URI_DELIMITER + srcBucket;
        res = ToolRunner.run(shell, new String[]{"-rm", "-skipTrash",
            "-f", "-R", linkPathStr + OZONE_URI_DELIMITER});
        assertEquals(0, res);

        assertEquals(srcBucket, objectStore.getVolume(destVolume)
            .getBucket(srcBucket).getName());
        assertTrue(objectStore.getVolume(destVolume)
            .getBucket(srcBucket).isLink());
        assertEquals(srcBucket, objectStore.getVolume(srcVolume)
            .getBucket(srcBucket).getName());
        try {
          objectStore.getVolume(srcVolume).getBucket(srcBucket).getKey(key);
          fail("Key should be deleted under srcBucket, " +
              "OMException expected");
        } catch (OMException ex) {
          assertEquals(KEY_NOT_FOUND, ex.getResult());
        }

        // test symlink -rm -R -f destVol/destBucket -> srcVol/srcBucket
        // should delete only link
        // run toolrunner ozone fs shell commands
        linkPathStr = rootPath + destVolume + OZONE_URI_DELIMITER + srcBucket;
        res = ToolRunner.run(shell, new String[]{"-rm", "-skipTrash",
            "-f", "-R", linkPathStr});
        assertEquals(0, res);

        assertEquals(srcBucket, objectStore.getVolume(srcVolume)
            .getBucket(srcBucket).getName());
        // test link existence
        try {
          objectStore.getVolume(destVolume).getBucket(srcBucket);
          fail("link should not exist, " +
              "OMException expected");
        } catch (OMException ex) {
          assertEquals(BUCKET_NOT_FOUND, ex.getResult());
        }
        // test src bucket existence
        assertEquals(objectStore.getVolume(srcVolume)
            .getBucket(srcBucket).getName(), srcBucket);
      } finally {
        shell.close();
      }
    } finally {
      // cleanup; note must delete link before link src bucket
      // due to bug - HDDS-7884
      fs.delete(new Path(OZONE_URI_DELIMITER + destVolume + OZONE_URI_DELIMITER
          + srcBucket));
      fs.delete(new Path(OZONE_URI_DELIMITER + srcVolume + OZONE_URI_DELIMITER
          + srcBucket));
      fs.delete(new Path(OZONE_URI_DELIMITER + srcVolume), false);
      fs.delete(new Path(OZONE_URI_DELIMITER + destVolume), false);
    }
  }

  @Test
  void testDeleteBucketLink() throws Exception {
    // Create test volume, bucket, directory
    String volumeStr1 = getRandomNonExistVolumeName();
    Path volumePath1 = new Path(OZONE_URI_DELIMITER + volumeStr1);
    String bucketStr1 = "bucket";
    Path bucketPath1 = new Path(volumePath1, bucketStr1);
    String dirStr1 = "dir1";
    Path dirPath1 = new Path(bucketPath1, dirStr1);
    fs.mkdirs(dirPath1);
    FileStatus dir1Status = fs.getFileStatus(dirPath1);

    // Create volume with link to first
    String linkVolume = getRandomNonExistVolumeName();
    Path linkVolumePath = new Path(OZONE_URI_DELIMITER + linkVolume);
    fs.mkdirs(linkVolumePath);

    String linkStr = "link";
    createLinkBucket(linkVolume, linkStr, volumeStr1, bucketStr1);
    Path linkPath = new Path(linkVolumePath, linkStr);
    Path dirPathLink = new Path(linkPath, dirStr1);

    // confirm data through link
    FileStatus dirLinkStatus = fs.getFileStatus(dirPathLink);
    assertNotNull(dirLinkStatus);

    // confirm non recursive delete of volume with link fails
    deleteNonRecursivelyAndFail(linkVolumePath);

    fs.delete(linkPath, true);
    fs.delete(linkVolumePath, false);

    // confirm vol1 data is unaffected
    assertEquals(dir1Status, fs.getFileStatus(dirPath1));

    // confirm link is gone
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> fs.getFileStatus(dirPathLink));
    assertThat(exception.getMessage()).contains("File not found.");

    // Cleanup
    fs.delete(bucketPath1, true);
    fs.delete(volumePath1, false);

  }

  @Test
  void testFailToDeleteRoot() throws IOException {
    // rm root should always fail for OFS
    assertFalse(fs.delete(new Path("/"), false));
    assertFalse(fs.delete(new Path("/"), true));
  }

  /**
   * Helper function for testGetTrashRoots() for checking the first element
   * in the FileStatus Collection.
   * @param expected Expected path String
   * @param res Collection of FileStatus from getTrashRoots()
   */
  private void checkFirstFileStatusPath(String expected,
      Collection<FileStatus> res) {
    Optional<FileStatus> optional = res.stream().findFirst();
    assertTrue(optional.isPresent());
    assertEquals(expected, optional.get().getPath().toUri().getPath());
  }

  /**
   * Helper function for testGetTrashRoots() for checking all owner field in
   * FileStatuses in the Collection.
   * @param expectedSize Expected size of the FileStatus Collection
   * @param expectedOwner Expected owner String
   * @param res Collection of FileStatus from getTrashRoots()
   */
  private void checkFileStatusOwner(int expectedSize, String expectedOwner,
      Collection<FileStatus> res) {
    assertEquals(expectedSize, res.size());
    res.forEach(e -> assertEquals(expectedOwner, e.getOwner()));
  }

  @Test
  void testGetTrashRoot() {
    String testKeyName = "keyToBeDeleted";
    Path keyPath1 = new Path(bucketPath, testKeyName);
    assertEquals(new Path(rootPath + volumeName + "/" + bucketName + "/" +
        TRASH_PREFIX + "/" +  USER1 + "/"), userOfs.getTrashRoot(keyPath1));
  }

  /**
   * Test getTrashRoots() in OFS. Different from the existing test for o3fs.
   */
  @Test
  void testGetTrashRoots() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    OzoneVolume volume1 = objectStore.getVolume(volumeName);
    String prevOwner = volume1.getOwner();
    // Set owner of the volume to current user, so it will show up in vol list
    assertTrue(volume1.setOwner(username));

    Path trashRoot1 = new Path(bucketPath, TRASH_PREFIX);
    Path user1Trash1 = new Path(trashRoot1, username);
    // When user trash dir hasn't been created
    assertEquals(0, fs.getTrashRoots(false).size());
    assertEquals(0, fs.getTrashRoots(true).size());
    // Let's create our first user1 (current user) trash dir.
    fs.mkdirs(user1Trash1);
    // Results should be getTrashRoots(false)=1, gTR(true)=1
    Collection<FileStatus> res = fs.getTrashRoots(false);
    assertEquals(1, res.size());
    checkFirstFileStatusPath(user1Trash1.toString(), res);
    res = fs.getTrashRoots(true);
    assertEquals(1, res.size());
    checkFirstFileStatusPath(user1Trash1.toString(), res);

    // Create one more trash for user2 in the same bucket
    Path user2Trash1 = new Path(trashRoot1, "testuser2");
    fs.mkdirs(user2Trash1);
    // Results should be getTrashRoots(false)=1, gTR(true)=2
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    assertEquals(2, fs.getTrashRoots(true).size());

    // Create a new bucket in the same volume
    final String bucketName2 = "trashroottest2";
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setBucketLayout(bucketLayout);
    volume1.createBucket(bucketName2, builder.build());
    Path bucketPath2 = new Path(volumePath, bucketName2);
    Path trashRoot2 = new Path(bucketPath2, TRASH_PREFIX);
    Path user1Trash2 = new Path(trashRoot2, username);
    // Create a file at the trash location, it shouldn't be recognized as trash
    try (FSDataOutputStream out1 = fs.create(user1Trash2)) {
      out1.write(123);
    }
    // Results should still be getTrashRoots(false)=1, gTR(true)=2
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    checkFirstFileStatusPath(user1Trash1.toString(), res);
    assertEquals(2, fs.getTrashRoots(true).size());
    // Remove the file and create a dir instead. It should be recognized now
    fs.delete(user1Trash2, false);
    fs.mkdirs(user1Trash2);
    // Results should now be getTrashRoots(false)=2, gTR(true)=3
    checkFileStatusOwner(2, username, fs.getTrashRoots(false));
    assertEquals(3, fs.getTrashRoots(true).size());

    // Create a new volume and a new bucket
    OzoneBucket bucket3 =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    OzoneVolume volume3 = objectStore.getVolume(bucket3.getVolumeName());
    // Need to setOwner to current test user so it has permission to list vols
    volume3.setOwner(username);
    Path volumePath3 = new Path(OZONE_URI_DELIMITER, bucket3.getVolumeName());
    Path bucketPath3 = new Path(volumePath3, bucket3.getName());
    Path trashRoot3 = new Path(bucketPath3, TRASH_PREFIX);
    Path user1Trash3 = new Path(trashRoot3, username);
    // Results should be getTrashRoots(false)=3, gTR(true)=4
    fs.mkdirs(user1Trash3);
    checkFileStatusOwner(3, username, fs.getTrashRoots(false));
    assertEquals(4, fs.getTrashRoots(true).size());
    // One more user
    Path user3Trash1 = new Path(trashRoot3, "testuser3");
    fs.mkdirs(user3Trash1);
    // Results should be getTrashRoots(false)=3, gTR(true)=5
    checkFileStatusOwner(3, username, fs.getTrashRoots(false));
    assertEquals(5, fs.getTrashRoots(true).size());

    // Clean up, and check while doing so
    fs.delete(trashRoot3, true);
    checkFileStatusOwner(2, username, fs.getTrashRoots(false));
    assertEquals(3, fs.getTrashRoots(true).size());
    fs.delete(trashRoot2, true);
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    assertEquals(2, fs.getTrashRoots(true).size());
    fs.delete(user2Trash1, true);
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    assertEquals(1, fs.getTrashRoots(true).size());

    volume3.deleteBucket(bucket3.getName());
    objectStore.deleteVolume(volume3.getName());
    volume1.deleteBucket(bucketName2);

    fs.delete(user1Trash1, true);
    assertEquals(0, fs.getTrashRoots(false).size());
    assertEquals(0, fs.getTrashRoots(true).size());
    fs.delete(trashRoot1, true);
    // Restore owner
    assertTrue(volume1.setOwner(prevOwner));
  }

  @Test
  void testFileDelete() throws Exception {
    Path grandparent = new Path(bucketPath, "testBatchDelete");
    Path parent = new Path(grandparent, "parent");
    Path childFolder = new Path(parent, "childFolder");
    // BatchSize is 5, so we're going to set a number that's not a
    // multiple of 5. In order to test the final number of keys less than
    // batchSize can also be deleted.
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      Path childFolderFile = new Path(childFolder, "child" + i);
      ContractTestUtils.touch(fs, childFile);
      ContractTestUtils.touch(fs, childFolderFile);
    }

    assertEquals(1, fs.listStatus(grandparent).length);
    assertEquals(9, fs.listStatus(parent).length);
    assertEquals(8, fs.listStatus(childFolder).length);

    assertTrue(fs.delete(grandparent, true));
    assertFalse(fs.exists(grandparent));
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      // Make sure all keys under testBatchDelete/parent should be deleted
      assertFalse(fs.exists(childFile));

      // Test to recursively delete child folder, make sure all keys under
      // testBatchDelete/parent/childFolder should be deleted.
      Path childFolderFile = new Path(childFolder, "child" + i);
      assertFalse(fs.exists(childFolderFile));
    }
    // Will get: WARN  ozone.BasicOzoneFileSystem delete: Path does not exist.
    // This will return false.
    assertFalse(fs.delete(parent, true));
  }

  /**
   * 1.Move a Key to Trash
   * 2.Verify that the key gets deleted by the trash emptier.
   * 3.Create a second Key in different bucket and verify deletion.
   */
  @Test
  void testTrash() throws Exception {
    String testKeyName = "keyToBeDeleted";
    Path keyPath1 = new Path(bucketPath, testKeyName);
    try (FSDataOutputStream stream = fs.create(keyPath1)) {
      stream.write(1);
    }
    // create second bucket and write a key in it.
    OzoneBucket bucket2 =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    String volumeName2 = bucket2.getVolumeName();
    Path volumePath2 = new Path(OZONE_URI_DELIMITER, volumeName2);
    String bucketName2 = bucket2.getName();
    Path bucketPath2 = new Path(volumePath2, bucketName2);
    Path keyPath2 = new Path(bucketPath2, testKeyName + "1");
    try (FSDataOutputStream stream = fs.create(keyPath2)) {
      stream.write(1);
    }

    assertTrue(trash.getConf().getClass(
        "fs.trash.classname", TrashPolicy.class).
        isAssignableFrom(OzoneTrashPolicy.class));

    long prevNumTrashDeletes = getOMMetrics().getNumTrashDeletes();
    long prevNumTrashFileDeletes = getOMMetrics().getNumTrashFilesDeletes();

    long prevNumTrashRenames = getOMMetrics().getNumTrashRenames();
    long prevNumTrashFileRenames = getOMMetrics().getNumTrashFilesRenames();

    long prevNumTrashAtomicDirDeletes = getOMMetrics()
        .getNumTrashAtomicDirDeletes();
    long prevNumTrashAtomicDirRenames = getOMMetrics()
        .getNumTrashAtomicDirRenames();

    // Construct paths for first key
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path trashRoot = new Path(bucketPath, TRASH_PREFIX);
    Path userTrash = new Path(trashRoot, username);
    Path userTrashCurrent = new Path(userTrash, "Current");
    Path trashPath = new Path(userTrashCurrent, testKeyName);

    // Construct paths for second key in different bucket
    Path trashRoot2 = new Path(bucketPath2, TRASH_PREFIX);
    Path userTrash2 = new Path(trashRoot2, username);
    Path trashPath2 = new Path(userTrashCurrent, testKeyName + "1");

    // Call moveToTrash. We can't call protected fs.rename() directly
    trash.moveToTrash(keyPath1);
    // for key in second bucket
    trash.moveToTrash(keyPath2);

    // key should either be present in Current or checkpointDir
    assertTrue(fs.exists(trashPath)
        || fs.listStatus(fs.listStatus(userTrash)[0].getPath()).length > 0);


    // Wait until the TrashEmptier purges the keys
    GenericTestUtils.waitFor(() -> {
      try {
        return !fs.exists(trashPath) && !fs.exists(trashPath2);
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed", e);
        fail("Delete from Trash Failed");
        return false;
      }
    }, 1000, 180000);

    if (isBucketFSOptimized) {
      assertThat(getOMMetrics().getNumTrashAtomicDirRenames())
          .isGreaterThan(prevNumTrashAtomicDirRenames);
    } else {
      // This condition should pass after the checkpoint
      assertThat(getOMMetrics().getNumTrashRenames())
          .isGreaterThan(prevNumTrashRenames);
      // With new layout version, file renames wouldn't be counted
      assertThat(getOMMetrics().getNumTrashFilesRenames())
          .isGreaterThan(prevNumTrashFileRenames);
    }

    // wait for deletion of checkpoint dir
    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(userTrash).length == 0 &&
            fs.listStatus(userTrash2).length == 0;
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed", e);
        fail("Delete from Trash Failed");
        return false;
      }
    }, 1000, 120000);

    // This condition should succeed once the checkpoint directory is deleted
    if (isBucketFSOptimized) {
      GenericTestUtils.waitFor(
          () -> getOMMetrics().getNumTrashAtomicDirDeletes() >
              prevNumTrashAtomicDirDeletes, 100, 180000);
    } else {
      GenericTestUtils.waitFor(
          () -> getOMMetrics().getNumTrashDeletes() > prevNumTrashDeletes
              && getOMMetrics().getNumTrashFilesDeletes()
              >= prevNumTrashFileDeletes, 100, 180000);
    }
    // Cleanup
    fs.delete(trashRoot, true);
    fs.delete(trashRoot2, true);

  }

  @Test
  void testCreateWithInvalidPaths() {
    assumeFalse(isBucketFSOptimized);

    // Test for path with ..
    Path parent = new Path("../../../../../d1/d2/");
    Path file1 = new Path(parent, "key1");
    checkInvalidPath(file1);

    // Test for path with :
    file1 = new Path("/:/:");
    checkInvalidPath(file1);

    // Test for path with scheme and authority.
    file1 = new Path(fs.getUri() + "/:/:");
    checkInvalidPath(file1);
  }

  private void checkInvalidPath(Path path) {
    InvalidPathException exception = assertThrows(InvalidPathException.class,
        () -> fs.create(path, false));
    assertThat(exception.getMessage()).contains("Invalid path Name");
  }

  @Test
  void testRenameFile() throws Exception {
    final String dir = "/dir" + RandomUtils.secure().randomInt(0, 1000);
    Path dirPath = new Path(getBucketPath() + dir);
    Path file1Source = new Path(getBucketPath() + dir
        + "/file1_Copy");
    Path file1Destin = new Path(getBucketPath() + dir + "/file1");
    try {
      getFs().mkdirs(dirPath);

      ContractTestUtils.touch(getFs(), file1Source);
      assertTrue(getFs().rename(file1Source, file1Destin), "Renamed failed");
      assertTrue(getFs().exists(file1Destin), "Renamed failed: /dir/file1");
      FileStatus[] fStatus = getFs().listStatus(dirPath);
      assertEquals(1, fStatus.length, "Renamed failed");
    } finally {
      // clean up
      fs.delete(dirPath, true);
    }
  }



  /**
   * Rename file to an existed directory.
   */
  @Test
  void testRenameFileToDir() throws Exception {
    final String dir = "/dir" + RandomUtils.secure().randomInt(0, 1000);
    Path dirPath = new Path(getBucketPath() + dir);
    getFs().mkdirs(dirPath);

    Path file1Destin = new Path(getBucketPath() + dir  + "/file1");
    ContractTestUtils.touch(getFs(), file1Destin);
    Path abcRootPath = new Path(getBucketPath() + "/a/b/c");
    getFs().mkdirs(abcRootPath);
    assertTrue(getFs().rename(file1Destin, abcRootPath), "Renamed failed");
    assertTrue(getFs().exists(new Path(
        abcRootPath, "file1")), "Renamed filed: /a/b/c/file1");
    getFs().delete(getBucketPath(), true);
  }

  /**
   * Rename to the source's parent directory, it will succeed.
   * 1. Rename from /root_dir/dir1/dir2 to /root_dir.
   * Expected result : /root_dir/dir2
   * <p>
   * 2. Rename from /root_dir/dir1/file1 to /root_dir.
   * Expected result : /root_dir/file1.
   */
  @Test
  void testRenameToParentDir() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(getBucketPath() + dir2);
    final Path destRootPath = new Path(getBucketPath() + root);
    Path file1Source = new Path(getBucketPath() + dir1 + "/file2");
    try {
      getFs().mkdirs(dir2SourcePath);

      ContractTestUtils.touch(getFs(), file1Source);

      // rename source directory to its parent directory(destination).
      assertTrue(getFs().rename(dir2SourcePath, destRootPath), "Rename failed");
      final Path expectedPathAfterRename =
          new Path(getBucketPath() + root + "/dir2");
      assertTrue(getFs().exists(expectedPathAfterRename), "Rename failed");

      // rename source file to its parent directory(destination).
      assertTrue(getFs().rename(file1Source, destRootPath), "Rename failed");
      final Path expectedFilePathAfterRename =
          new Path(getBucketPath() + root + "/file2");
      assertTrue(getFs().exists(expectedFilePathAfterRename), "Rename failed");
    } finally {
      // clean up
      fs.delete(file1Source, true);
      fs.delete(dir2SourcePath, true);
      fs.delete(destRootPath, true);
    }
  }

  /**
   *  Cannot rename a directory to its own subdirectory.
   */
  @Test
  void testRenameDirToItsOwnSubDir() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final Path dir1Path = new Path(getBucketPath() + dir1);
    // Add a sub-dir1 to the directory to be moved.
    final Path subDir1 = new Path(dir1Path, "sub_dir1");
    getFs().mkdirs(subDir1);
    LOG.info("Created dir1 {}", subDir1);

    final Path sourceRoot = new Path(getBucketPath() + root);
    LOG.info("Rename op-> source:{} to destin:{}", sourceRoot, subDir1);
    //  rename should fail and return false
    try {
      getFs().rename(sourceRoot, subDir1);
      fail("Should throw exception : Cannot rename a directory to" +
          " its own subdirectory");
    } catch (IllegalArgumentException e) {
      //expected
    } finally {
      // clean up
      fs.delete(sourceRoot, true);
    }
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Test
  void testRenameDestinationParentDoesNotExist() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(getBucketPath() + dir2);
    getFs().mkdirs(dir2SourcePath);
    // (a) parent of dst does not exist.  /root_dir/b/c
    final Path destinPath = new Path(getBucketPath()
        + root + "/b/c");

    // rename should throw exception
    try {
      getFs().rename(dir2SourcePath, destinPath);
      fail("Should fail as parent of dst does not exist!");
    } catch (FileNotFoundException fnfe) {
      //expected
    }
    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(getBucketPath() + root + "/file1");
    ContractTestUtils.touch(getFs(), filePath);
    Path newDestinPath = new Path(filePath, "c");
    // rename shouldthrow exception
    try {
      getFs().rename(dir2SourcePath, newDestinPath);
      fail("Should fail as parent of dst is a file!");
    } catch (IOException e) {
      //expected
    }
  }

  @Test
  void testBucketDefaultsShouldNotBeInheritedToFileForNonEC()
      throws Exception {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    builder.setDefaultReplicationConfig(
        new DefaultReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE)));
    BucketArgs omBucketArgs = builder.build();
    String vol = UUID.randomUUID().toString();
    String buck = UUID.randomUUID().toString();
    final OzoneBucket bucket100 = TestDataUtil
        .createVolumeAndBucket(client, vol, buck,
            omBucketArgs);
    assertEquals(ReplicationType.STAND_ALONE.name(), bucket100.getReplicationConfig().getReplicationType().name());

    // Bucket has default STAND_ALONE and client has default RATIS.
    // In this case, it should not inherit from bucket
    try (OzoneFSOutputStream file = adapter
        .createFile(vol + "/" + buck + "/test", (short) 3, true, false)) {
      file.write(new byte[1024]);
    }
    OFSPath ofsPath = new OFSPath(vol + "/" + buck + "/test", conf);
    final OzoneBucket bucket = adapter.getBucket(ofsPath, false);
    final OzoneKeyDetails key = bucket.getKey(ofsPath.getKeyName());
    assertEquals(key.getReplicationConfig().getReplicationType().name(), ReplicationType.RATIS.name());
  }

  @Test
  void testBucketDefaultsShouldBeInheritedToFileForEC()
      throws Exception {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    builder.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig("RS-3-2-1024k")));
    BucketArgs omBucketArgs = builder.build();
    String vol = UUID.randomUUID().toString();
    String buck = UUID.randomUUID().toString();
    final OzoneBucket bucket101 = TestDataUtil
        .createVolumeAndBucket(client, vol, buck,
            omBucketArgs);
    assertEquals(ReplicationType.EC.name(), bucket101.getReplicationConfig().getReplicationType().name());
    // Bucket has default EC and client has default RATIS.
    // In this case, it should inherit from bucket
    try (OzoneFSOutputStream file = adapter
        .createFile(vol + "/" + buck + "/test", (short) 3, true, false)) {
      file.write(new byte[1024]);
    }
    OFSPath ofsPath = new OFSPath(vol + "/" + buck + "/test", conf);
    final OzoneBucket bucket = adapter.getBucket(ofsPath, false);
    final OzoneKeyDetails key = bucket.getKey(ofsPath.getKeyName());
    assertEquals(ReplicationType.EC.name(), key.getReplicationConfig().getReplicationType().name());
  }

  @Test
  void testGetFileStatus() throws Exception {
    String volumeNameLocal = getRandomNonExistVolumeName();
    String bucketNameLocal = RandomStringUtils.secure().nextNumeric(5);
    Path volume = new Path("/" + volumeNameLocal);
    fs.mkdirs(volume);
    assertThrows(OMException.class,
        () -> fs.getFileStatus(new Path(volume, bucketNameLocal)));
    // Cleanup
    fs.delete(volume, false);
  }

  @Test
  void testUnbuffer() throws IOException {
    String testKeyName = "testKey2";
    Path path = new Path(bucketPath, testKeyName);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }

    try (FSDataInputStream stream = fs.open(path)) {
      assertTrue(stream.hasCapability(StreamCapabilities.UNBUFFER));
      stream.unbuffer();
    }

  }

  @Test
  void testCreateAndCheckECFileDiskUsage() throws Exception {
    String key = "eckeytest";
    Path volPathTest = new Path(OZONE_URI_DELIMITER, volumeName);
    Path bucketPathTest = new Path(volPathTest, bucketName);

    // write some test data into bucket
    TestDataUtil.createKey(objectStore.getVolume(volumeName).
            getBucket(bucketName), key, new ECReplicationConfig("RS-3-2-1024k"),
        RandomUtils.secure().randomBytes(1));
    // make sure the disk usage matches the expected value
    Path filePath = new Path(bucketPathTest, key);
    ContentSummary contentSummary = fs.getContentSummary(filePath);
    long length = contentSummary.getLength();
    long spaceConsumed = contentSummary.getSpaceConsumed();
    long expectDiskUsage = QuotaUtil.getReplicatedSize(length,
        new ECReplicationConfig(3, 2, RS, (int) OzoneConsts.MB));
    assertEquals(expectDiskUsage, spaceConsumed);
    //clean up
    fs.delete(filePath, true);
  }

  @Test
  void testCreateAndCheckRatisFileDiskUsage() throws Exception {
    String key = "ratiskeytest";
    Path volPathTest = new Path(OZONE_URI_DELIMITER, volumeName);
    Path bucketPathTest = new Path(volPathTest, bucketName);
    Path filePathTest = new Path(bucketPathTest, key);

    // write some test data into bucket
    TestDataUtil.createKey(objectStore.
        getVolume(volumeName).getBucket(bucketName), key,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        RandomUtils.secure().randomBytes(1));
    // make sure the disk usage matches the expected value
    ContentSummary contentSummary = fs.getContentSummary(filePathTest);
    long length = contentSummary.getLength();
    long spaceConsumed = contentSummary.getSpaceConsumed();
    long expectDiskUsage = QuotaUtil.getReplicatedSize(length,
            RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.THREE));
    assertEquals(expectDiskUsage, spaceConsumed);
    //clean up
    fs.delete(filePathTest, true);
  }

  @Test
  void testNonPrivilegedUserMkdirCreateBucket() throws IOException {
    // This test is only meaningful when ACL is enabled
    assumeTrue(enableAcl, "ACL is not enabled. Skipping this test as it requires " +
            "ACL to be enabled to be meaningful.");

    // Sanity check
    assertTrue(cluster.getOzoneManager().getAclsEnabled());

    final String volume = "volume-for-test-get-bucket";
    // Create a volume as admin
    // Create volume "tmp" with world access. allow non-admin to create buckets
    ClientProtocol proxy = objectStore.getClientProxy();

    // Get default acl rights for user
    OmConfig omConfig = cluster.getOzoneManager().getConfig();
    // Construct ACL for world access
    OzoneAcl aclWorldAccess = OzoneAcl.of(ACLIdentityType.WORLD, "",
        ACCESS, omConfig.getUserDefaultRights());
    // Construct VolumeArgs, set ACL to world access
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addAcl(aclWorldAccess)
        .build();
    proxy.createVolume(volume, volumeArgs);

    // Create a bucket as non-admin, should succeed
    final String bucket = "test-bucket-1";
    try {
      final Path myBucketPath = new Path(volume, bucket);
      // Have to prepend the root to bucket path here.
      // Otherwise, FS will automatically prepend user home directory path
      // which is not we want here.
      assertTrue(userOfs.mkdirs(new Path("/", myBucketPath)));
    } catch (IOException e) {
      fail("Should not have thrown exception when creating bucket as" +
          " a regular user here");
    }

    // Clean up
    proxy.deleteBucket(volume, bucket);
    proxy.deleteVolume(volume);
  }

  private void createLinkBucket(String linkVolume, String linkBucket,
      String sourceVolume, String sourceBucket) throws IOException {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setSourceVolume(sourceVolume)
        .setSourceBucket(sourceBucket);
    OzoneVolume ozoneVolume = objectStore.getVolume(linkVolume);
    ozoneVolume.createBucket(linkBucket, builder.build());
  }

  private Path createAndGetBucketPath()
      throws IOException {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(bucketLayout);
    BucketArgs omBucketArgs = builder.build();
    String vol = UUID.randomUUID().toString();
    String buck = UUID.randomUUID().toString();
    final OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, vol, buck, omBucketArgs);
    Path volume = new Path(OZONE_URI_DELIMITER, bucket.getVolumeName());
    return new Path(volume, bucket.getName());
  }

  @Test
  void testSnapshotRead() throws Exception {
    // Init data
    OzoneBucket bucket1 =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    Path volume1Path = new Path(OZONE_URI_DELIMITER, bucket1.getVolumeName());
    Path bucket1Path = new Path(volume1Path, bucket1.getName());
    Path file1 = new Path(bucket1Path, "key1");
    Path file2 = new Path(bucket1Path, "key2");

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    OzoneBucket bucket2 =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    Path volume2Path = new Path(OZONE_URI_DELIMITER, bucket2.getVolumeName());
    Path bucket2Path = new Path(volume2Path, bucket2.getName());

    fs.mkdirs(bucket2Path);
    Path snapPath1 = fs.createSnapshot(bucket1Path, "snap1");
    Path snapPath2 = fs.createSnapshot(bucket2Path, "snap1");

    Path file3 = new Path(bucket1Path, "key3");
    ContractTestUtils.touch(fs, file3);

    Path snapPath3 = fs.createSnapshot(bucket1Path, "snap2");

    try {
      FileStatus[] f1 = fs.listStatus(snapPath1);
      FileStatus[] f2 = fs.listStatus(snapPath2);
      FileStatus[] f3 = fs.listStatus(snapPath3);
      assertEquals(2, f1.length);
      assertEquals(0, f2.length);
      assertEquals(3, f3.length);
    } catch (Exception e) {
      fail("Failed to read/list on snapshotPath, exception: " + e);
    }
  }

  @Test
  void testFileSystemDeclaresCapability() throws Throwable {
    assertHasPathCapabilities(fs, getBucketPath(), FS_ACLS);
    assertHasPathCapabilities(fs, getBucketPath(), FS_CHECKSUMS);
  }

  @Test
  void testSnapshotDiff() throws Exception {
    OzoneBucket bucket1 =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    Path volumePath1 = new Path(OZONE_URI_DELIMITER, bucket1.getVolumeName());
    Path bucketPath1 = new Path(volumePath1, bucket1.getName());
    Path snap1 = fs.createSnapshot(bucketPath1);
    Path file1 = new Path(bucketPath1, "key1");
    Path file2 = new Path(bucketPath1, "key2");
    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);
    Path snap2 = fs.createSnapshot(bucketPath1);
    java.nio.file.Path fromSnapPath = Paths.get(snap1.toString()).getFileName();
    java.nio.file.Path toSnapPath = Paths.get(snap2.toString()).getFileName();
    String fromSnap = fromSnapPath != null ? fromSnapPath.toString() : null;
    String toSnap = toSnapPath != null ? toSnapPath.toString() : null;
    SnapshotDiffReport diff =
        fs.getSnapshotDiffReport(bucketPath1, fromSnap, toSnap);
    assertEquals(2, diff.getDiffList().size());
    assertEquals(SnapshotDiffReport.DiffType.CREATE, diff.getDiffList().get(0).getType());
    assertEquals(SnapshotDiffReport.DiffType.CREATE, diff.getDiffList().get(1).getType());
    assertArrayEquals("key1".getBytes(StandardCharsets.UTF_8), diff.getDiffList().get(0).getSourcePath());
    assertArrayEquals("key2".getBytes(StandardCharsets.UTF_8), diff.getDiffList().get(1).getSourcePath());

    // test whether snapdiff returns aggregated response as
    // page size is 4.
    for (int fileCount = 0; fileCount < 10; fileCount++) {
      Path file =
          new Path(bucketPath1, "key" + RandomStringUtils.secure().nextAlphabetic(5));
      ContractTestUtils.touch(fs, file);
    }
    Path snap3 = fs.createSnapshot(bucketPath1);
    fromSnapPath = toSnapPath;
    toSnapPath = Paths.get(snap3.toString()).getFileName();
    fromSnap = fromSnapPath != null ? fromSnapPath.toString() : null;
    toSnap = toSnapPath != null ? toSnapPath.toString() : null;
    diff = fs.getSnapshotDiffReport(bucketPath1, fromSnap, toSnap);
    assertEquals(10, diff.getDiffList().size());

    Path file =
        new Path(bucketPath1, "key" + RandomStringUtils.secure().nextAlphabetic(5));
    ContractTestUtils.touch(fs, file);
    diff = fs.getSnapshotDiffReport(bucketPath1, toSnap, "");
    assertEquals(1, diff.getDiffList().size());

    diff = fs.getSnapshotDiffReport(bucketPath1, "", toSnap);
    assertEquals(1, diff.getDiffList().size());

    diff = fs.getSnapshotDiffReport(bucketPath1, "", "");
    assertEquals(0, diff.getDiffList().size());

    // try snapDiff between non-bucket paths
    String errorMsg = "Path is not a bucket";
    String finalFromSnap = fromSnap;
    String finalToSnap = toSnap;
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> fs.getSnapshotDiffReport(volumePath1, finalFromSnap,
            finalToSnap));
    assertThat(exception.getMessage()).contains(errorMsg);
  }

  @Test
  void testSetTimes() throws Exception {
    // Create a file
    OzoneBucket bucket1 =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    Path volumePath1 = new Path(OZONE_URI_DELIMITER, bucket1.getVolumeName());
    Path bucketPath1 = new Path(volumePath1, bucket1.getName());
    Path path = new Path(bucketPath1, "key1");
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }

    long mtime = 1000;
    fs.setTimes(path, mtime, 2000);

    FileStatus fileStatus = fs.getFileStatus(path);
    // verify that mtime is updated as expected.
    assertEquals(mtime, fileStatus.getModificationTime());

    long mtimeDontUpdate = -1;
    fs.setTimes(path, mtimeDontUpdate, 2000);

    fileStatus = fs.getFileStatus(path);
    // verify that mtime is NOT updated as expected.
    assertEquals(mtime, fileStatus.getModificationTime());
  }

  @Test
  public void testSetTimesForLinkedBucketPath() throws Exception {
    // Create a file
    OzoneBucket sourceBucket =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    Path volumePath1 =
        new Path(OZONE_URI_DELIMITER, sourceBucket.getVolumeName());
    Path sourceBucketPath = new Path(volumePath1, sourceBucket.getName());
    Path path = new Path(sourceBucketPath, "key1");
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }
    OzoneVolume sourceVol = client.getObjectStore().getVolume(sourceBucket.getVolumeName());
    String linkBucketName = UUID.randomUUID().toString();
    createLinkBucket(sourceVol.getName(), linkBucketName,
        sourceVol.getName(), sourceBucket.getName());

    Path linkedBucketPath = new Path(volumePath1, linkBucketName);
    Path keyInLinkedBucket = new Path(linkedBucketPath, "key1");

    // test setTimes in linked bucket path
    long mtime = 1000;
    fs.setTimes(keyInLinkedBucket, mtime, 2000);

    FileStatus fileStatus = fs.getFileStatus(path);
    // verify that mtime is updated as expected.
    assertEquals(mtime, fileStatus.getModificationTime());

    long mtimeDontUpdate = -1;
    fs.setTimes(keyInLinkedBucket, mtimeDontUpdate, 2000);

    fileStatus = fs.getFileStatus(keyInLinkedBucket);
    // verify that mtime is NOT updated as expected.
    assertEquals(mtime, fileStatus.getModificationTime());
  }

  @ParameterizedTest(name = "Source Replication Factor = {0}")
  @ValueSource(shorts = { 1, 3 })
  public void testDistcp(short sourceRepFactor) throws Exception {
    Path srcBucketPath = createAndGetBucketPath();
    Path insideSrcBucket = new Path(srcBucketPath, "*");
    Path dstBucketPath = createAndGetBucketPath();
    // create 2 files on source
    List<String> fileNames = createFiles(srcBucketPath, 2, sourceRepFactor);
    // Create target directory/bucket
    fs.mkdirs(dstBucketPath);

    // perform distcp
    DistCpOptions options =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath).build();
    options.appendToConf(conf);
    Job distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 2);
    FileStatus sourceFileStatus = fs.listStatus(srcBucketPath)[0];
    FileStatus dstFileStatus = fs.listStatus(dstBucketPath)[0];
    assertEquals(sourceRepFactor, sourceFileStatus.getReplication());
    // without preserve distcp should create file with default replication
    assertEquals(fs.getDefaultReplication(dstBucketPath),
        dstFileStatus.getReplication());

    deleteFiles(dstBucketPath, fileNames);

    // test preserve option
    options =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath).preserve(DistCpOptions.FileAttribute.REPLICATION)
            .build();
    options.appendToConf(conf);
    distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 2);
    dstFileStatus = fs.listStatus(dstBucketPath)[0];
    // src and dst should have same replication
    assertEquals(sourceRepFactor, dstFileStatus.getReplication());

    // test if copy is skipped due to matching checksums
    assertFalse(options.shouldSkipCRC());
    distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 0, 2);
  }

  private void verifyCopy(Path dstBucketPath, Job distcpJob,
      long expectedFilesToBeCopied, long expectedTotalFilesInDest) throws IOException {
    long filesCopied =
        distcpJob.getCounters().findCounter(CopyMapper.Counter.COPY).getValue();
    FileStatus[] destinationFileStatus = fs.listStatus(dstBucketPath);
    assertEquals(expectedTotalFilesInDest, destinationFileStatus.length);
    assertEquals(expectedFilesToBeCopied, filesCopied);
  }

  private List<String> createFiles(Path srcBucketPath, int fileCount, short factor) throws IOException {
    List<String> createdFiles = new ArrayList<>();
    for (int i = 1; i <= fileCount; i++) {
      String keyName = "key" + RandomStringUtils.secure().nextNumeric(5);
      Path file = new Path(srcBucketPath, keyName);
      try (FSDataOutputStream fsDataOutputStream = fs.create(file, factor)) {
        fsDataOutputStream.writeBytes("Hello");
      }
      createdFiles.add(keyName);
    }
    return createdFiles;
  }

  private void deleteFiles(Path base, List<String> fileNames) throws IOException {
    for (String key : fileNames) {
      fs.delete(new Path(base, key));
    }
  }
}
