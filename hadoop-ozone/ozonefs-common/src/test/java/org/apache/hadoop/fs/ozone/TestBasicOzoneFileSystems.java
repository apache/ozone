/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

/**
 * Unit test for Basic*OzoneFileSystem.
 */
@RunWith(Parameterized.class)
public class TestBasicOzoneFileSystems {

  private final FileSystem subject;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{new BasicOzoneFileSystem()},
        new Object[]{new BasicRootedOzoneFileSystem()}
    );
  }

  public TestBasicOzoneFileSystems(FileSystem subject) {
    this.subject = subject;
  }

  @Test
  public void defaultBlockSize() {
    Configuration conf = new OzoneConfiguration();
    subject.setConf(conf);

    long expected = toBytes(OZONE_SCM_BLOCK_SIZE_DEFAULT);
    assertDefaultBlockSize(expected);
  }

  @Test
  public void defaultBlockSizeCustomized() {
    String customValue = "128MB";
    Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_BLOCK_SIZE, customValue);
    subject.setConf(conf);

    assertDefaultBlockSize(toBytes(customValue));
  }

  // test for filesystem pseduo-posix symlink support
  @Test
  public void testFileSystemPosixSymlinkSupport() {
    if (subject instanceof BasicRootedOzoneFileSystem) {
      Assert.assertTrue(subject.supportsSymlinks());
    } else if (subject instanceof BasicOzoneFileSystem) {
      Assert.assertFalse(subject.supportsSymlinks());
    } else {
      Assert.fail("Test case not implemented for FileSystem: " +
          subject.getClass().getSimpleName());
    }
  }

  @Test
  public void testCreateSnapshotReturnPath() throws IOException {
    final String snapshotName = "snap1";

    if (subject instanceof BasicRootedOzoneFileSystem) {
      BasicRootedOzoneClientAdapterImpl adapter =
          Mockito.mock(BasicRootedOzoneClientAdapterImpl.class);
      Mockito.doReturn(snapshotName).when(adapter).createSnapshot(any(), any());

      BasicRootedOzoneFileSystem ofs =
          Mockito.spy((BasicRootedOzoneFileSystem) subject);
      Mockito.when(ofs.getAdapter()).thenReturn(adapter);

      Path ofsBucketStr = new Path("ofs://om/vol1/buck1/");
      Path ofsDir1 = new Path(ofsBucketStr, "dir1");
      Path res = ofs.createSnapshot(new Path(ofsDir1, snapshotName));

      Path expectedSnapshotRoot = new Path(ofsBucketStr, OM_SNAPSHOT_INDICATOR);
      Path expectedSnapshotPath = new Path(expectedSnapshotRoot, snapshotName);

      // Return value path should be "ofs://om/vol1/buck1/.snapshot/snap1"
      // without the subdirectory "dir1" in the Path.
      Assert.assertEquals(expectedSnapshotPath, res);
    } else if (subject instanceof BasicOzoneFileSystem) {
      BasicOzoneClientAdapterImpl adapter =
          Mockito.mock(BasicOzoneClientAdapterImpl.class);
      Mockito.doReturn(snapshotName).when(adapter).createSnapshot(any(), any());

      BasicOzoneFileSystem o3fs = Mockito.spy((BasicOzoneFileSystem) subject);
      Mockito.when(o3fs.getAdapter()).thenReturn(adapter);

      Path o3fsBucketStr = new Path("o3fs://buck1.vol1.om/");
      Path o3fsDir1 = new Path(o3fsBucketStr, "dir1");
      Path res = o3fs.createSnapshot(new Path(o3fsDir1, snapshotName));

      Path expectedSnapshotRoot =
          new Path(o3fsBucketStr, OM_SNAPSHOT_INDICATOR);
      Path expectedSnapshotPath = new Path(expectedSnapshotRoot, snapshotName);

      // Return value path should be "o3fs://buck1.vol1.om/.snapshot/snap1"
      // without the subdirectory "dir1" in the Path.
      Assert.assertEquals(expectedSnapshotPath, res);
    } else {
      Assert.fail("Test case not implemented for FileSystem: " +
          subject.getClass().getSimpleName());
    }
  }

  private void assertDefaultBlockSize(long expected) {
    assertEquals(expected, subject.getDefaultBlockSize());

    Path anyPath = new Path("/");
    assertEquals(expected, subject.getDefaultBlockSize(anyPath));

    Path nonExistentFile = new Path("/no/such/file");
    assertEquals(expected, subject.getDefaultBlockSize(nonExistentFile));
  }

  private static long toBytes(String value) {
    StorageSize blockSize = StorageSize.parse(value);
    return (long) blockSize.getUnit().toBytes(blockSize.getValue());
  }

}
