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

package org.apache.hadoop.ozone.dn;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Utility offers low-level access methods to datanode.
 */
public final class DatanodeTestUtils {
  private static final String FAILURE_SUFFIX = ".origin";

  private DatanodeTestUtils() {
  }

  /**
   * It injects disk failures to data dirs by replacing these data dirs with
   * regular files.
   *
   * @param dirs data directories.
   * @throws IOException on I/O error.
   */
  public static void injectDataDirFailure(File... dirs) throws IOException {
    for (File dir : dirs) {
      File renamedTo = new File(dir.getPath() + FAILURE_SUFFIX);
      if (renamedTo.exists()) {
        throw new IOException(String.format(
            "Can not inject failure to dir: %s because %s exists.",
            dir, renamedTo));
      }
      if (!dir.renameTo(renamedTo)) {
        throw new IOException(String.format("Failed to rename %s to %s.",
            dir, renamedTo));
      }
      if (!dir.createNewFile()) {
        throw new IOException(String.format(
            "Failed to create file %s to inject disk failure.", dir));
      }
    }
  }

  /**
   * Restore the injected data dir failures.
   *
   * @see {@link #injectDataDirFailure}.
   * @param dirs data directories.
   * @throws IOException
   */
  public static void restoreDataDirFromFailure(File... dirs)
      throws IOException {
    for (File dir : dirs) {
      File renamedDir = new File(dir.getPath() + FAILURE_SUFFIX);
      if (renamedDir.exists()) {
        if (dir.exists()) {
          if (!dir.isFile()) {
            throw new IOException(
                "Injected failure data dir is supposed to be file: " + dir);
          }
          if (!dir.delete()) {
            throw new IOException(
                "Failed to delete injected failure data dir: " + dir);
          }
        }
        if (!renamedDir.renameTo(dir)) {
          throw new IOException(String.format(
              "Failed to recover injected failure data dir %s to %s.",
              renamedDir, dir));
        }
      }
    }
  }

  /**
   * Inject disk failures to data files by replacing with dirs.
   * @param files
   * @throws IOException
   */
  public static void injectDataFileFailure(File... files) throws IOException {
    for (File file : files) {
      if (file.exists()) {
        File renamedTo = new File(file.getPath() + FAILURE_SUFFIX);
        if (renamedTo.exists()) {
          throw new IOException(String.format(
              "Can not inject failure to file: %s because %s exists.",
              file, renamedTo));
        }
        if (!file.renameTo(renamedTo)) {
          throw new IOException(String.format("Failed to rename %s to %s.",
              file, renamedTo));
        }
        if (!file.mkdir()) {
          throw new IOException(String.format(
              "Failed to create dir %s to inject disk failure.", file));
        }
      }
    }
  }

  /**
   * Restore the injected data files from disk failures.
   * @see {@link #injectDataFileFailure(File...)}
   * @param files
   * @throws IOException
   */
  public static void restoreDataFileFromFailure(File... files)
      throws IOException {
    for (File file : files) {
      File renamedFile = new File(file.getPath() + FAILURE_SUFFIX);
      if (renamedFile.exists()) {
        if (file.exists()) {
          if (!file.isDirectory()) {
            throw new IOException(
                "Injected failure data file is supposed to be dir: " + file);
          }
          if (!file.delete()) {
            throw new IOException(
                "Failed to delete injected failure data file: " + file);
          }
        }
        if (!renamedFile.renameTo(file)) {
          throw new IOException(String.format(
              "Failed to recover injected failure data file %s to %s.",
              renamedFile, file));
        }
      }
    }
  }

  /**
   * Inject failure to container metadata dir by removing write
   * permission on it.
   * KeyValueContainer uses a creatTemp & rename way to update
   * container metadata file, so we cannot use a rename or
   * change permission way to do injection.
   * @param dirs
   */
  public static void injectContainerMetaDirFailure(File... dirs) {
    for (File dir : dirs) {
      if (dir.exists()) {
        assertTrue(dir.setWritable(false, false));
      }
    }
  }

  /**
   * Restore container metadata dir from failure.
   * @see {@link #injectContainerMetaDirFailure(File...)}
   * @param dirs
   */
  public static void restoreContainerMetaDirFromFailure(File... dirs) {
    for (File dir : dirs) {
      if (dir.exists()) {
        assertTrue(dir.setWritable(true, true));
      }
    }
  }

  /**
   * Simulate a bad rootDir by removing write permission.
   * @see {@link org.apache.hadoop.ozone.container.common.volume
   * .StorageVolume#check(Boolean)}
   * @param rootDir
   */
  public static void simulateBadRootDir(File rootDir) {
    if (rootDir.exists()) {
      assertTrue(rootDir.setWritable(false));
    }
  }

  /**
   * Simulate a bad volume by removing write permission.
   * @see {@link org.apache.hadoop.ozone.container.common.volume
   * .StorageVolume#check(Boolean)}
   * @param vol
   */
  public static void simulateBadVolume(StorageVolume vol) {
    simulateBadRootDir(vol.getStorageDir());
  }

  /**
   * Restore a simulated bad volume to normal.
   * @see {@link #simulateBadVolume(StorageVolume)}
   * @param rootDir
   */
  public static void restoreBadRootDir(File rootDir) {
    if (rootDir.exists()) {
      assertTrue(rootDir.setWritable(true));
    }
  }

  /**
   * Restore a simulated bad rootDir to normal.
   * @see {@link #simulateBadVolume(StorageVolume)}
   * @param vol
   */
  public static void restoreBadVolume(StorageVolume vol) {
    restoreBadRootDir(vol.getStorageDir());
  }

  /**
   * Wait for failed volume to be handled.
   * @param volSet
   * @param numOfFailedVolumes
   * @throws Exception
   */
  public static void waitForHandleFailedVolume(
      MutableVolumeSet volSet, int numOfFailedVolumes) throws Exception {
    GenericTestUtils.waitFor(
        () -> numOfFailedVolumes == volSet.getFailedVolumesList().size(),
        100, 10000);
  }

  public static File getHddsVolumeClusterDir(HddsVolume vol) {
    File hddsVolumeRootDir = vol.getStorageDir();
    return new File(hddsVolumeRootDir, vol.getClusterID());
  }
}
