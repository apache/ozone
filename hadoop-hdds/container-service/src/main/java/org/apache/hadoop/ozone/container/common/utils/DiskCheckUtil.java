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

package org.apache.hadoop.ozone.container.common.utils;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SyncFailedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that supports checking disk health when provided a directory
 * where the disk is mounted.
 */
public final class DiskCheckUtil {
  // For testing purposes, an alternate check implementation can be provided
  // to inject failures.
  private static DiskChecks impl = new DiskChecksImpl();

  private DiskCheckUtil() {
  }

  @VisibleForTesting
  public static void setTestImpl(DiskChecks diskChecks) {
    impl = diskChecks;
  }

  @VisibleForTesting
  public static void clearTestImpl() {
    impl = new DiskChecksImpl();
  }

  public static boolean checkExistence(File storageDir) {
    return impl.checkExistence(storageDir);
  }

  public static boolean checkPermissions(File storageDir) {
    return impl.checkPermissions(storageDir);
  }

  public static boolean checkReadWrite(File storageDir, File testFileDir,
      int numBytesToWrite) {
    return impl.checkReadWrite(storageDir, testFileDir, numBytesToWrite);
  }

  /**
   * Defines operations that must be implemented by a class injecting
   * failures into this class. Default implementations return true so that
   * tests only need to override methods for the failures they want to test.
   */
  public interface DiskChecks {
    default boolean checkExistence(File storageDir) {
      return true;
    }

    default boolean checkPermissions(File storageDir) {
      return true;
    }

    default boolean checkReadWrite(File storageDir, File testFileDir,
                            int numBytesToWrite) {
      return true;
    }
  }

  /**
   * The default implementation of DiskCheck that production code will use
   * for disk checking.
   */
  private static class DiskChecksImpl implements DiskChecks {

    private static final Logger LOG =
        LoggerFactory.getLogger(DiskCheckUtil.class);

    private static final Random RANDOM = new Random();

    @Override
    public boolean checkExistence(File diskDir) {
      if (!diskDir.exists()) {
        logError(diskDir, "Directory does not exist.");
        return false;
      }
      return true;
    }

    @Override
    public boolean checkPermissions(File storageDir) {
      // Check all permissions on the volume. If there are multiple permission
      // errors, count it as one failure so the admin can fix them all at once.
      boolean permissionsCorrect = true;
      if (!storageDir.canRead()) {
        logError(storageDir,
            "Datanode does not have read permission on volume.");
        permissionsCorrect = false;
      }
      if (!storageDir.canWrite()) {
        logError(storageDir,
            "Datanode does not have write permission on volume.");
        permissionsCorrect = false;
      }
      if (!storageDir.canExecute()) {
        logError(storageDir, "Datanode does not have execute" +
            "permission on volume.");
        permissionsCorrect = false;
      }

      return permissionsCorrect;
    }

    @Override
    public boolean checkReadWrite(File storageDir,
        File testFileDir, int numBytesToWrite) {
      File testFile = new File(testFileDir, "disk-check-" + UUID.randomUUID());
      byte[] writtenBytes = new byte[numBytesToWrite];
      RANDOM.nextBytes(writtenBytes);
      try (OutputStream fos = FileUtils.newOutputStreamForceAtClose(testFile, CREATE, TRUNCATE_EXISTING, WRITE)) {
        fos.write(writtenBytes);
      } catch (FileNotFoundException | NoSuchFileException notFoundEx) {
        logError(storageDir, String.format("Could not find file %s for " +
            "volume check.", testFile.getAbsolutePath()), notFoundEx);
        return false;
      } catch (SyncFailedException syncEx) {
        logError(storageDir, String.format("Could sync file %s to disk.",
            testFile.getAbsolutePath()), syncEx);
        return false;
      } catch (IOException ioEx) {
        logError(storageDir, String.format("Could not write file %s " +
            "for volume check.", testFile.getAbsolutePath()), ioEx);
        return false;
      }

      // Read data back from the test file.
      byte[] readBytes = new byte[numBytesToWrite];
      try (InputStream fis = Files.newInputStream(testFile.toPath())) {
        int numBytesRead = fis.read(readBytes);
        if (numBytesRead != numBytesToWrite) {
          logError(storageDir, String.format("%d bytes written to file %s " +
                  "but %d bytes were read back.", numBytesToWrite,
              testFile.getAbsolutePath(), numBytesRead));
          return false;
        }
      } catch (FileNotFoundException | NoSuchFileException notFoundEx) {
        logError(storageDir, String.format("Could not find file %s " +
            "for volume check.", testFile.getAbsolutePath()), notFoundEx);
        return false;
      } catch (IOException ioEx) {
        logError(storageDir, String.format("Could not read file %s " +
            "for volume check.", testFile.getAbsolutePath()), ioEx);
        return false;
      }

      // Check that test file has the expected content.
      if (!Arrays.equals(writtenBytes, readBytes)) {
        logError(storageDir, String.format("%d Bytes read from file " +
                "%s do not match the %d bytes that were written.",
            writtenBytes.length, testFile.getAbsolutePath(), readBytes.length));
        return false;
      }

      // Delete the file.
      if (!testFile.delete()) {
        logError(storageDir, String.format("Could not delete file %s " +
            "for volume check.", testFile.getAbsolutePath()));
        return false;
      }

      // If all checks passed, the volume is healthy.
      return true;
    }

    private void logError(File storageDir, String message) {
      LOG.error("Volume {} failed health check. {}", storageDir, message);
    }

    private void logError(File storageDir, String message, Exception ex) {
      LOG.error("Volume {} failed health check. {}", storageDir, message, ex);
    }
  }
}
