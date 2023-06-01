/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.utils;

import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

public class DiskCheckUtil {
  private static void logError(Logger log, File storageDir, String message) {
    log.error("Volume {} failed health check. {}", storageDir, message);
  }

  private static void logError(Logger log, File storageDir, String message,
                               Exception ex) {
    log.error("Volume {} failed health check. {}", storageDir, message, ex);
  }

  public static boolean checkExistence(Logger log, File diskDir) {
    if (!diskDir.exists()) {
      logError(log, diskDir, "Directory does not exist.");
      return false;
    }
    return true;
  }

  public static boolean checkPermissions(Logger log, File storageDir) {
    // Check all permissions on the volume. If there are multiple permission
    // errors, count it as one failure so the admin can fix them all at once.
    boolean permissionsCorrect = true;
    if (!storageDir.canRead()) {
      logError(log, storageDir,
          "Datanode does not have read permission on volume.");
      permissionsCorrect = false;
    }
    if (!storageDir.canWrite()) {
      logError(log, storageDir,
          "Datanode does not have write permission on volume.");
      permissionsCorrect = false;
    }
    if (!storageDir.canExecute()) {
      logError(log, storageDir,
          "Datanode does not have execute permission on volume.");
      permissionsCorrect = false;
    }

    return permissionsCorrect;
  }

  public static boolean checkReadWrite(Logger log, File storageDir,
      File testFileDir, int numBytesToWrite) {
    File testFile = new File(testFileDir, "disk-check-" + UUID.randomUUID());
    byte[] writtenBytes = new byte[numBytesToWrite];
    new Random().nextBytes(writtenBytes);
    try (FileOutputStream fos = new FileOutputStream(testFile)) {
      fos.write(writtenBytes);
      fos.getFD().sync();
    } catch (FileNotFoundException notFoundEx) {
      logError(log, storageDir, String.format("Could not find file %s for " +
              "volume check.", testFile), notFoundEx);
      return false;
    } catch (SyncFailedException syncEx) {
      logError(log, storageDir, String.format("Could sync file %s to disk.",
          testFile), syncEx);
      return false;
    } catch (IOException ioEx) {
      logError(log, storageDir, String.format("Could not write file %s " +
          "for volume check.", testFile), ioEx);
      return false;
    }

    // Read data back from the test file.
    byte[] readBytes = new byte[numBytesToWrite];
    try (FileInputStream fis = new FileInputStream(testFile)) {
      int numBytesRead = fis.read(readBytes);
      if (numBytesRead != numBytesToWrite) {
        logError(log, storageDir, String.format("%d bytes written to file %s " +
            "but %d bytes were read back.", numBytesToWrite, testFile,
            numBytesRead));
        return false;
      }
    } catch (FileNotFoundException notFoundEx) {
      logError(log, storageDir, String.format("Could not find file %s " +
          "for volume check.", testFile), notFoundEx);
      return false;
    } catch (IOException ioEx) {
      logError(log, storageDir, String.format("Could not read file %s " +
          "for volume check.", testFile), ioEx);
      return false;
    }

    // Check that test file has the expected content.
    if (!Arrays.equals(writtenBytes, readBytes)) {
      logError(log, storageDir, String.format("%d Bytes read from file " +
          "%s do not match the %d bytes that were written.",
          writtenBytes.length, testFile, readBytes.length));
      return false;
    }

    // Delete the file.
    if (!testFile.delete()) {
      logError(log, storageDir, String.format("Could not delete file %s " +
          "for volume check.", testFile));
      return false;
    }

    // If all checks passed, the volume is healthy.
    return true;
  }
}
