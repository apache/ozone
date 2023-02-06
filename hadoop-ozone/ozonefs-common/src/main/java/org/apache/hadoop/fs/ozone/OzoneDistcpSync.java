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



import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.tools.DistCpSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

/**
 * Ozone's DistcpSync implementation.
 */
public class OzoneDistcpSync extends DistCpSync {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneDistcpSync.class);

  @Override
  public void checkFilesystemSupport(FileSystem srcFs, FileSystem tgtFs) {
    if (!(srcFs instanceof BasicRootedOzoneFileSystem)) {
      throw new IllegalArgumentException(
          "Unsupported source file system: " + srcFs.getScheme() + "://. "
              + "Supported file systems: ofs. If scheme is either " +
              "hdfs/webhdfs use respective distcp.sync.class.name " +
              "in the configuration ");
    }
    if (!(tgtFs instanceof BasicRootedOzoneFileSystem)) {
      throw new IllegalArgumentException(
          "Unsupported source file system: " + srcFs.getScheme() + "://. "
              + "Supported file systems: ofs. If scheme is either " +
              "hdfs/webhdfs use respective distcp.sync.class.name " +
              "in the configuration ");
    }

  }

  @Override
  protected SnapshotDiffReport getSnapshotDiffReport(Path ssDir, FileSystem fs,
      String from, String to) throws IOException {
    if (fs instanceof BasicRootedOzoneFileSystem) {
      BasicRootedOzoneFileSystem ofs = (BasicRootedOzoneFileSystem) fs;
      return ofs.getSnapshotDiffReport(ssDir, from, to);
    } else {
      throw new IllegalArgumentException(
          "Unsupported source file system: " + fs.getScheme() + "://. "
              + "Supported file systems: ofs. If scheme is either " +
              "hdfs/webhdfs use respective distcp.sync.class.name " +
              "in the configuration ");
    }
  }

  @Override
  protected boolean checkNoChange(FileSystem fs, Path path) {
    // TODO fix HDDS-7905.
    return true;
  }
}
