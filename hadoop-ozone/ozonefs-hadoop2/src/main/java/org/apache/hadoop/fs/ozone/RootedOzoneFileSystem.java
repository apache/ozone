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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Minimal Rooted Ozone File System compatible with Hadoop 2.x.
 */
public class RootedOzoneFileSystem extends BasicRootedOzoneFileSystem {
  @Override
  protected FileStatus constructFileStatus(
          FileStatusAdapter fileStatusAdapter) {
    return new FileStatus(fileStatusAdapter.getLength(),
            fileStatusAdapter.isDir(),
            fileStatusAdapter.getBlockReplication(),
            fileStatusAdapter.getBlocksize(),
            fileStatusAdapter.getModificationTime(),
            fileStatusAdapter.getAccessTime(),
            new FsPermission(fileStatusAdapter.getPermission()),
            fileStatusAdapter.getOwner(),
            fileStatusAdapter.getGroup(),
            fileStatusAdapter.getSymlink(),
            fileStatusAdapter.getPath()
    );
  }
}
