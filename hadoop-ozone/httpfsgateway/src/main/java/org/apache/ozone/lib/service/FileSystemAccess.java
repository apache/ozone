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

package org.apache.ozone.lib.service;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Interface for accessing the filesystem.
 */
@InterfaceAudience.Private
public interface FileSystemAccess {

  /**
   * Executor to filesystem operations.
   *
   * @param <T> return type of operations.
   */
  interface FileSystemExecutor<T> {

    T execute(FileSystem fs) throws IOException;
  }

  <T> T execute(String user,
                Configuration conf,
                FileSystemExecutor<T> executor)
      throws FileSystemAccessException;

  FileSystem createFileSystem(String user, Configuration conf)
      throws IOException, FileSystemAccessException;

  void releaseFileSystem(FileSystem fs) throws IOException;

  Configuration getFileSystemConfiguration();

}
