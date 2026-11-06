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

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;

/**
 * The status that ofs and o3fs {@code getFileStatus} return. Like HDFS, it has no block locations, and
 * {@code getFileBlockLocations} fetches them from OM for it.
 * <p>
 * A status from {@code listStatus} has no block locations either, but {@code getFileBlockLocations} keeps returning a
 * placeholder location for it without an OM call, so that a listing that asks for the locations of every file (as
 * Spark does) does not pay an OM call per file.
 */
final class FileStatusWithoutBlockLocations extends FileStatus {
  private static final long serialVersionUID = 1L;

  private final boolean encrypted;
  private final boolean erasureCoded;

  /**
   * @param erasureCoded {@code status.isErasureCoded()}, which is not in Hadoop 2
   */
  FileStatusWithoutBlockLocations(FileStatus status, boolean erasureCoded) throws IOException {
    // the copy constructor does not copy the encrypted and erasure coded flags
    super(status);
    this.encrypted = status.isEncrypted();
    this.erasureCoded = erasureCoded;
  }

  @Override
  public boolean isEncrypted() {
    return encrypted;
  }

  @Override
  public boolean isErasureCoded() {
    return erasureCoded;
  }
}
