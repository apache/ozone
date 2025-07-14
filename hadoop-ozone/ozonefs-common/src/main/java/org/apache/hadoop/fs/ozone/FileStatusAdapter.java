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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

/**
 * Class to hold the internal information of a FileStatus.
 * <p>
 * As FileStatus class is not compatible between 3.x and 2.x hadoop we can
 * use this adapter to hold all the required information. Hadoop 3.x FileStatus
 * information can be converted to this class, and this class can be used to
 * create hadoop 2.x FileStatus.
 * <p>
 * FileStatus (Hadoop 3.x) --&gt; FileStatusAdapter --&gt; FileStatus (Hadoop 2.x)
 */
public final class FileStatusAdapter {

  private final long length;
  private final long diskConsumed;
  private final Path path;
  private final boolean isdir;
  private final short blockReplication;
  private final long blocksize;
  private final long modificationTime;
  private final long accessTime;
  private final short permission;
  private final String owner;
  private final String group;
  private final Path symlink;
  private final List<BlockLocation> blockLocations;

  private final boolean isEncrypted;

  private final boolean isErasureCoded;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public FileStatusAdapter(long length, long diskConsumed, Path path,
      boolean isdir, short blockReplication, long blocksize,
      long modificationTime, long accessTime, short permission,
      String owner, String group, Path symlink,
      BlockLocation[] locations, boolean isEncrypted,
      boolean isErasureCoded) {
    this.length = length;
    this.diskConsumed = diskConsumed;
    this.path = path;
    this.isdir = isdir;
    this.blockReplication = blockReplication;
    this.blocksize = blocksize;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.permission = permission;
    this.owner = owner;
    this.group = group;
    this.symlink = symlink;
    this.blockLocations = new ArrayList<>(Arrays.asList(locations));
    this.isEncrypted = isEncrypted;
    this.isErasureCoded = isErasureCoded;
  }

  public Path getPath() {
    return path;
  }

  public boolean isDir() {
    return isdir;
  }

  public boolean isFile() {
    return !isdir;
  }

  public short getBlockReplication() {
    return blockReplication;
  }

  public long getBlocksize() {
    return blocksize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public short getPermission() {
    return permission;
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }

  public Path getSymlink() {
    return symlink;
  }

  public long getLength() {
    return length;
  }

  public long getDiskConsumed() {
    return diskConsumed;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public boolean isErasureCoded() {
    return isErasureCoded;
  }

  public BlockLocation[] getBlockLocations() {
    return blockLocations.toArray(new BlockLocation[0]);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName())
        .append('{')
        .append("path=").append(path)
        .append("; isDirectory=").append(isdir);
    if (isFile()) {
      sb.append("; length=").append(length)
              .append("; diskConsumed= ").append(getDiskConsumed())
          .append("; blockReplication=").append(blockReplication)
          .append("; blocksize=").append(blocksize);
    }
    sb.append("; accessTime=").append(accessTime)
        .append("; owner=").append(owner)
        .append("; group=").append(group)
        .append("; permission=").append(permission)
        .append("; isSymlink=").append(getSymlink())
        .append('}');
    
    return sb.toString();
  }

}
