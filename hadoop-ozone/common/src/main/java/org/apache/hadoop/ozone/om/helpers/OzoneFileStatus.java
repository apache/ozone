/**
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

package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ozone.protocol.proto.FSProtos;
import org.apache.hadoop.ozone.protocol.proto.FSProtos.FileStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * File Status of the Ozone Key.
 */
public class OzoneFileStatus extends FileStatus {

  private static final long serialVersionUID = 1L;

  transient private OmKeyInfo keyInfo;

  public OzoneFileStatus(OmKeyInfo key, long blockSize, boolean isDirectory) {
    super(key.getDataSize(), isDirectory, key.getFactor().getNumber(),
        blockSize, key.getModificationTime(), getPath(key.getKeyName()));
    keyInfo = key;
  }

  public OzoneFileStatus(FileStatus status, OmKeyInfo key) throws IOException {
    super(status);
    keyInfo = key;
  }

  // Use this constructor only for directories
  public OzoneFileStatus(String keyName) {
    super(0, true, 0, 0, 0, getPath(keyName));
  }

  public OzoneFileStatusProto getProtobuf() throws IOException {
    OzoneFileStatusProto.Builder builder = OzoneFileStatusProto.newBuilder()
        .setStatus(convert(this));
    if (keyInfo != null) {
      builder.setKeyInfo(keyInfo.getProtobuf());
    }
    return builder.build();
  }

  public static OzoneFileStatus getFromProtobuf(OzoneFileStatusProto response)
      throws IOException {
    return new OzoneFileStatus(convert(response.getStatus()),
        OmKeyInfo.getFromProtobuf(response.getKeyInfo()));
  }

  public static Path getPath(String keyName) {
    return new Path(OZONE_URI_DELIMITER + keyName);
  }

  public FileStatus makeQualified(URI defaultUri, Path parent,
                                  String owner, String group) {
    // fully-qualify path
    setPath(parent.makeQualified(defaultUri, null));
    setGroup(group);
    setOwner(owner);
    if (isDirectory()) {
      setPermission(FsPermission.getDirDefault());
    } else {
      setPermission(FsPermission.getFileDefault());
    }
    return this; // API compatibility
  }

  /** Get the modification time of the file/directory.
   *
   * o3fs uses objects as "fake" directories, which are not updated to
   * reflect the accurate modification time. We choose to report the
   * current time because some parts of the ecosystem (e.g. the
   * HistoryServer) use modification time to ignore "old" directories.
   *
   * @return for files the modification time in milliseconds since January 1,
   *         1970 UTC or for directories the current time.
   */
  @Override
  public long getModificationTime(){
    if (isDirectory() && super.getModificationTime() == 0) {
      return System.currentTimeMillis();
    } else {
      return super.getModificationTime();
    }
  }

  public OmKeyInfo getKeyInfo() {
    return keyInfo;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  private static FileStatus convert(FileStatusProto proto) throws IOException {
    final Path path;
    final long length;
    final boolean isdir;
    final short blockReplication;
    final long blocksize;
    final long mtime;
    final long atime;
    final String owner;
    final String group;
    final FsPermission permission;
    final Path symlink;
    switch (proto.getFileType()) {
    case FT_DIR:
      isdir = true;
      symlink = null;
      blocksize = 0;
      length = 0;
      blockReplication = 0;
      break;
    case FT_SYMLINK:
      isdir = false;
      symlink = new Path(proto.getSymlink());
      blocksize = 0;
      length = 0;
      blockReplication = 0;
      break;
    case FT_FILE:
      isdir = false;
      symlink = null;
      blocksize = proto.getBlockSize();
      length = proto.getLength();
      int brep = proto.getBlockReplication();
      if ((brep & 0xffff0000) != 0) {
        throw new IOException(String.format("Block replication 0x%08x " +
            "doesn't fit in 16 bits.", brep));
      }
      blockReplication = (short) brep;
      break;
    default:
      throw new IllegalStateException("Unknown type: " + proto.getFileType());
    }
    path = new Path(proto.getPath());
    mtime = proto.getModificationTime();
    atime = proto.getAccessTime();
    permission = convertPermission(proto.getPermission());
    owner = proto.getOwner();
    group = proto.getGroup();
    int flags = proto.getFlags();
    FileStatus fileStatus = new FileStatus(length, isdir, blockReplication,
        blocksize, mtime, atime, permission, owner, group, symlink, path,
        FileStatus.attributes(
            (flags & FileStatusProto.Flags.HAS_ACL_VALUE) != 0,
            (flags & FileStatusProto.Flags.HAS_CRYPT_VALUE) != 0,
            (flags & FileStatusProto.Flags.HAS_EC_VALUE) != 0,
            (flags & FileStatusProto.Flags.SNAPSHOT_ENABLED_VALUE) != 0));
    return fileStatus;
  }

  private static FileStatusProto convert(FileStatus stat) throws IOException {
    FileStatusProto.Builder bld = FileStatusProto.newBuilder();
    bld.setPath(stat.getPath().toString());
    if (stat.isDirectory()) {
      bld.setFileType(FileStatusProto.FileType.FT_DIR);
    } else if (stat.isSymlink()) {
      bld.setFileType(FileStatusProto.FileType.FT_SYMLINK)
          .setSymlink(stat.getSymlink().toString());
    } else {
      bld.setFileType(FileStatusProto.FileType.FT_FILE)
          .setLength(stat.getLen())
          .setBlockReplication(stat.getReplication())
          .setBlockSize(stat.getBlockSize());
    }
    bld.setAccessTime(stat.getAccessTime())
        .setModificationTime(stat.getModificationTime())
        .setOwner(stat.getOwner())
        .setGroup(stat.getGroup())
        .setPermission(convertPermission(stat.getPermission()));
    int flags = 0;
    flags |= stat.hasAcl() ? FileStatusProto.Flags.HAS_ACL_VALUE : 0;
    flags |= stat.isEncrypted() ? FileStatusProto.Flags.HAS_CRYPT_VALUE : 0;
    flags |= stat.isErasureCoded() ? FileStatusProto.Flags.HAS_EC_VALUE : 0;
    flags |= stat.isSnapshotEnabled() ? FileStatusProto.Flags
        .SNAPSHOT_ENABLED_VALUE : 0;
    bld.setFlags(flags);
    return bld.build();
  }

  private static FSProtos.FsPermissionProto convertPermission(FsPermission p)
      throws IOException {
    FSProtos.FsPermissionProto.Builder bld =
        FSProtos.FsPermissionProto.newBuilder();
    bld.setPerm(p.toShort());
    return bld.build();
  }

  private static FsPermission
      convertPermission(FSProtos.FsPermissionProto proto) throws IOException {
    return new FsPermission((short) proto.getPerm());
  }
}
