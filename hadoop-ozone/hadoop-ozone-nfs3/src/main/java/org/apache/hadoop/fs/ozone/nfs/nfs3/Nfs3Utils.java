/**
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
package org.apache.hadoop.fs.ozone.nfs.nfs3;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.FileSystemException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * Utility/helper methods related to NFS
 */
public class Nfs3Utils {
  public final static String INODEID_PATH_PREFIX = "/.reserved/.inodes/";

  
  public final static String READ_RPC_START =  "READ_RPC_CALL_START____";
  public final static String READ_RPC_END =    "READ_RPC_CALL_END______";
  public final static String WRITE_RPC_START = "WRITE_RPC_CALL_START____";
  public final static String WRITE_RPC_END =   "WRITE_RPC_CALL_END______";
  
  public static URI getResolvedURI(FileSystem fs, String exportPath)
      throws IOException {
    URI fsURI = fs.getUri();
    String scheme = fs.getScheme();
    if (scheme.equalsIgnoreCase(FsConstants.VIEWFS_SCHEME)) {
      ViewFileSystem viewFs = (ViewFileSystem)fs;
      ViewFileSystem.MountPoint[] mountPoints = viewFs.getMountPoints();
      for (ViewFileSystem.MountPoint mount : mountPoints) {
        String mountedPath = mount.getMountedOnPath().toString();
        if (exportPath.startsWith(mountedPath)) {
          String subpath = exportPath.substring(mountedPath.length());
          fsURI = mount.getTargetFileSystemURIs()[0].resolve(subpath);
          break;
        }
      }
    } else if (scheme.equalsIgnoreCase(OzoneConsts.OZONE_URI_SCHEME)) {
      fsURI = fsURI.resolve(exportPath);
    }

    if (!fsURI.getScheme().equalsIgnoreCase(OzoneConsts.OZONE_URI_SCHEME)) {
      throw new FileSystemException("Only Ozone is supported as underlying"
          + "FileSystem, fs scheme:" + scheme + " uri to be added" + fsURI);
    }
    return fsURI;
  }
}
