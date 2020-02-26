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
package org.apache.hadoop.fs.ozone.nfs.mount;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ozone.BasicOzoneFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.fs.ozone.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.fs.ozone.nfs.conf.NfsConfiguration;
import org.apache.hadoop.fs.ozone.nfs.nfs3.Nfs3Utils;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.mount.MountEntry;
import org.apache.hadoop.mount.MountInterface;
import org.apache.hadoop.mount.MountResponse;
import org.apache.hadoop.nfs.AccessPrivilege;
import org.apache.hadoop.nfs.NfsExports;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcInfo;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcResponse;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;

import com.google.common.annotations.VisibleForTesting;

/**
 * RPC program corresponding to mountd daemon. See {@link Mountd}.
 */
public class RpcProgramMountd extends RpcProgram implements MountInterface {
  private static final Logger LOG =
      LoggerFactory.getLogger(RpcProgramMountd.class);
  public static final int PROGRAM = 100005;
  public static final int VERSION_1 = 1;
  public static final int VERSION_2 = 2;
  public static final int VERSION_3 = 3;

  /** Synchronized list. */
  private final List<MountEntry> mounts;

  /** List that is unmodifiable. */
  private final HashMap<String, URI> exports;
  private final NfsConfiguration config;
  private final NfsExports hostsMatcher;

  public RpcProgramMountd(NfsConfiguration config,
      DatagramSocket registrationSocket, boolean allowInsecurePorts)
      throws IOException {
    // Note that RPC cache is not enabled
    super("mountd", "localhost", config.getInt(
        NfsConfigKeys.DFS_NFS_MOUNTD_PORT_KEY,
        NfsConfigKeys.DFS_NFS_MOUNTD_PORT_DEFAULT), PROGRAM, VERSION_1,
        VERSION_3, registrationSocket, allowInsecurePorts, config.getInt(
              NfsConfigKeys.NFS_UDP_CLIENT_PORTMAP_TIMEOUT_MILLIS_KEY,
              NfsConfigKeys.NFS_UDP_CLIENT_PORTMAP_TIMEOUT_MILLIS_DEFAULT));
    this.config = config;
    exports = new HashMap<>();
    addExports();
    this.hostsMatcher = NfsExports.getInstance(config);
    this.mounts = Collections.synchronizedList(new ArrayList<MountEntry>());
    UserGroupInformation.setConfiguration(config);
    SecurityUtil.login(config, NfsConfigKeys.DFS_NFS_KEYTAB_FILE_KEY,
        NfsConfigKeys.DFS_NFS_KERBEROS_PRINCIPAL_KEY);
  }

  private void addExports() throws IOException {
    // This requires default.FS to be set.
    FileSystem fs = BasicOzoneFileSystem.get(config);
    String[] exportsPath =
        config.getStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY,
            NfsConfigKeys.DFS_NFS_EXPORT_POINT_DEFAULT);
    for (String exportPath : exportsPath) {
      URI exportURI = Nfs3Utils.getResolvedURI(fs, exportPath);
      LOG.info("FS:" + fs.getScheme() + " adding export Path:" + exportPath +
          " with URI: " + exportURI.toString());
      exports.put(exportPath, exportURI);
    }
  }

  @Override
  public XDR nullOp(XDR out, int xid, InetAddress client) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT NULLOP : " + " client: " + client);
    }
    return RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(
        out);
  }

  @Override
  public XDR mnt(XDR xdr, XDR out, int xid, InetAddress client) {
    if (hostsMatcher == null) {
      return MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_ACCES, out, xid,
          null);
    }
    AccessPrivilege accessPrivilege = hostsMatcher.getAccessPrivilege(client);
    if (accessPrivilege == AccessPrivilege.NONE) {
      return MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_ACCES, out, xid,
          null);
    }

    String path = xdr.readString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT MNT path: " + path + " client: " + client);
    }

    String host = client.getHostName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got host: " + host + " path: " + path);
    }
    URI exportURI = exports.get(path);
    if (exportURI == null) {
      LOG.info("Path " + path + " is not shared.");
      MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_NOENT, out, xid, null);
      return out;
    }

    BasicOzoneFileSystem bofs;
    // Pseudocode for Ozone/GenericFS specific handling
    try {
      FileSystem fs = FileSystem.get(exportURI, config);
      bofs = (BasicOzoneFileSystem) fs;
    } catch (IOException e) {
      LOG.error("Can't get handle for export:" + path, e);
      MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_NOENT, out, xid, null);
      return out;
    }

    FileHandle handle = null;
    try {
      // need to modify hadoop base FileSystem and BasicOzoneFileSystem
      Path root = new Path("/");
      String fileHandleInfo = bofs.getFileHandleInfo(root);
      if (fileHandleInfo != null) {
        handle = new FileHandle(fileHandleInfo);
      } else {
        LOG.error("Can't get handle for export:" + path);
        MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_NOENT, out, xid, null);
        return out;
      }
    } catch (IOException e) {
      LOG.error("Can't get handle for export:" + path, e);
      MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_NOENT, out, xid, null);
      return out;
    }

    assert (handle != null);
    LOG.info("Giving handle (fileHandle:" + handle.dumpFileHandle()
        + " file URI: " + exportURI
        + ") to client for export " + path);
    mounts.add(new MountEntry(host, path));

    MountResponse.writeMNTResponse(Nfs3Status.NFS3_OK, out, xid,
        handle.getContent());
    return out;
  }

  @Override
  public XDR dump(XDR out, int xid, InetAddress client) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT NULLOP : " + " client: " + client);
    }

    List<MountEntry> copy = new ArrayList<MountEntry>(mounts);
    MountResponse.writeMountList(out, xid, copy);
    return out;
  }

  @Override
  public XDR umnt(XDR xdr, XDR out, int xid, InetAddress client) {
    String path = xdr.readString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT UMNT path: " + path + " client: " + client);
    }
    
    String host = client.getHostName();
    mounts.remove(new MountEntry(host, path));
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(out);
    return out;
  }

  @Override
  public XDR umntall(XDR out, int xid, InetAddress client) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT UMNTALL : " + " client: " + client);
    }
    mounts.clear();
    return RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(
        out);
  }

  @Override
  public void handleInternal(ChannelHandlerContext ctx, RpcInfo info) {
    RpcCall rpcCall = (RpcCall) info.header();
    final MNTPROC mntproc = MNTPROC.fromValue(rpcCall.getProcedure());
    int xid = rpcCall.getXid();
    byte[] data = new byte[info.data().readableBytes()];
    info.data().readBytes(data);
    XDR xdr = new XDR(data);
    XDR out = new XDR();
    InetAddress client =
        ((InetSocketAddress) info.remoteAddress()).getAddress();

    if (mntproc == MNTPROC.NULL) {
      out = nullOp(out, xid, client);
    } else if (mntproc == MNTPROC.MNT) {
      // Only do port monitoring for MNT
      if (!doPortMonitoring(info.remoteAddress())) {
        out = MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_ACCES, out,
            xid, null);
      } else {
        out = mnt(xdr, out, xid, client);
      }
    } else if (mntproc == MNTPROC.DUMP) {
      out = dump(out, xid, client);
    } else if (mntproc == MNTPROC.UMNT) {      
      out = umnt(xdr, out, xid, client);
    } else if (mntproc == MNTPROC.UMNTALL) {
      umntall(out, xid, client);
    } else if (mntproc == MNTPROC.EXPORT) {
      // Currently only support one NFS export per namenode
      List<NfsExports> hostsMatchers = new ArrayList<NfsExports>();
      if (hostsMatcher != null) {
        List exportsList = getExports();
        for (int i = 0; i < exportsList.size(); i++) {
          hostsMatchers.add(hostsMatcher);
        }
        out = MountResponse.writeExportList(out, xid,
                                            exportsList, hostsMatchers);
      } else {
        // This means there are no valid exports provided.
        RpcAcceptedReply.getInstance(xid,
            RpcAcceptedReply.AcceptState.PROC_UNAVAIL, new VerifierNone())
            .write(out);
      }
    } else {
      // Invalid procedure
      RpcAcceptedReply.getInstance(xid,
          RpcAcceptedReply.AcceptState.PROC_UNAVAIL, new VerifierNone()).write(
          out);
    }
    ChannelBuffer buf =
        ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap().buffer());
    RpcResponse rsp = new RpcResponse(buf, info.remoteAddress());
    RpcUtil.sendRpcResponse(ctx, rsp);
  }
  
  @Override
  protected boolean isIdempotent(RpcCall call) {
    // Not required, because cache is turned off
    return false;
  }

  @VisibleForTesting
  public List<String> getExports() {
    return new ArrayList<>(this.exports.keySet());
  }
}
