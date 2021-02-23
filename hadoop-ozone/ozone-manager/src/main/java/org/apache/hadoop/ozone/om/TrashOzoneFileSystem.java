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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.addTrailingSlashIfNeeded;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.pathToKey;

/**
 * FileSystem to be used by the Trash Emptier.
 * Only the apis used by the trash emptier are implemented.
 */
public class TrashOzoneFileSystem extends FileSystem {

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private static final int OZONE_FS_ITERATE_BATCH_SIZE = 100;

  private final OzoneManager ozoneManager;

  private final String userName;

  private final AtomicLong runCount;

  private static final ClientId CLIENT_ID = ClientId.randomId();

  private static final Logger LOG =
      LoggerFactory.getLogger(TrashOzoneFileSystem.class);

  public TrashOzoneFileSystem(OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    this.userName =
          UserGroupInformation.getCurrentUser().getShortUserName();
    this.runCount = new AtomicLong(0);
  }

  private RaftClientRequest getRatisRequest(
      OzoneManagerProtocolProtos.OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(CLIENT_ID)
        .setServerId(ozoneManager.getOmRatisServer().getRaftPeerId())
        .setGroupId(ozoneManager.getOmRatisServer().getRaftGroupId())
        .setCallId(runCount.getAndIncrement())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();

  }

  private void submitRequest(OzoneManagerProtocolProtos.OMRequest omRequest)
      throws Exception {
    ozoneManager.getMetrics().incNumTrashWriteRequests();
    if (ozoneManager.isRatisEnabled()) {
      OMClientRequest omClientRequest =
          OzoneManagerRatisUtils.createClientRequest(omRequest);
      omRequest = omClientRequest.preExecute(ozoneManager);
      RaftClientRequest req = getRatisRequest(omRequest);
      ozoneManager.getOmRatisServer().submitRequest(omRequest, req);
    } else {
      ozoneManager.getOmServerProtocol().
          submitRequest(NULL_RPC_CONTROLLER, omRequest);
    }
  }

  @Override
  public URI getUri() {
    throw new UnsupportedOperationException(
        "fs.getUri() not implemented in TrashOzoneFileSystem");
  }

  @Override
  public FSDataInputStream open(Path path, int i) {
    throw new UnsupportedOperationException(
        "fs.open() not implemented in TrashOzoneFileSystem");
  }

  @Override
  public FSDataOutputStream create(Path path,
      FsPermission fsPermission,
      boolean b, int i, short i1,
      long l, Progressable progressable){
    throw new UnsupportedOperationException(
        "fs.create() not implemented in TrashOzoneFileSystem");
  }

  @Override
  public FSDataOutputStream append(Path path, int i,
      Progressable progressable) {
    throw new UnsupportedOperationException(
        "fs.append() not implemented in TrashOzoneFileSystem");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    ozoneManager.getMetrics().incNumTrashRenames();
    LOG.trace("Src:" + src + "Dst:" + dst);
    // check whether the src and dst belong to the same bucket & trashroot.
    OFSPath srcPath = new OFSPath(src);
    OFSPath dstPath = new OFSPath(dst);
    Preconditions.checkArgument(srcPath.getBucketName().
        equals(dstPath.getBucketName()));
    Preconditions.checkArgument(srcPath.getTrashRoot().
        toString().equals(dstPath.getTrashRoot().toString()));
    RenameIterator iterator = new RenameIterator(src, dst);
    iterator.iterate();
    return true;
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    ozoneManager.getMetrics().incNumTrashDeletes();
    DeleteIterator iterator = new DeleteIterator(path, true);
    iterator.iterate();
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws  IOException {
    ozoneManager.getMetrics().incNumTrashListStatus();
    List<FileStatus> fileStatuses = new ArrayList<>();
    OmKeyArgs keyArgs = constructOmKeyArgs(path);
    List<OzoneFileStatus> list = ozoneManager.
        listStatus(keyArgs, false, null, Integer.MAX_VALUE);
    for (OzoneFileStatus status : list) {
      FileStatus fileStatus = convertToFileStatus(status);
      fileStatuses.add(fileStatus);
    }
    return fileStatuses.toArray(new FileStatus[0]);
  }


  /**
   * converts OzoneFileStatus object to FileStatus.
   */
  private FileStatus convertToFileStatus(OzoneFileStatus status) {
    Path temp = new Path(OZONE_URI_DELIMITER +
        status.getKeyInfo().getVolumeName() +
        OZONE_URI_DELIMITER +
        status.getKeyInfo().getBucketName() +
        OZONE_URI_DELIMITER +
        status.getKeyInfo().getKeyName());
    return new FileStatus(
        status.getKeyInfo().getDataSize(),
        status.isDirectory(),
        status.getKeyInfo().getFactor().getNumber(),
        status.getBlockSize(),
        status.getKeyInfo().getModificationTime(),
        temp
    );
  }

  @Override
  public void setWorkingDirectory(Path path) {
    throw new UnsupportedOperationException(
        "fs.setWorkingDirectory() not implemented in TrashOzoneFileSystem");
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException(
        "fs.getWorkingDirectory() not implemented in TrashOzoneFileSystem");
  }

  @Override
  public boolean mkdirs(Path path,
      FsPermission fsPermission) {
    throw new UnsupportedOperationException(
        "fs.mkdirs() not implemented in TrashOzoneFileSystem");
  }


  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    ozoneManager.getMetrics().incNumGetFileStatus();
    OmKeyArgs keyArgs = constructOmKeyArgs(path);
    OzoneFileStatus ofs = ozoneManager.getKeyManager().getFileStatus(keyArgs);
    FileStatus fileStatus = convertToFileStatus(ofs);
    return fileStatus;
  }

  private OmKeyArgs constructOmKeyArgs(Path path) {
    OFSPath ofsPath = new OFSPath(path);
    String volume = ofsPath.getVolumeName();
    String bucket = ofsPath.getBucketName();
    String key = ofsPath.getKeyName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .build();
    return keyArgs;
  }

  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    Preconditions.checkArgument(allUsers);
    ozoneManager.getMetrics().incNumTrashGetTrashRoots();
    Iterator<Map.Entry<CacheKey<String>,
        CacheValue<OmBucketInfo>>> bucketIterator =
        ozoneManager.getMetadataManager().getBucketIterator();
    List<FileStatus> ret = new ArrayList<>();
    while (bucketIterator.hasNext()){
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry =
          bucketIterator.next();
      OmBucketInfo omBucketInfo = entry.getValue().getCacheValue();
      Path volumePath = new Path(OZONE_URI_DELIMITER,
          omBucketInfo.getVolumeName());
      Path bucketPath = new Path(volumePath, omBucketInfo.getBucketName());
      Path trashRoot = new Path(bucketPath, FileSystem.TRASH_PREFIX);
      try {
        if (exists(trashRoot)) {
          FileStatus[] list = this.listStatus(trashRoot);
          for (FileStatus candidate : list) {
            if (exists(candidate.getPath()) && candidate.isDirectory()) {
              ret.add(candidate);
            }
          }
        }
      } catch (Exception e){
        LOG.error("Couldn't perform fs operation " +
            "fs.listStatus()/fs.exists()", e);
      }
    }
    return ret;
  }

  @Override
  public boolean exists(Path f) throws IOException {
    ozoneManager.getMetrics().incNumTrashExists();
    try {
      this.getFileStatus(f);
      return true;
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        LOG.trace("Couldn't execute getFileStatus()", e);
        return false;
      } else {
        throw e;
      }
    }
  }

  private abstract class OzoneListingIterator {
    private final Path path;
    private final FileStatus status;
    private String pathKey;
    private TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
          keyIterator;

    OzoneListingIterator(Path path)
          throws IOException {
      this.path = path;
      this.status = getFileStatus(path);
      this.pathKey = pathToKey(path);
      if (status.isDirectory()) {
        this.pathKey = addTrailingSlashIfNeeded(pathKey);
      }
      keyIterator = ozoneManager.getMetadataManager().getKeyIterator();
    }

      /**
       * The output of processKey determines if further iteration through the
       * keys should be done or not.
       *
       * @return true if we should continue iteration of keys, false otherwise.
       * @throws IOException
       */
    abstract boolean processKeyPath(List<String> keyPathList)
          throws IOException;

      /**
       * Iterates through all the keys prefixed with the input path's key and
       * processes the key though processKey().
       * If for any key, the processKey() returns false, then the iteration is
       * stopped and returned with false indicating that all the keys could not
       * be processed successfully.
       *
       * @return true if all keys are processed successfully, false otherwise.
       * @throws IOException
       */
    boolean iterate() throws IOException {
      LOG.trace("Iterating path: {}", path);
      List<String> keyPathList = new ArrayList<>();
      if (status.isDirectory()) {
        LOG.trace("Iterating directory: {}", pathKey);
        OFSPath ofsPath = new OFSPath(pathKey);
        String ofsPathprefix =
            ofsPath.getNonKeyPathNoPrefixDelim() + OZONE_URI_DELIMITER;
        while (keyIterator.hasNext()) {
          Table.KeyValue< String, OmKeyInfo > kv = keyIterator.next();
          String keyPath = ofsPathprefix + kv.getValue().getKeyName();
          LOG.trace("iterating key path: {}", keyPath);
          if (!kv.getValue().getKeyName().equals("")
              && kv.getKey().startsWith("/" + pathKey)) {
            keyPathList.add(keyPath);
          }
          if (keyPathList.size() >= OZONE_FS_ITERATE_BATCH_SIZE) {
            if (!processKeyPath(keyPathList)) {
              return false;
            } else {
              keyPathList.clear();
            }
          }
        }
        if (keyPathList.size() > 0) {
          if (!processKeyPath(keyPathList)) {
            return false;
          }
        }
        return true;
      } else {
        LOG.trace("iterating file: {}", path);
        keyPathList.add(pathKey);
        return processKeyPath(keyPathList);
      }
    }

    FileStatus getStatus() {
      return status;
    }
  }

  private class RenameIterator extends OzoneListingIterator {
    private final String srcPath;
    private final String dstPath;

    RenameIterator(Path srcPath, Path dstPath)
        throws IOException {
      super(srcPath);
      this.srcPath = pathToKey(srcPath);
      this.dstPath = pathToKey(dstPath);
      LOG.trace("rename from:{} to:{}", this.srcPath, this.dstPath);
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) {
      for (String keyPath : keyPathList) {
        String newPath = dstPath.concat(keyPath.substring(srcPath.length()));
        OFSPath src = new OFSPath(keyPath);
        OFSPath dst = new OFSPath(newPath);

        OzoneManagerProtocolProtos.OMRequest omRequest =
            getRenameKeyRequest(src, dst);
        try {
          ozoneManager.getMetrics().incNumTrashFilesRenames();
          submitRequest(omRequest);
        } catch (Throwable e) {
          LOG.error("Couldn't send rename request.", e);
        }

      }
      return true;
    }

    private OzoneManagerProtocolProtos.OMRequest
        getRenameKeyRequest(
        OFSPath src, OFSPath dst) {
      String volumeName = src.getVolumeName();
      String bucketName = src.getBucketName();
      String keyName = src.getKeyName();

      OzoneManagerProtocolProtos.KeyArgs keyArgs =
          OzoneManagerProtocolProtos.KeyArgs.newBuilder()
              .setKeyName(keyName)
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .build();
      String toKeyName = dst.getKeyName();
      OzoneManagerProtocolProtos.RenameKeyRequest renameKeyRequest =
          OzoneManagerProtocolProtos.RenameKeyRequest.newBuilder()
              .setKeyArgs(keyArgs)
              .setToKeyName(toKeyName)
              .build();
      OzoneManagerProtocolProtos.OMRequest omRequest =
          null;
      try {
        omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setClientId(CLIENT_ID.toString())
            .setUserInfo(getUserInfo())
            .setRenameKeyRequest(renameKeyRequest)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .build();
      } catch (IOException e) {
        LOG.error("Couldn't get userinfo", e);
      }
      return omRequest;
    }
  }

  private class DeleteIterator extends OzoneListingIterator {
    private final boolean recursive;
    private List<String> keysList;


    DeleteIterator(Path f, boolean recursive)
        throws IOException {
      super(f);
      this.recursive = recursive;
      keysList = new ArrayList<>();
      if (getStatus().isDirectory()
          && !this.recursive
          && listStatus(f).length != 0) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) {
      LOG.trace("Deleting keys: {}", keyPathList);
      for (String keyPath : keyPathList) {
        OFSPath path = new OFSPath(keyPath);
        OzoneManagerProtocolProtos.OMRequest omRequest =
            getDeleteKeyRequest(path);
        try {
          ozoneManager.getMetrics().incNumTrashFilesDeletes();
          submitRequest(omRequest);
        } catch (Throwable e) {
          LOG.error("Couldn't send rename request.", e);
        }
      }
      return true;
    }

    private OzoneManagerProtocolProtos.OMRequest
        getDeleteKeyRequest(
        OFSPath keyPath) {
      String volumeName = keyPath.getVolumeName();
      String bucketName = keyPath.getBucketName();
      String keyName = keyPath.getKeyName();
      keysList.clear();
      // Keys List will have only 1 entry.
      keysList.add(keyName);
      OzoneManagerProtocolProtos.DeleteKeyArgs.Builder deleteKeyArgs =
          OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
              .setBucketName(bucketName)
              .setVolumeName(volumeName);
      deleteKeyArgs.addAllKeys(keysList);
      OzoneManagerProtocolProtos.DeleteKeysRequest deleteKeysRequest =
          OzoneManagerProtocolProtos.DeleteKeysRequest.newBuilder()
              .setDeleteKeys(deleteKeyArgs)
              .build();
      OzoneManagerProtocolProtos.OMRequest omRequest =
          null;
      try {
        omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setClientId(CLIENT_ID.toString())
            .setUserInfo(getUserInfo())
            .setDeleteKeysRequest(deleteKeysRequest)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKeys)
            .build();
      } catch (IOException e) {
        LOG.error("Couldn't get userinfo", e);
      }
      return omRequest;
    }
  }

  OzoneManagerProtocolProtos.UserInfo getUserInfo() throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    InetAddress remoteAddress = ozoneManager.getOmRpcServerAddr().getAddress();
    OzoneManagerProtocolProtos.UserInfo.Builder userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder();
    if (user != null) {
      userInfo.setUserName(user.getUserName());
    }

    if (remoteAddress != null) {
      userInfo.setHostName(remoteAddress.getHostName());
      userInfo.setRemoteAddress(remoteAddress.getHostAddress());
    }

    return userInfo.build();
  }



}
