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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_O3TRASH_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.addTrailingSlashIfNeeded;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.pathToKey;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSystem to be used by the Trash Emptier.
 * Only the apis used by the trash emptier are implemented.
 */
public class TrashOzoneFileSystem extends FileSystem {

  private static final int OZONE_FS_ITERATE_BATCH_SIZE = 100;

  private static final int OZONE_MAX_LIST_KEYS_SIZE = 10000;

  private final OzoneManager ozoneManager;

  private final AtomicLong runCount;

  private static final ClientId CLIENT_ID = ClientId.randomId();

  private static final Logger LOG =
      LoggerFactory.getLogger(TrashOzoneFileSystem.class);

  private final OzoneConfiguration ozoneConfiguration;

  public TrashOzoneFileSystem(OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    this.runCount = new AtomicLong(0);
    setConf(ozoneManager.getConfiguration());
    ozoneConfiguration = OzoneConfiguration.of(getConf());
  }

  private void submitRequest(OzoneManagerProtocolProtos.OMRequest omRequest)
      throws Exception {
    ozoneManager.getMetrics().incNumTrashWriteRequests();
    // perform preExecute as ratis submit do no perform preExecute
    OMClientRequest omClientRequest = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager);
    omRequest = omClientRequest.preExecute(ozoneManager);
    OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, CLIENT_ID, runCount.getAndIncrement());
  }

  @Override
  public URI getUri() {
    return URI.create(OZONE_O3TRASH_URI_SCHEME + ":///");
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
      long l, Progressable progressable) {
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
    OFSPath srcPath = new OFSPath(src, ozoneConfiguration);
    OFSPath dstPath = new OFSPath(dst, ozoneConfiguration);
    OmBucketInfo bucket = ozoneManager.getBucketInfo(srcPath.getVolumeName(),
        srcPath.getBucketName());
    if (bucket.getBucketLayout().isFileSystemOptimized()) {
      return renameFSO(srcPath, dstPath);
    }
    Preconditions.checkArgument(srcPath.getBucketName().
        equals(dstPath.getBucketName()));
    Preconditions.checkArgument(srcPath.getTrashRoot().
        toString().equals(dstPath.getTrashRoot().toString()));
    RenameIterator iterator = new RenameIterator(src, dst);
    iterator.iterate();
    return true;
  }

  private boolean renameFSO(OFSPath srcPath, OFSPath dstPath) {
    ozoneManager.getMetrics().incNumTrashAtomicDirRenames();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        getRenameKeyRequest(srcPath, dstPath);
    try {
      if (omRequest != null) {
        submitRequest(omRequest);
        return true;
      }
      return false;
    } catch (Exception e) {
      LOG.error("Couldn't send rename request", e);
      return false;
    }
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    ozoneManager.getMetrics().incNumTrashDeletes();
    OFSPath srcPath = new OFSPath(path, ozoneConfiguration);
    OmBucketInfo bucket = ozoneManager.getBucketInfo(srcPath.getVolumeName(),
        srcPath.getBucketName());
    if (bucket.getBucketLayout().isFileSystemOptimized()) {
      return deleteFSO(srcPath);
    }
    DeleteIterator iterator = new DeleteIterator(path, true);
    iterator.iterate();
    return true;
  }

  private boolean deleteFSO(OFSPath srcPath) {
    ozoneManager.getMetrics().incNumTrashAtomicDirDeletes();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        getDeleteKeyRequest(srcPath);
    try {
      if (omRequest != null) {
        submitRequest(omRequest);
        return true;
      }
      return false;
    } catch (Throwable e) {
      LOG.error("Couldn't send delete request.", e);
      return false;
    }
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
        status.getKeyInfo().getReplicationConfig().getRequiredNodes(),
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
    OFSPath ofsPath = new OFSPath(path, ozoneConfiguration);
    String volume = ofsPath.getVolumeName();
    String bucket = ofsPath.getBucketName();
    String key = ofsPath.getKeyName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .setHeadOp(true)
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
    while (bucketIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry =
          bucketIterator.next();
      OmBucketInfo omBucketInfo = entry.getValue().getCacheValue();
      if (omBucketInfo == null) {
        continue;
      }
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
      } catch (Exception e) {
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
    private Iterator<String> keyIterator;

    OzoneListingIterator(Path path)
          throws IOException {
      this.path = path;
      this.status = getFileStatus(path);
      this.pathKey = pathToKey(path);
      if (status.isDirectory()) {
        this.pathKey = addTrailingSlashIfNeeded(pathKey);
      }
      OFSPath fsPath = new OFSPath(pathKey,
          ozoneConfiguration);
      keyIterator =
          getKeyIterator(fsPath.getVolumeName(), fsPath.getBucketName(),
              fsPath.getKeyName());
    }

    private Iterator<String> getKeyIterator(String volumeName,
        String bucketName, String keyName) throws IOException {
      List<String> keys = new ArrayList<>(
          listKeys(volumeName, bucketName, "", keyName));
      String lastKey = keys.get(keys.size() - 1);
      List<String> nextBatchKeys =
          listKeys(volumeName, bucketName, lastKey, keyName);

      while (!nextBatchKeys.isEmpty()) {
        keys.addAll(nextBatchKeys);
        lastKey = nextBatchKeys.get(nextBatchKeys.size() - 1);
        nextBatchKeys = listKeys(volumeName, bucketName, lastKey, keyName);
      }
      return keys.iterator();
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
        OFSPath ofsPath = new OFSPath(pathKey,
            ozoneConfiguration);
        String ofsPathprefix =
            ofsPath.getNonKeyPathNoPrefixDelim() + OZONE_URI_DELIMITER;
        while (keyIterator.hasNext()) {
          String keyName = keyIterator.next();
          String keyPath = ofsPathprefix + keyName;
          LOG.trace("iterating key path: {}", keyPath);
          keyPathList.add(keyPath);
          if (keyPathList.size() >= OZONE_FS_ITERATE_BATCH_SIZE) {
            if (!processKeyPath(keyPathList)) {
              return false;
            } else {
              keyPathList.clear();
            }
          }
        }
        if (!keyPathList.isEmpty()) {
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

    /**
     * Return a listKeys output with only a list of keyNames.
     */
    List<String> listKeys(String volumeName, String bucketName, String startKey,
        String keyPrefix) throws IOException {
      OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
      return metadataManager.listKeys(volumeName, bucketName, startKey,
              keyPrefix, OZONE_MAX_LIST_KEYS_SIZE).getKeys().stream()
          .map(OmKeyInfo::getKeyName)
          .collect(Collectors.toList());
    }
  }

  /**
   * Returns a OMRequest builder with specified type.
   * @param cmdType type of the request
   */
  private OzoneManagerProtocolProtos.OMRequest.Builder
       createOMRequest(OzoneManagerProtocolProtos.Type cmdType) throws IOException {
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setClientId(CLIENT_ID.toString())
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setUserInfo(getUserInfo())
        .setCmdType(cmdType);
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
      omRequest = createOMRequest(OzoneManagerProtocolProtos.Type.RenameKey)
              .setRenameKeyRequest(renameKeyRequest)
              .build();
    } catch (IOException e) {
      LOG.error("Couldn't get userinfo", e);
    }
    return omRequest;
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
        OFSPath src = new OFSPath(keyPath,
            ozoneConfiguration);
        OFSPath dst = new OFSPath(newPath,
            ozoneConfiguration);

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
  }

  private OzoneManagerProtocolProtos.OMRequest getDeleteKeyRequest(
      OFSPath srcPath) {
    String volume = srcPath.getVolumeName();
    String bucket = srcPath.getBucketName();
    String key  = srcPath.getKeyName();
    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        OzoneManagerProtocolProtos.KeyArgs.newBuilder()
            .setKeyName(key)
            .setVolumeName(volume)
            .setBucketName(bucket)
            .setRecursive(true)
            .build();
    OzoneManagerProtocolProtos.DeleteKeyRequest deleteKeyRequest =
        OzoneManagerProtocolProtos.DeleteKeyRequest.newBuilder()
            .setKeyArgs(keyArgs).build();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        null;
    try {
      omRequest = createOMRequest(OzoneManagerProtocolProtos.Type.DeleteKey)
              .setDeleteKeyRequest(deleteKeyRequest)
              .build();
    } catch (IOException e) {
      LOG.error("Couldn't get userinfo", e);
    }
    return omRequest;
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
        OFSPath path = new OFSPath(keyPath,
            ozoneConfiguration);
        OzoneManagerProtocolProtos.OMRequest omRequest =
            getDeleteKeysRequest(path);
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
        getDeleteKeysRequest(
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
              .setSourceType(OzoneManagerProtocolProtos.RequestSource.TRASH)
              .build();
      OzoneManagerProtocolProtos.OMRequest omRequest =
          null;
      try {
        omRequest = createOMRequest(OzoneManagerProtocolProtos.Type.DeleteKeys)
            .setDeleteKeysRequest(deleteKeysRequest)
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
