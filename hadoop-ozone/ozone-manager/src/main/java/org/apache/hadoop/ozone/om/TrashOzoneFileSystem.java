package org.apache.hadoop.ozone.om;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.addTrailingSlashIfNeeded;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.pathToKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;

/**
 * FileSystem to be used by the Trash Emptier.
 * Only the apis used by the trash emptier are implemented.
 */
public class TrashOzoneFileSystem extends FileSystem {

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private OzoneManager ozoneManager;

  private String userName;


  private static final Logger LOG =
      LoggerFactory.getLogger(TrashOzoneFileSystem.class);

  public TrashOzoneFileSystem(OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    this.userName =
          UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @Override
  public URI getUri() {
    return null;
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path path,
      FsPermission fsPermission,
      boolean b, int i, short i1,
      long l, Progressable progressable) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable)
      throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("Src:" + src + "Dst:" + dst);
    RenameIterator iterator = new RenameIterator(src, dst);
    iterator.iterate();
    return true;
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    DeleteIterator iterator = new DeleteIterator(path, true);
    iterator.iterate();
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws  IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    OFSPath ofsPath = new OFSPath(path);
    String volume = ofsPath.getVolumeName();
    String bucket = ofsPath.getBucketName();
    String key =  ofsPath.getKeyName();
    String ofsPathPrefix = "/" + volume + "/" + bucket;
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .build();
    List<OzoneFileStatus> list = ozoneManager.
        listStatus(keyArgs, false, null, Integer.MAX_VALUE);
    for (OzoneFileStatus status : list) {
      Path temp = new Path(ofsPathPrefix +
          OZONE_URI_DELIMITER + status.getKeyInfo().getKeyName());
      FileStatus fileStatus = new FileStatus(
          status.getKeyInfo().getDataSize(),
          status.isDirectory(),
          status.getKeyInfo().getFactor().getNumber(),
          status.getBlockSize(),
          status.getKeyInfo().getModificationTime(),
          temp
      );
      fileStatuses.add(fileStatus);

    }
    return fileStatuses.toArray(new FileStatus[0]);
  }

  @Override
  public void setWorkingDirectory(Path path) {

  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path,
      FsPermission fsPermission) throws IOException {
    return false;
  }


  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    OFSPath ofsPath = new OFSPath(path);
    String volume = ofsPath.getVolumeName();
    String bucket = ofsPath.getBucketName();
    String key =  ofsPath.getKeyName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .build();
    String ofsPathPrefix = OZONE_URI_DELIMITER +
        volume + OZONE_URI_DELIMITER + bucket;
    OzoneFileStatus ofs = ozoneManager.getKeyManager().getFileStatus(keyArgs);
    Path temp = new Path(ofsPathPrefix +
        OZONE_URI_DELIMITER + ofs.getKeyInfo().getKeyName());
    FileStatus fileStatus = new FileStatus(ofs.getKeyInfo().getDataSize(),
        ofs.isDirectory(),
        ofs.getKeyInfo().getFactor().getNumber(),
        ofs.getBlockSize(),
        ofs.getKeyInfo().getModificationTime(),
        temp);
    return fileStatus;
  }

  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
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
      LOG.info(trashRoot.toString());
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
            "fs.listStatus()/fs.exists()" + e);
      }
    }
    return ret;
  }

  @Override
  public boolean exists(Path f) throws IOException {
    try {
      return this.getFileStatus(f) != null;
    } catch (FileNotFoundException var3) {
      LOG.info("Couldn't execute getFileStatus()"  + var3);
      return false;
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
      //hardcoded make it read from conf
      int batchSize = 100;
      if (status.isDirectory()) {
        LOG.trace("Iterating directory: {}", pathKey);
        OFSPath ofsPath = new OFSPath(pathKey);
        String ofsPathPrefix =
            ofsPath.getNonKeyPathNoPrefixDelim() + OZONE_URI_DELIMITER;
        while (keyIterator.hasNext()) {
          Table.KeyValue< String, OmKeyInfo > kv = keyIterator.next();
          String keyPath = ofsPathPrefix + kv.getValue().getKeyName();
          LOG.trace("iterating key path: {}", keyPath);
          String k = kv.getKey();
          if (!kv.getValue().getKeyName().equals("")
              && kv.getKey().startsWith("/" + pathKey)) {
            keyPathList.add(keyPath);
          }
          if (keyPathList.size() >= batchSize) {
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
      // Initialize bucket here to reduce number of RPC calls
      // TODO: Refactor later.
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) throws IOException {
      for (String keyPath : keyPathList) {
        String newPath = dstPath.concat(keyPath.substring(srcPath.length()));
        LOG.info(newPath);
        OFSPath src = new OFSPath(keyPath);
        OFSPath dst = new OFSPath(newPath);

        String volumeName = src.getVolumeName();
        String bucketName = src.getBucketName();
        String keyName = src.getKeyName();

        OzoneManagerProtocolProtos.KeyArgs keyArgs =
            OzoneManagerProtocolProtos.KeyArgs.newBuilder().setKeyName(keyName)
            .setVolumeName(volumeName).setBucketName(bucketName).build();

        String toKeyName = dst.getKeyName();

        OzoneManagerProtocolProtos.RenameKeyRequest renameKeyRequest =
            OzoneManagerProtocolProtos.RenameKeyRequest.newBuilder()
            .setKeyArgs(keyArgs).setToKeyName(toKeyName).build();
        OzoneManagerProtocolProtos.OMRequest omRequest =
            OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setClientId(UUID.randomUUID().toString())
            .setRenameKeyRequest(renameKeyRequest)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey).build();
        try {
          ozoneManager.getOmServerProtocol().
              submitRequest(NULL_RPC_CONTROLLER, omRequest);
        } catch (ServiceException e) {
          LOG.error("Couldn't send rename request.");
        }

      }
      return true;
    }
  }

  private class DeleteIterator extends OzoneListingIterator {
    final private boolean recursive;


    DeleteIterator(Path f, boolean recursive)
        throws IOException {
      super(f);
      this.recursive = recursive;
      if (getStatus().isDirectory()
          && !this.recursive
          && listStatus(f).length != 0) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
      // Initialize bucket here to reduce number of RPC calls
      OFSPath ofsPath = new OFSPath(f);
      // TODO: Refactor later.
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) {
      LOG.trace("Deleting keys: {}", keyPathList);

      String volumeName;
      String bucketName;
      List<String> keyList = keyPathList.stream()
          .map(p -> new OFSPath(p).getKeyName())
          .collect(Collectors.toList());

      if(!keyPathList.isEmpty()){
        OFSPath p = new OFSPath(keyPathList.get(0));
        volumeName = p.getVolumeName();
        bucketName = p.getBucketName();
        OzoneManagerProtocolProtos.DeleteKeyArgs.Builder deleteKeyArgs =
            OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName);
        deleteKeyArgs.addAllKeys(keyList);
        OzoneManagerProtocolProtos.OMRequest omRequest =
            OzoneManagerProtocolProtos.OMRequest.newBuilder()
                .setClientId(UUID.randomUUID().toString())
                .setCmdType(DeleteKeys)
                .setDeleteKeysRequest(OzoneManagerProtocolProtos
                    .DeleteKeysRequest.newBuilder()
                    .setDeleteKeys(deleteKeyArgs).build()).build();
        try {
          ozoneManager.getOmServerProtocol().
              submitRequest(NULL_RPC_CONTROLLER, omRequest);
        } catch (ServiceException e) {
          LOG.error("Couldn't send rename request.");
        }
        return true;
      } else {
        LOG.info("No keys to process");
        return true;
      }

    }
  }



}
