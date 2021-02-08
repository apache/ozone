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
package org.apache.hadoop.fs.ozone;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .VOLUME_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .BUCKET_NOT_FOUND;

/**
 * Basic Implementation of the RootedOzoneFileSystem calls.
 * <p>
 * This is the minimal version which doesn't include any statistics.
 * <p>
 * For full featured version use RootedOzoneClientAdapterImpl.
 */
public class BasicRootedOzoneClientAdapterImpl
    implements OzoneClientAdapter {

  static final Logger LOG =
      LoggerFactory.getLogger(BasicRootedOzoneClientAdapterImpl.class);

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private ClientProtocol proxy;
  private ReplicationType replicationType;
  private ReplicationFactor replicationFactor;
  private boolean securityEnabled;
  private int configuredDnPort;

  /**
   * Create new OzoneClientAdapter implementation.
   *
   * @throws IOException In case of a problem.
   */
  public BasicRootedOzoneClientAdapterImpl() throws IOException {
    this(createConf());
  }

  private static OzoneConfiguration createConf() {
    ClassLoader contextClassLoader =
        Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    try {
      return new OzoneConfiguration();
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  public BasicRootedOzoneClientAdapterImpl(OzoneConfiguration conf)
      throws IOException {
    this(null, -1, conf);
  }

  public BasicRootedOzoneClientAdapterImpl(String omHost, int omPort,
      ConfigurationSource hadoopConf) throws IOException {

    ClassLoader contextClassLoader =
        Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);

    try {
      OzoneConfiguration conf = OzoneConfiguration.of(hadoopConf);

      if (omHost == null && OmUtils.isServiceIdsDefined(conf)) {
        // When the host name or service id isn't given
        // but ozone.om.service.ids is defined, declare failure.

        // This is a safety precaution that prevents the client from
        // accidentally failing over to an unintended OM.
        throw new IllegalArgumentException("Service ID or host name must not"
            + " be omitted when ozone.om.service.ids is defined.");
      }

      if (omPort != -1) {
        // When the port number is specified, perform the following check
        if (OmUtils.isOmHAServiceId(conf, omHost)) {
          // If omHost is a service id, it shouldn't use a port
          throw new IllegalArgumentException("Port " + omPort +
              " specified in URI but host '" + omHost + "' is a "
              + "logical (HA) OzoneManager and does not use port information.");
        }
      } else {
        // When port number is not specified, read it from config
        omPort = OmUtils.getOmRpcPort(conf);
      }

      SecurityConfig secConfig = new SecurityConfig(conf);

      if (secConfig.isSecurityEnabled()) {
        this.securityEnabled = true;
      }

      String replicationTypeConf =
          conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
              OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT);

      int replicationCountConf = conf.getInt(OzoneConfigKeys.OZONE_REPLICATION,
          OzoneConfigKeys.OZONE_REPLICATION_DEFAULT);

      if (OmUtils.isOmHAServiceId(conf, omHost)) {
        // omHost is listed as one of the service ids in the config,
        // thus we should treat omHost as omServiceId
        this.ozoneClient =
            OzoneClientFactory.getRpcClient(omHost, conf);
      } else if (StringUtils.isNotEmpty(omHost) && omPort != -1) {
        this.ozoneClient =
            OzoneClientFactory.getRpcClient(omHost, omPort, conf);
      } else {
        this.ozoneClient =
            OzoneClientFactory.getRpcClient(conf);
      }
      objectStore = ozoneClient.getObjectStore();
      proxy = objectStore.getClientProxy();
      this.replicationType = ReplicationType.valueOf(replicationTypeConf);
      this.replicationFactor = ReplicationFactor.valueOf(replicationCountConf);
      this.configuredDnPort = conf.getInt(
          OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  OzoneBucket getBucket(OFSPath ofsPath, boolean createIfNotExist)
      throws IOException {

    return getBucket(ofsPath.getVolumeName(), ofsPath.getBucketName(),
        createIfNotExist);
  }

  /**
   * Get OzoneBucket object to operate in.
   * Optionally create volume and bucket if not found.
   *
   * @param createIfNotExist Set this to true if the caller is a write operation
   *                         in order to create the volume and bucket.
   * @throws IOException Exceptions other than OMException with result code
   *                     VOLUME_NOT_FOUND or BUCKET_NOT_FOUND.
   */
  private OzoneBucket getBucket(String volumeStr, String bucketStr,
      boolean createIfNotExist) throws IOException {
    Preconditions.checkNotNull(volumeStr);
    Preconditions.checkNotNull(bucketStr);
    
    if (bucketStr.isEmpty()) {
      // throw FileNotFoundException in this case to make Hadoop common happy
      throw new FileNotFoundException(
          "getBucket: Invalid argument: given bucket string is empty.");
    }

    OzoneBucket bucket;
    try {
      bucket = proxy.getBucketDetails(volumeStr, bucketStr);
    } catch (OMException ex) {
      // Note: always create bucket if volumeStr matches "tmp" so -put works
      if (createIfNotExist) {
        // Note: getBucketDetails always throws BUCKET_NOT_FOUND, even if
        // the volume doesn't exist.
        if (ex.getResult().equals(BUCKET_NOT_FOUND)) {
          OzoneVolume volume;
          try {
            volume = proxy.getVolumeDetails(volumeStr);
          } catch (OMException getVolEx) {
            if (getVolEx.getResult().equals(VOLUME_NOT_FOUND)) {
              // Volume doesn't exist. Create it
              try {
                objectStore.createVolume(volumeStr);
              } catch (OMException newVolEx) {
                // Ignore the case where another client created the volume
                if (!newVolEx.getResult().equals(VOLUME_ALREADY_EXISTS)) {
                  throw newVolEx;
                }
              }
            } else {
              throw getVolEx;
            }
            // Try get volume again
            volume = proxy.getVolumeDetails(volumeStr);
          }
          // Create the bucket
          try {
            volume.createBucket(bucketStr);
          } catch (OMException newBucEx) {
            // Ignore the case where another client created the bucket
            if (!newBucEx.getResult().equals(BUCKET_ALREADY_EXISTS)) {
              throw newBucEx;
            }
          }
        }
        // Try get bucket again
        bucket = proxy.getBucketDetails(volumeStr, bucketStr);
      } else {
        throw ex;
      }
    }

    return bucket;
  }

  @Override
  public short getDefaultReplication() {
    return (short) replicationFactor.getValue();
  }

  @Override
  public void close() throws IOException {
    ozoneClient.close();
  }

  @Override
  public InputStream readFile(String pathStr) throws IOException {
    incrementCounter(Statistic.OBJECTS_READ, 1);
    OFSPath ofsPath = new OFSPath(pathStr);
    String key = ofsPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      return bucket.readFile(key).getInputStream();
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.FILE_NOT_FOUND
          || ex.getResult() == OMException.ResultCodes.NOT_A_FILE) {
        throw new FileNotFoundException(
            ex.getResult().name() + ": " + ex.getMessage());
      } else {
        throw ex;
      }
    }
  }

  protected void incrementCounter(Statistic objectsRead, long count) {
    //noop: Use RootedOzoneClientAdapterImpl which supports statistics.
  }

  @Override
  public OzoneFSOutputStream createFile(String pathStr, short replication,
      boolean overWrite, boolean recursive) throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    OFSPath ofsPath = new OFSPath(pathStr);
    if (ofsPath.isRoot() || ofsPath.isVolume() || ofsPath.isBucket()) {
      throw new IOException("Cannot create file under root or volume.");
    }
    String key = ofsPath.getKeyName();
    try {
      // Hadoop CopyCommands class always sets recursive to true
      OzoneBucket bucket = getBucket(ofsPath, recursive);
      OzoneOutputStream ozoneOutputStream = null;
      if (replication == ReplicationFactor.ONE.getValue()
          || replication == ReplicationFactor.THREE.getValue()) {
        ReplicationFactor clientReplication = ReplicationFactor
            .valueOf(replication);
        ozoneOutputStream = bucket.createFile(key, 0, replicationType,
            clientReplication, overWrite, recursive);
      } else {
        ozoneOutputStream = bucket.createFile(key, 0, replicationType,
            replicationFactor, overWrite, recursive);
      }
      return new OzoneFSOutputStream(ozoneOutputStream.getOutputStream());
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.FILE_ALREADY_EXISTS
          || ex.getResult() == OMException.ResultCodes.NOT_A_FILE) {
        throw new FileAlreadyExistsException(
            ex.getResult().name() + ": " + ex.getMessage());
      } else {
        throw ex;
      }
    }
  }

  @Override
  public void renameKey(String key, String newKeyName) throws IOException {
    throw new IOException("OFS doesn't support renameKey, use rename instead.");
  }

  /**
   * Rename a path into another.
   *
   * In OFS, the parameters for rename are no longer key path, but effectively
   * full path containing volume and bucket. Therefore, the method name
   * renameKey becomes misleading if continued to be used.
   *
   * @param path Source path
   * @param newPath Target path
   * @throws IOException
   */
  @Override
  public void rename(String path, String newPath) throws IOException {
    incrementCounter(Statistic.OBJECTS_RENAMED, 1);
    OFSPath ofsPath = new OFSPath(path);
    OFSPath ofsNewPath = new OFSPath(newPath);

    // Check path and newPathName are in the same volume and same bucket.
    // This should have been checked in BasicRootedOzoneFileSystem#rename
    // already via regular call path unless bypassed.
    if (!ofsPath.isInSameBucketAs(ofsNewPath)) {
      throw new IOException("Can't rename a key to a different bucket.");
    }

    OzoneBucket bucket = getBucket(ofsPath, false);
    String key = ofsPath.getKeyName();
    String newKey = ofsNewPath.getKeyName();
    bucket.renameKey(key, newKey);
  }

  /**
   * Package-private helper function to reduce calls to getBucket().
   * @param bucket Bucket to operate in.
   * @param path Existing key path.
   * @param newPath New key path.
   * @throws IOException IOException from bucket.renameKey().
   */
  void rename(OzoneBucket bucket, String path, String newPath)
      throws IOException {
    incrementCounter(Statistic.OBJECTS_RENAMED, 1);
    OFSPath ofsPath = new OFSPath(path);
    OFSPath ofsNewPath = new OFSPath(newPath);
    // No same-bucket policy check here since this call path is controlled
    String key = ofsPath.getKeyName();
    String newKey = ofsNewPath.getKeyName();
    bucket.renameKey(key, newKey);
  }

  /**
   * Helper method to create an directory specified by key name in bucket.
   *
   * @param pathStr path to be created as directory
   * @return true if the key is created, false otherwise
   */
  @Override
  public boolean createDirectory(String pathStr) throws IOException {
    LOG.trace("creating dir for path: {}", pathStr);
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    OFSPath ofsPath = new OFSPath(pathStr);
    if (ofsPath.getVolumeName().isEmpty()) {
      // Volume name unspecified, invalid param, return failure
      return false;
    }
    if (ofsPath.getBucketName().isEmpty()) {
      // Create volume only
      objectStore.createVolume(ofsPath.getVolumeName());
      return true;
    }
    String keyStr = ofsPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, true);
      // Empty keyStr here indicates only volume and bucket is
      // given in pathStr, so getBucket above should handle the creation
      // of volume and bucket. We won't feed empty keyStr to
      // bucket.createDirectory as that would be a NPE.
      if (keyStr != null && keyStr.length() > 0) {
        bucket.createDirectory(keyStr);
      }
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_ALREADY_EXISTS) {
        throw new FileAlreadyExistsException(e.getMessage());
      }
      throw e;
    }
    return true;
  }

  /**
   * Helper method to delete an object specified by key name in bucket.
   *
   * @param path path to a key to be deleted
   * @return true if the key is deleted, false otherwise
   */
  @Override
  public boolean deleteObject(String path) {
    LOG.trace("issuing delete for path to key: {}", path);
    incrementCounter(Statistic.OBJECTS_DELETED, 1);
    OFSPath ofsPath = new OFSPath(path);
    String keyName = ofsPath.getKeyName();
    if (keyName.length() == 0) {
      return false;
    }
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      bucket.deleteKey(keyName);
      return true;
    } catch (IOException ioe) {
      LOG.error("delete key failed " + ioe.getMessage());
      return false;
    }
  }

  /**
   * Helper function to check if the list of key paths are in the same volume
   * and same bucket.
   */
  private boolean areInSameBucket(List<String> keyNameList) {
    if (keyNameList.isEmpty()) {
      return true;
    }
    String firstKeyPath = keyNameList.get(0);
    final String volAndBucket = new OFSPath(firstKeyPath).getNonKeyPath();
    // return true only if all key paths' volume and bucket in the list match
    // the first element's
    return keyNameList.stream().skip(1).allMatch(p ->
        new OFSPath(p).getNonKeyPath().equals(volAndBucket));
  }

  /**
   * Helper method to delete an object specified by key name in bucket.
   *
   * Only supports deleting keys in the same bucket in one call.
   *
   * Each item in the given list should be the String of an OFS path:
   * e.g. ofs://om/vol1/buck1/k1
   *
   * @param keyNameList key name list to be deleted
   * @return true if the key deletion is successful, false otherwise
   */
  @Override
  public boolean deleteObjects(List<String> keyNameList) {
    if (keyNameList.size() == 0) {
      return true;
    }
    // Sanity check. Support only deleting a list of keys in the same bucket
    if (!areInSameBucket(keyNameList)) {
      LOG.error("Deleting keys from different buckets in a single batch "
          + "is not supported.");
      return false;
    }
    try {
      OFSPath firstKeyPath = new OFSPath(keyNameList.get(0));
      OzoneBucket bucket = getBucket(firstKeyPath, false);
      return deleteObjects(bucket, keyNameList);
    } catch (IOException ioe) {
      LOG.error("delete key failed: {}", ioe.getMessage());
      return false;
    }
  }

  /**
   * Package-private helper function to reduce calls to getBucket().
   *
   * This will be faster than the public variant of the method since this
   * doesn't verify the same-bucket condition.
   *
   * @param bucket Bucket to operate in.
   * @param keyNameList key name list to be deleted.
   * @return true if operation succeeded, false on IOException.
   */
  boolean deleteObjects(OzoneBucket bucket, List<String> keyNameList) {
    List<String> keyList = keyNameList.stream()
        .map(p -> new OFSPath(p).getKeyName())
        .collect(Collectors.toList());
    try {
      incrementCounter(Statistic.OBJECTS_DELETED, keyNameList.size());
      bucket.deleteKeys(keyList);
      return true;
    } catch (IOException ioe) {
      LOG.error("delete key failed: {}", ioe.getMessage());
      return false;
    }
  }

  @Override
  public FileStatusAdapter getFileStatus(String path, URI uri,
      Path qualifiedPath, String userName) throws IOException {
    incrementCounter(Statistic.OBJECTS_QUERY, 1);
    OFSPath ofsPath = new OFSPath(path);
    String key = ofsPath.getKeyName();
    if (ofsPath.isRoot()) {
      return getFileStatusAdapterForRoot(uri);
    }
    if (ofsPath.isVolume()) {
      OzoneVolume volume = objectStore.getVolume(ofsPath.getVolumeName());
      return getFileStatusAdapterForVolume(volume, uri);
    }
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      OzoneFileStatus status = bucket.getFileStatus(key);
      return toFileStatusAdapter(status, userName, uri, qualifiedPath,
          ofsPath.getNonKeyPath());
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        throw new FileNotFoundException(key + ": No such file or directory!");
      } else if (e.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw new FileNotFoundException(key + ": Bucket doesn't exist!");
      }
      throw e;
    }
  }

  /**
   * Get trash roots for current user or all users.
   *
   * Note:
   * 1. When allUsers flag is false, this only returns the trash roots for
   * those that the current user has access to.
   * 2. Also it is not particularly efficient to use this API when there are
   * a lot of volumes and buckets as the client has to iterate through all
   * buckets in all volumes.
   *
   * @param allUsers return trashRoots of all users if true, used by emptier
   * @param fs Pointer to the current OFS FileSystem
   * @return
   */
  public Collection<FileStatus> getTrashRoots(boolean allUsers,
      BasicRootedOzoneFileSystem fs) {
    List<FileStatus> ret = new ArrayList<>();
    try {
      Iterator<? extends OzoneVolume> iterVol;
      String username = UserGroupInformation.getCurrentUser().getUserName();
      if (allUsers) {
        iterVol = objectStore.listVolumes("");
      } else {
        iterVol = objectStore.listVolumesByUser(username, "", "");
      }
      while (iterVol.hasNext()) {
        OzoneVolume volume = iterVol.next();
        Path volumePath = new Path(OZONE_URI_DELIMITER, volume.getName());
        Iterator<? extends OzoneBucket> bucketIter = volume.listBuckets("");
        while (bucketIter.hasNext()) {
          OzoneBucket bucket = bucketIter.next();
          Path bucketPath = new Path(volumePath, bucket.getName());
          Path trashRoot = new Path(bucketPath, FileSystem.TRASH_PREFIX);
          if (allUsers) {
            if (fs.exists(trashRoot)) {
              for (FileStatus candidate : fs.listStatus(trashRoot)) {
                if (fs.exists(candidate.getPath()) && candidate.isDirectory()) {
                  ret.add(candidate);
                }
              }
            }
          } else {
            Path userTrash = new Path(trashRoot, username);
            if (fs.exists(userTrash) &&
                fs.getFileStatus(userTrash).isDirectory()) {
              ret.add(fs.getFileStatus(userTrash));
            }
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Can't get all trash roots", ex);
      return Collections.emptyList();
    }
    return ret;
  }

  @Override
  public Iterator<BasicKeyInfo> listKeys(String pathStr) throws IOException {
    incrementCounter(Statistic.OBJECTS_LIST, 1);
    OFSPath ofsPath = new OFSPath(pathStr);
    String key = ofsPath.getKeyName();
    OzoneBucket bucket;
    try {
      bucket = getBucket(ofsPath, false);
    } catch (IOException ex) {
      // return an empty list on error
      return new IteratorAdapter(Collections.emptyIterator());
    }
    return new IteratorAdapter(bucket.listKeys(key));
  }

  /**
   * Helper for OFS listStatus on root.
   */
  private List<FileStatusAdapter> listStatusRoot(
      boolean recursive, String startPath, long numEntries,
      URI uri, Path workingDir, String username) throws IOException {

    OFSPath ofsStartPath = new OFSPath(startPath);
    // list volumes
    Iterator<? extends OzoneVolume> iter = objectStore.listVolumesByUser(
        username, null, ofsStartPath.getVolumeName());
    List<FileStatusAdapter> res = new ArrayList<>();
    while (iter.hasNext() && res.size() < numEntries) {
      OzoneVolume volume = iter.next();
      res.add(getFileStatusAdapterForVolume(volume, uri));
      if (recursive) {
        String pathStrNextVolume = volume.getName();
        res.addAll(listStatus(pathStrNextVolume, recursive, startPath,
            numEntries - res.size(), uri, workingDir, username));
      }
    }
    return res;
  }

  /**
   * Helper for OFS listStatus on a volume.
   */
  private List<FileStatusAdapter> listStatusVolume(String volumeStr,
      boolean recursive, String startPath, long numEntries,
      URI uri, Path workingDir, String username) throws IOException {

    OFSPath ofsStartPath = new OFSPath(startPath);
    // list buckets in the volume
    OzoneVolume volume = objectStore.getVolume(volumeStr);
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(volume.getOwner());
    String owner = ugi.getShortUserName();
    String group = getGroupName(ugi);
    Iterator<? extends OzoneBucket> iter =
        volume.listBuckets(null, ofsStartPath.getBucketName());
    List<FileStatusAdapter> res = new ArrayList<>();
    while (iter.hasNext() && res.size() < numEntries) {
      OzoneBucket bucket = iter.next();
      res.add(getFileStatusAdapterForBucket(bucket, uri, owner, group));
      if (recursive) {
        String pathStrNext = volumeStr + OZONE_URI_DELIMITER + bucket.getName();
        res.addAll(listStatus(pathStrNext, recursive, startPath,
            numEntries - res.size(), uri, workingDir, username));
      }
    }
    return res;
  }

  /**
   * OFS listStatus implementation.
   *
   * @param pathStr Path for the listStatus to operate on.
   *                This takes an absolute path from OFS root.
   * @param recursive Set to true to get keys inside subdirectories.
   * @param startPath Start path of next batch of result for continuation.
   *                  This takes an absolute path from OFS root. e.g.
   *                  /volumeA/bucketB/dirC/fileD
   *                  Note startPath can optionally begin with uri, e.g.
   *                  when uri=ofs://svc1
   *                  startPath=ofs://svc1/volumeA/bucketB/dirC/fileD
   *                  will be accepted, but NOT startPath=ofs://svc2/volumeA/...
   * @param numEntries Number of maximum entries in the batch.
   * @param uri URI of OFS root.
   *            Used in making the return path qualified.
   * @param workingDir Working directory.
   *                   Used in making the return path qualified.
   * @param username User name.
   *                 Used in making the return path qualified.
   * @return A list of FileStatusAdapter.
   * @throws IOException Bucket exception or FileNotFoundException.
   */
  @Override
  public List<FileStatusAdapter> listStatus(String pathStr, boolean recursive,
      String startPath, long numEntries, URI uri,
      Path workingDir, String username) throws IOException {

    incrementCounter(Statistic.OBJECTS_LIST, 1);
    // Remove authority from startPath if it exists
    if (startPath.startsWith(uri.toString())) {
      try {
        startPath = new URI(startPath).getPath();
      } catch (URISyntaxException ex) {
        throw new IOException(ex);
      }
    }
    // Note: startPath could still have authority at this point if it's
    //  authority doesn't match uri. This is by design. In this case,
    //  OFSPath initializer will error out.
    //  The goal is to refuse processing startPaths from other authorities.

    OFSPath ofsPath = new OFSPath(pathStr);
    if (ofsPath.isRoot()) {
      return listStatusRoot(
          recursive, startPath, numEntries, uri, workingDir, username);
    }
    OFSPath ofsStartPath = new OFSPath(startPath);
    if (ofsPath.isVolume()) {
      String startBucket = ofsStartPath.getBucketName();
      return listStatusVolume(ofsPath.getVolumeName(),
          recursive, startBucket, numEntries, uri, workingDir, username);
    }

    String keyName = ofsPath.getKeyName();
    // Internally we need startKey to be passed into bucket.listStatus
    String startKey = ofsStartPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      List<OzoneFileStatus> statuses = bucket
          .listStatus(keyName, recursive, startKey, numEntries);
      // Note: result in statuses above doesn't have volume/bucket path since
      //  they are from the server.
      String ofsPathPrefix = ofsPath.getNonKeyPath();

      List<FileStatusAdapter> result = new ArrayList<>();
      for (OzoneFileStatus status : statuses) {
        result.add(toFileStatusAdapter(status, username, uri, workingDir,
            ofsPathPrefix));
      }
      return result;
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        throw new FileNotFoundException(e.getMessage());
      }
      throw e;
    }
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    if (!securityEnabled) {
      return null;
    }
    Token<OzoneTokenIdentifier> token = ozoneClient.getObjectStore()
        .getDelegationToken(renewer == null ? null : new Text(renewer));
    token.setKind(OzoneTokenIdentifier.KIND_NAME);
    return token;

  }

  public ObjectStore getObjectStore() {
    return objectStore;
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return objectStore.getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return objectStore.getKeyProviderUri();
  }

  @Override
  public String getCanonicalServiceName() {
    return objectStore.getCanonicalServiceName();
  }

  /**
   * Ozone Delegation Token Renewer.
   */
  @InterfaceAudience.Private
  public static class Renewer extends TokenRenewer {

    //Ensure that OzoneConfiguration files are loaded before trying to use
    // the renewer.
    static {
      OzoneConfiguration.activate();
    }

    public Text getKind() {
      return OzoneTokenIdentifier.KIND_NAME;
    }

    @Override
    public boolean handleKind(Text kind) {
      return getKind().equals(kind);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @Override
    public long renew(Token<?> token, Configuration conf)
        throws IOException, InterruptedException {
      Token<OzoneTokenIdentifier> ozoneDt =
          (Token<OzoneTokenIdentifier>) token;
      OzoneClient ozoneClient =
          OzoneClientFactory.getOzoneClient(OzoneConfiguration.of(conf),
              ozoneDt);
      return ozoneClient.getObjectStore().renewDelegationToken(ozoneDt);
    }

    @Override
    public void cancel(Token<?> token, Configuration conf)
        throws IOException, InterruptedException {
      Token<OzoneTokenIdentifier> ozoneDt =
          (Token<OzoneTokenIdentifier>) token;
      OzoneClient ozoneClient =
          OzoneClientFactory.getOzoneClient(OzoneConfiguration.of(conf),
              ozoneDt);
      ozoneClient.getObjectStore().cancelDelegationToken(ozoneDt);
    }
  }

  /**
   * Adapter to convert OzoneKey to a safe and simple Key implementation.
   */
  public static class IteratorAdapter implements Iterator<BasicKeyInfo> {

    private Iterator<? extends OzoneKey> original;

    public IteratorAdapter(Iterator<? extends OzoneKey> listKeys) {
      this.original = listKeys;
    }

    @Override
    public boolean hasNext() {
      return original.hasNext();
    }

    @Override
    public BasicKeyInfo next() {
      OzoneKey next = original.next();
      if (next == null) {
        return null;
      } else {
        return new BasicKeyInfo(
            next.getName(),
            next.getModificationTime().toEpochMilli(),
            next.getDataSize()
        );
      }
    }
  }

  private FileStatusAdapter toFileStatusAdapter(OzoneFileStatus status,
      String owner, URI defaultUri, Path workingDir, String ofsPathPrefix) {
    OmKeyInfo keyInfo = status.getKeyInfo();
    short replication = (short) keyInfo.getFactor().getNumber();
    return new FileStatusAdapter(
        keyInfo.getDataSize(),
        new Path(ofsPathPrefix + OZONE_URI_DELIMITER + keyInfo.getKeyName())
            .makeQualified(defaultUri, workingDir),
        status.isDirectory(),
        replication,
        status.getBlockSize(),
        keyInfo.getModificationTime(),
        keyInfo.getModificationTime(),
        status.isDirectory() ? (short) 00777 : (short) 00666,
        owner,
        owner,
        null,
        getBlockLocations(status)
    );
  }

  /**
   * Helper method to get List of BlockLocation from OM Key info.
   * @param fileStatus Ozone key file status.
   * @return list of block locations.
   */
  private BlockLocation[] getBlockLocations(OzoneFileStatus fileStatus) {

    if (fileStatus == null) {
      return new BlockLocation[0];
    }

    OmKeyInfo keyInfo = fileStatus.getKeyInfo();
    if (keyInfo == null || CollectionUtils.isEmpty(
        keyInfo.getKeyLocationVersions())) {
      return new BlockLocation[0];
    }
    List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups =
        keyInfo.getKeyLocationVersions();
    if (CollectionUtils.isEmpty(omKeyLocationInfoGroups)) {
      return new BlockLocation[0];
    }

    OmKeyLocationInfoGroup omKeyLocationInfoGroup =
        keyInfo.getLatestVersionLocations();
    BlockLocation[] blockLocations = new BlockLocation[
        omKeyLocationInfoGroup.getBlocksLatestVersionOnly().size()];

    int i = 0;
    long offsetOfBlockInFile = 0L;
    for (OmKeyLocationInfo omKeyLocationInfo :
        omKeyLocationInfoGroup.getBlocksLatestVersionOnly()) {
      List<String> hostList = new ArrayList<>();
      List<String> nameList = new ArrayList<>();
      omKeyLocationInfo.getPipeline().getNodes()
          .forEach(dn -> {
            hostList.add(dn.getHostName());
            int port = dn.getPort(
                DatanodeDetails.Port.Name.STANDALONE).getValue();
            if (port == 0) {
              port = configuredDnPort;
            }
            nameList.add(dn.getHostName() + ":" + port);
          });

      String[] hosts = hostList.toArray(new String[hostList.size()]);
      String[] names = nameList.toArray(new String[nameList.size()]);
      BlockLocation blockLocation = new BlockLocation(
          names, hosts, offsetOfBlockInFile,
          omKeyLocationInfo.getLength());
      offsetOfBlockInFile += omKeyLocationInfo.getLength();
      blockLocations[i++] = blockLocation;
    }
    return blockLocations;
  }

  /**
   * Helper function to get the primary group name from a UGI.
   * @param ugi UserGroupInformation
   * @return String of the primary group name, empty String on exception.
   */
  private static String getGroupName(UserGroupInformation ugi) {
    try {
      return ugi.getPrimaryGroupName();
    } catch (IOException ignored) {
      return "";
    }
  }

  /**
   * Generate a FileStatusAdapter for a volume.
   * @param ozoneVolume OzoneVolume object
   * @param uri Full URI to OFS root.
   * @return FileStatusAdapter for a volume.
   */
  private static FileStatusAdapter getFileStatusAdapterForVolume(
      OzoneVolume ozoneVolume, URI uri) {
    String pathStr = uri.toString() +
        OZONE_URI_DELIMITER + ozoneVolume.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFileStatusAdapterForVolume: ozoneVolume={}, pathStr={}",
          ozoneVolume.getName(), pathStr);
    }
    Path path = new Path(pathStr);
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(ozoneVolume.getOwner());
    String owner = ugi.getShortUserName();
    String group = getGroupName(ugi);
    return new FileStatusAdapter(0L, path, true, (short)0, 0L,
        ozoneVolume.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),
        owner, group, path,
        new BlockLocation[0]
    );
  }

  /**
   * Generate a FileStatusAdapter for a bucket.
   * @param ozoneBucket OzoneBucket object.
   * @param uri Full URI to OFS root.
   * @param owner Owner of the parent volume of the bucket.
   * @param group Group of the parent volume of the bucket.
   * @return FileStatusAdapter for a bucket.
   */
  private static FileStatusAdapter getFileStatusAdapterForBucket(
      OzoneBucket ozoneBucket, URI uri, String owner, String group) {
    String pathStr = uri.toString() +
        OZONE_URI_DELIMITER + ozoneBucket.getVolumeName() +
        OZONE_URI_DELIMITER + ozoneBucket.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFileStatusAdapterForBucket: ozoneBucket={}, pathStr={}",
          ozoneBucket.getVolumeName() + OZONE_URI_DELIMITER +
              ozoneBucket.getName(), pathStr);
    }
    Path path = new Path(pathStr);
    return new FileStatusAdapter(0L, path, true, (short)0, 0L,
        ozoneBucket.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),
        owner, group, path, new BlockLocation[0]);
  }

  /**
   * Generate a FileStatusAdapter for OFS root.
   * @param uri Full URI to OFS root.
   * @return FileStatusAdapter for root.
   */
  private static FileStatusAdapter getFileStatusAdapterForRoot(URI uri) {
    // Note that most fields are mimicked from HDFS FileStatus for root,
    //  except modification time, permission, owner and group.
    Path path = new Path(uri.toString() + OZONE_URI_DELIMITER);
    return new FileStatusAdapter(0L, path, true, (short)0, 0L,
        System.currentTimeMillis(), 0L,
        FsPermission.getDirDefault().toShort(),
        null, null, null, new BlockLocation[0]
    );
  }
}
