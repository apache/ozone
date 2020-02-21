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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
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
 * Basic Implementation of the OzoneFileSystem calls.
 * <p>
 * This is the minimal version which doesn't include any statistics.
 * <p>
 * For full featured version use OzoneClientAdapterImpl.
 */
public class BasicRootedOzoneClientAdapterImpl
    implements RootedOzoneClientAdapter {

  static final Logger LOG =
      LoggerFactory.getLogger(BasicRootedOzoneClientAdapterImpl.class);

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private ClientProtocol proxy;
  private ReplicationType replicationType;
  private ReplicationFactor replicationFactor;
  private boolean securityEnabled;

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
      Configuration hadoopConf) throws IOException {

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
  public void close() throws IOException {
    ozoneClient.close();
  }

  @Override
  public InputStream readFile(String pathStr) throws IOException {
    incrementCounter(Statistic.OBJECTS_READ);
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

  protected void incrementCounter(Statistic objectsRead) {
    //noop: Use OzoneClientAdapterImpl which supports statistics.
  }

  @Override
  public OzoneFSOutputStream createFile(String pathStr, boolean overWrite,
      boolean recursive) throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED);
    OFSPath ofsPath = new OFSPath(pathStr);
    String key = ofsPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      OzoneOutputStream ozoneOutputStream = bucket.createFile(
          key, 0, replicationType, replicationFactor, overWrite, recursive);
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

  @Override
  public void rename(String path, String newPath) throws IOException {
    incrementCounter(Statistic.OBJECTS_RENAMED);
    OFSPath ofsPath = new OFSPath(path);
    OFSPath ofsNewPath = new OFSPath(newPath);

    // Check path and newPathName are in the same volume and same bucket.
    // This should have been checked in BasicRootedOzoneFileSystem#rename
    // already via regular call path unless bypassed.
    if (!ofsPath.isInSameBucketAs(ofsNewPath)) {
      throw new IOException("Cannot rename a key to a different bucket");
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
    incrementCounter(Statistic.OBJECTS_RENAMED);
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
    incrementCounter(Statistic.OBJECTS_CREATED);
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
    incrementCounter(Statistic.OBJECTS_DELETED);
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
   * Package-private helper function to reduce calls to getBucket().
   * @param bucket Bucket to operate in.
   * @param path Path to delete.
   * @return true if operation succeeded, false upon IOException.
   */
  boolean deleteObject(OzoneBucket bucket, String path) {
    LOG.trace("issuing delete for path to key: {}", path);
    incrementCounter(Statistic.OBJECTS_DELETED);
    OFSPath ofsPath = new OFSPath(path);
    String keyName = ofsPath.getKeyName();
    if (keyName.length() == 0) {
      return false;
    }
    try {
      bucket.deleteKey(keyName);
      return true;
    } catch (IOException ioe) {
      LOG.error("delete key failed " + ioe.getMessage());
      return false;
    }
  }

  public FileStatusAdapter getFileStatus(String path, URI uri,
      Path qualifiedPath, String userName) throws IOException {
    incrementCounter(Statistic.OBJECTS_QUERY);
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
      // Note: qualifiedPath passed in is good from
      //  BasicRootedOzoneFileSystem#getFileStatus. No need to prepend here.
      makeQualified(status, uri, qualifiedPath, userName);
      return toFileStatusAdapter(status);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        throw new FileNotFoundException(key + ": No such file or directory!");
      }
      throw e;
    }
  }

  public void makeQualified(FileStatus status, URI uri, Path path,
      String username) {
    if (status instanceof OzoneFileStatus) {
      ((OzoneFileStatus) status)
          .makeQualified(uri, path,
              username, username);
    }
  }

  @Override
  public Iterator<BasicKeyInfo> listKeys(String pathStr) {
    incrementCounter(Statistic.OBJECTS_LIST);
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
        username, ofsStartPath.getVolumeName(), null);
    List<FileStatusAdapter> res = new ArrayList<>();
    // TODO: Test continuation
    while (iter.hasNext() && res.size() <= numEntries) {
      OzoneVolume volume = iter.next();
      res.add(getFileStatusAdapterForVolume(volume, uri));
      if (recursive) {
        String pathStrNextVolume = volume.getName();
        // TODO: Check startPath
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
    Iterator<? extends OzoneBucket> iter =
        volume.listBuckets(null, ofsStartPath.getBucketName());
    List<FileStatusAdapter> res = new ArrayList<>();
    // TODO: Test continuation
    while (iter.hasNext() && res.size() <= numEntries) {
      OzoneBucket bucket = iter.next();
      res.add(getFileStatusAdapterForBucket(bucket, uri, username));
      if (recursive) {
        String pathStrNext = volumeStr + OZONE_URI_DELIMITER + bucket.getName();
        // TODO: Check startPath
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
  public List<FileStatusAdapter> listStatus(String pathStr, boolean recursive,
      String startPath, long numEntries, URI uri,
      Path workingDir, String username) throws IOException {

    incrementCounter(Statistic.OBJECTS_LIST);
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
        // Get raw path (without volume and bucket name) and remove leading '/'
        String rawPath = status.getPath().toString().substring(1);
        Path appendedPath = new Path(ofsPathPrefix, rawPath);
        Path qualifiedPath = appendedPath.makeQualified(uri, workingDir);
        makeQualified(status, uri, qualifiedPath, username);
        result.add(toFileStatusAdapter(status));
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
          OzoneClientFactory.getRpcClient(conf);
      return ozoneClient.getObjectStore().renewDelegationToken(ozoneDt);
    }

    @Override
    public void cancel(Token<?> token, Configuration conf)
        throws IOException, InterruptedException {
      Token<OzoneTokenIdentifier> ozoneDt =
          (Token<OzoneTokenIdentifier>) token;
      OzoneClient ozoneClient =
          OzoneClientFactory.getRpcClient(conf);
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

  private FileStatusAdapter toFileStatusAdapter(OzoneFileStatus status) {
    return new FileStatusAdapter(
        status.getLen(),
        status.getPath(),
        status.isDirectory(),
        status.getReplication(),
        status.getBlockSize(),
        status.getModificationTime(),
        status.getAccessTime(),
        status.getPermission().toShort(),
        status.getOwner(),
        status.getGroup(),
        status.getPath()
    );
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
    return new FileStatusAdapter(0L, path, true, (short)0, 0L,
        ozoneVolume.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),
        // TODO: Revisit owner and admin
        ozoneVolume.getOwner(), ozoneVolume.getAdmin(), path
    );
  }

  /**
   * Generate a FileStatusAdapter for a bucket.
   * @param ozoneBucket OzoneBucket object.
   * @param uri Full URI to OFS root.
   * @return FileStatusAdapter for a bucket.
   */
  private static FileStatusAdapter getFileStatusAdapterForBucket(
      OzoneBucket ozoneBucket, URI uri, String username) {
    String pathStr = uri.toString() +
        OZONE_URI_DELIMITER + ozoneBucket.getVolumeName() +
        OZONE_URI_DELIMITER + ozoneBucket.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFileStatusAdapterForBucket: ozoneBucket={}, pathStr={}, "
              + "username={}", ozoneBucket.getVolumeName() + OZONE_URI_DELIMITER
              + ozoneBucket.getName(), pathStr, username);
    }
    Path path = new Path(pathStr);
    return new FileStatusAdapter(0L, path, true, (short)0, 0L,
        ozoneBucket.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),  // TODO: derive from ACLs later
        // TODO: revisit owner and group
        username, username, path);
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
        null, null, null
    );
  }
}
