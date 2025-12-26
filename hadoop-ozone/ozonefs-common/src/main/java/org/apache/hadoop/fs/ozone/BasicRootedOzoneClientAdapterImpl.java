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

import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathPermissionException;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Implementation of the RootedOzoneFileSystem calls.
 * <p>
 * This is the minimal version which doesn't include any statistics.
 * <p>
 * For full featured version use RootedOzoneClientAdapterImpl.
 */
public class BasicRootedOzoneClientAdapterImpl
    implements OzoneClientAdapter {

  private static final Logger LOG =
      LoggerFactory.getLogger(BasicRootedOzoneClientAdapterImpl.class);

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private ClientProtocol proxy;
  private ReplicationConfig clientConfiguredReplicationConfig;
  private boolean securityEnabled;
  private int configuredDnPort;
  private BucketLayout defaultOFSBucketLayout;
  private OzoneConfiguration config;

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

      clientConfiguredReplicationConfig =
          OzoneClientUtils.getClientConfiguredReplicationConfig(conf);

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

      this.configuredDnPort = conf.getInt(
          OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
          OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);

      // Fetches the bucket layout to be used by OFS.
      try {
        initDefaultFsBucketLayout(conf);
      } catch (IOException | RuntimeException exception) {
        // in case of exception, the adapter object will not be
        // initialised making the client object unreachable, close the client
        // to release resources in this case and rethrow.
        ozoneClient.close();
        throw exception;
      }

      config = conf;
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  /**
   * Initialize the default bucket layout to be used by OFS.
   *
   * @param conf OzoneConfiguration
   * @throws OMException In case of unsupported value provided in the config.
   */
  private void initDefaultFsBucketLayout(OzoneConfiguration conf)
      throws OMException {
    try {
      this.defaultOFSBucketLayout = BucketLayout.fromString(
          conf.get(OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT,
              OzoneConfigKeys.OZONE_CLIENT_FS_BUCKET_LAYOUT_DEFAULT));
    } catch (IllegalArgumentException iae) {
      throw new OMException("Unsupported value provided for " +
          OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT +
          ". Supported values are " + BucketLayout.FILE_SYSTEM_OPTIMIZED +
          " and " + BucketLayout.LEGACY + ".",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    // Bucket Layout for buckets created with OFS cannot be OBJECT_STORE.
    if (defaultOFSBucketLayout.equals(BucketLayout.OBJECT_STORE)) {
      throw new OMException(
          "Buckets created with OBJECT_STORE layout do not support file " +
              "system semantics. Supported values for config " +
              OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT +
              " include " + BucketLayout.FILE_SYSTEM_OPTIMIZED + " and " +
              BucketLayout.LEGACY, OMException.ResultCodes.INVALID_REQUEST);
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
    Objects.requireNonNull(volumeStr, "volumeStr == null");
    Objects.requireNonNull(bucketStr, "bucketStr == null");

    if (bucketStr.isEmpty()) {
      // throw FileNotFoundException in this case to make Hadoop common happy
      throw new FileNotFoundException(
          "getBucket: Invalid argument: given bucket string is empty.");
    }

    OzoneBucket bucket;
    try {
      bucket = proxy.getBucketDetails(volumeStr, bucketStr);

      // resolve the bucket layout in case of Link Bucket
      BucketLayout resolvedBucketLayout =
          OzoneClientUtils.resolveLinkBucketLayout(bucket, objectStore,
              new HashSet<>());

      OzoneFSUtils.validateBucketLayout(bucket.getName(), resolvedBucketLayout);
    } catch (OMException ex) {
      if (createIfNotExist) {
        // getBucketDetails can throw VOLUME_NOT_FOUND when the parent volume
        // doesn't exist and ACL is enabled; it can only throw BUCKET_NOT_FOUND
        // when ACL is disabled. Both exceptions need to be handled.
        switch (ex.getResult()) {
        case VOLUME_NOT_FOUND:
          // Create the volume first when the volume doesn't exist
          try {
            objectStore.createVolume(volumeStr);
          } catch (OMException newVolEx) {
            // Ignore the case where another client created the volume
            if (!newVolEx.getResult().equals(VOLUME_ALREADY_EXISTS)) {
              throw newVolEx;
            }
          }
          // No break here. Proceed to create the bucket
        case BUCKET_NOT_FOUND:
          // When BUCKET_NOT_FOUND is thrown, we expect the parent volume
          // exists, so that we don't call create volume and incur
          // unnecessary ACL checks which could lead to unwanted behavior.
          OzoneVolume volume = proxy.getVolumeDetails(volumeStr);
          // Create the bucket
          try {
            // Buckets created by OFS should be in FSO layout
            volume.createBucket(bucketStr,
                BucketArgs.newBuilder().setBucketLayout(
                    this.defaultOFSBucketLayout).build());
          } catch (OMException newBucEx) {
            // Ignore the case where another client created the bucket
            if (!newBucEx.getResult().equals(BUCKET_ALREADY_EXISTS)) {
              throw newBucEx;
            }
          }
          break;
        default:
          // Throw unhandled exception
          throw ex;
        }
        // Try get bucket again
        bucket = proxy.getBucketDetails(volumeStr, bucketStr);
      } else {
        throw ex;
      }
    }

    return bucket;
  }

  /**
   * This API returns the value what is configured at client side only. It could
   * differ from the server side default values. If no replication config
   * configured at client, it will return 3.
   */
  @Override
  public short getDefaultReplication() {
    if (clientConfiguredReplicationConfig == null) {
      // to provide backward compatibility, we are just retuning 3;
      // However we need to handle with the correct behavior.
      // TODO: Please see HDDS-5646
      return (short) ReplicationFactor.THREE.getValue();
    }
    return (short) clientConfiguredReplicationConfig.getRequiredNodes();
  }

  @Override
  public void close() throws IOException {
    ozoneClient.close();
  }

  @Override
  public InputStream readFile(String pathStr) throws IOException {
    incrementCounter(Statistic.OBJECTS_READ, 1);
    OFSPath ofsPath = new OFSPath(pathStr,
        config);
    String key = ofsPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      return bucket.readFile(key).getInputStream();
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.FILE_NOT_FOUND
          || ex.getResult() == OMException.ResultCodes.KEY_NOT_FOUND
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
    OFSPath ofsPath = new OFSPath(pathStr, config);
    if (ofsPath.isRoot() || ofsPath.isVolume() || ofsPath.isBucket()) {
      throw new IOException("Cannot create file under root or volume.");
    }
    String key = ofsPath.getKeyName();
    try {
      // Hadoop CopyCommands class always sets recursive to true
      OzoneBucket bucket = getBucket(ofsPath, recursive);
      OzoneOutputStream ozoneOutputStream = bucket.createFile(key, 0,
          OzoneClientUtils.resolveClientSideReplicationConfig(replication,
              this.clientConfiguredReplicationConfig,
              bucket.getReplicationConfig(), config), overWrite, recursive);
      return new OzoneFSOutputStream(ozoneOutputStream);
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
  public OzoneFSDataStreamOutput createStreamFile(String pathStr,
      short replication, boolean overWrite, boolean recursive)
      throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    OFSPath ofsPath = new OFSPath(pathStr, config);
    if (ofsPath.isRoot() || ofsPath.isVolume() || ofsPath.isBucket()) {
      throw new IOException("Cannot create file under root or volume.");
    }
    String key = ofsPath.getKeyName();
    try {
      // Hadoop CopyCommands class always sets recursive to true
      final OzoneBucket bucket = getBucket(ofsPath, recursive);
      final ReplicationConfig replicationConfig
          = OzoneClientUtils.resolveClientSideReplicationConfig(
          replication, clientConfiguredReplicationConfig,
          bucket.getReplicationConfig(), config);
      final OzoneDataStreamOutput out = bucket.createStreamFile(
          key, 0, replicationConfig, overWrite, recursive);
      return new OzoneFSDataStreamOutput(out.getByteBufStreamOutput());
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
    OFSPath ofsPath = new OFSPath(path, config);
    OFSPath ofsNewPath = new OFSPath(newPath, config);

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
    OFSPath ofsPath = new OFSPath(path, config);
    OFSPath ofsNewPath = new OFSPath(newPath, config);
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
    OFSPath ofsPath = new OFSPath(pathStr, config);
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
      if (keyStr != null && !keyStr.isEmpty()) {
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
   * @param recursive recursive deletion of all sub path keys if true,
   *                  otherwise non-recursive
   * @return true if the key is deleted, false otherwise
   */
  @Override
  public boolean deleteObject(String path, boolean recursive)
      throws IOException {
    LOG.trace("issuing delete for path to key: {}", path);
    incrementCounter(Statistic.OBJECTS_DELETED, 1);
    OFSPath ofsPath = new OFSPath(path, config);
    String keyName = ofsPath.getKeyName();
    if (keyName.isEmpty()) {
      return false;
    }
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      bucket.deleteDirectory(keyName, recursive);
      return true;
    } catch (OMException ome) {
      LOG.error("Delete key failed. {}", ome.getMessage());
      if (OMException.ResultCodes.DIRECTORY_NOT_EMPTY == ome.getResult()) {
        throw new PathIsNotEmptyDirectoryException(ome.getMessage());
      } else if (OMException.ResultCodes.INVALID_KEY_NAME == ome.getResult()) {
        throw new PathPermissionException(ome.getMessage());
      }
      return false;
    } catch (IOException ioe) {
      LOG.error("Delete key failed. {}", ioe.getMessage());
      return false;
    }
  }

  @Override
  public boolean deleteObject(String path) throws IOException {
    return deleteObject(path, false);
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
    final String volAndBucket = new OFSPath(firstKeyPath, config)
        .getNonKeyPath();
    // return true only if all key paths' volume and bucket in the list match
    // the first element's
    return keyNameList.stream().skip(1).allMatch(p ->
        new OFSPath(p, config).getNonKeyPath().equals(volAndBucket));
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
    if (keyNameList.isEmpty()) {
      return true;
    }
    // Sanity check. Support only deleting a list of keys in the same bucket
    if (!areInSameBucket(keyNameList)) {
      LOG.error("Deleting keys from different buckets in a single batch "
          + "is not supported.");
      return false;
    }
    try {
      OFSPath firstKeyPath = new OFSPath(keyNameList.get(0), config);
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
        .map(p -> new OFSPath(p, config).getKeyName())
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
    OFSPath ofsPath = new OFSPath(path, config);
    if (ofsPath.isRoot()) {
      return getFileStatusAdapterForRoot(uri);
    } else if (ofsPath.isVolume()) {
      OzoneVolume volume = objectStore.getVolume(ofsPath.getVolumeName());
      return getFileStatusAdapterForVolume(volume, uri);
    } else {
      return getFileStatusForKeyOrSnapshot(
          ofsPath, uri, qualifiedPath, userName);
    }
  }

  /**
   * Return FileStatusAdapter based on OFSPath being a
   * valid bucket path or valid snapshot path.
   * Throws exception in case of failure.
   */
  private FileStatusAdapter getFileStatusForKeyOrSnapshot(
      OFSPath ofsPath, URI uri, Path qualifiedPath, String userName)
      throws IOException {
    String key = ofsPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      if (ofsPath.isSnapshotPath()) {
        OzoneVolume volume = objectStore.getVolume(ofsPath.getVolumeName());
        return getFileStatusAdapterWithSnapshotIndicator(
            volume, bucket, uri);
      } else {
        OzoneFileStatus status = bucket.getFileStatus(key);
        return toFileStatusAdapter(status, userName, uri, qualifiedPath,
            ofsPath.getNonKeyPath());
      }
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
   * @return {@code Collection<FileStatus>}
   */
  public Collection<FileStatus> getTrashRoots(boolean allUsers,
      BasicRootedOzoneFileSystem fs) {
    List<FileStatus> ret = new ArrayList<>();
    try {
      Iterator<? extends OzoneVolume> iterVol;
      final String username =
              UserGroupInformation.getCurrentUser().getShortUserName();
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
    OFSPath ofsPath = new OFSPath(pathStr, config);
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
      URI uri, Path workingDir, String username, boolean lite) throws IOException {

    OFSPath ofsStartPath = new OFSPath(startPath, config);
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
            numEntries - res.size(), uri, workingDir, username, lite));
      }
    }
    return res;
  }

  /**
   * Helper for OFS listStatus on a volume.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private List<FileStatusAdapter> listStatusVolume(String volumeStr,
      boolean recursive, String startPath, long numEntries,
      URI uri, Path workingDir, String username, boolean lite) throws IOException {

    OFSPath ofsStartPath = new OFSPath(startPath, config);
    // list buckets in the volume
    OzoneVolume volume = objectStore.getVolume(volumeStr);
    Iterator<? extends OzoneBucket> iter =
        volume.listBuckets(null, ofsStartPath.getBucketName());
    List<FileStatusAdapter> res = new ArrayList<>();
    while (iter.hasNext() && res.size() < numEntries) {
      OzoneBucket bucket = iter.next();
      res.add(getFileStatusAdapterForBucket(bucket, uri));
      if (recursive) {
        String pathStrNext = volumeStr + OZONE_URI_DELIMITER + bucket.getName();
        res.addAll(listStatus(pathStrNext, recursive, startPath,
            numEntries - res.size(), uri, workingDir, username, lite));
      }
    }
    return res;
  }

  /**
   * Helper for OFS listStatus on a bucket to get all snapshots.
   */
  private List<FileStatusAdapter> listStatusBucketSnapshot(
      String volumeName, String bucketName, URI uri, String prevSnapshot, long numberOfEntries) throws IOException {

    OzoneBucket ozoneBucket = getBucket(volumeName, bucketName, false);
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(ozoneBucket.getOwner());
    String owner = ugi.getShortUserName();
    String group = getGroupName(ugi);
    List<FileStatusAdapter> res = new ArrayList<>();

    Iterator<OzoneSnapshot> snapshotIter = objectStore.listSnapshot(volumeName, bucketName, null, prevSnapshot);

    while (snapshotIter.hasNext() && res.size() < numberOfEntries) {
      OzoneSnapshot ozoneSnapshot = snapshotIter.next();
      if (SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE.name().equals(ozoneSnapshot.getSnapshotStatus())) {
        res.add(getFileStatusAdapterForBucketSnapshot(
            ozoneBucket, ozoneSnapshot, uri, owner, group));
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
   * @param lite true if lightweight response needs to be returned otherwise false.
   * @return A list of FileStatusAdapter.
   * @throws IOException Bucket exception or FileNotFoundException.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  @Override
  public List<FileStatusAdapter> listStatus(String pathStr, boolean recursive,
      String startPath, long numEntries, URI uri,
      Path workingDir, String username, boolean lite) throws IOException {

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

    OFSPath ofsPath = new OFSPath(pathStr, config);
    if (ofsPath.isRoot()) {
      return listStatusRoot(
          recursive, startPath, numEntries, uri, workingDir, username, lite);
    }
    OFSPath ofsStartPath = new OFSPath(startPath, config);
    if (ofsPath.isVolume()) {
      String startBucketPath = ofsStartPath.getNonKeyPath();
      return listStatusVolume(ofsPath.getVolumeName(),
          recursive, startBucketPath, numEntries, uri, workingDir, username, lite);
    }

    if (ofsPath.isSnapshotPath()) {
      return listStatusBucketSnapshot(ofsPath.getVolumeName(),
          ofsPath.getBucketName(), uri, ofsStartPath.getSnapshotName(), numEntries);
    }
    List<FileStatusAdapter> result = new ArrayList<>();
    String keyName = ofsPath.getKeyName();
    // Internally we need startKey to be passed into bucket.listStatus
    String startKey = ofsStartPath.getKeyName();
    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      List<OzoneFileStatus> statuses = Collections.emptyList();
      List<OzoneFileStatusLight> lightStatuses = Collections.emptyList();
      if (bucket.isSourcePathExist()) {
        if (lite) {
          lightStatuses = bucket.listStatusLight(keyName, recursive, startKey, numEntries);
        } else {
          statuses = bucket.listStatus(keyName, recursive, startKey, numEntries);
        }

      } else {
        LOG.warn("Source Bucket does not exist, link bucket {} is orphan " +
            "and returning empty list of files inside it", bucket.getName());
      }
      
      // Note: result in statuses above doesn't have volume/bucket path since
      //  they are from the server.
      String ofsPathPrefix = ofsPath.getNonKeyPath();

      if (lite) {
        for (OzoneFileStatusLight status : lightStatuses) {
          result.add(toFileStatusAdapter(status, username, uri, workingDir, ofsPathPrefix));
        }
      } else {
        for (OzoneFileStatus status : statuses) {
          result.add(toFileStatusAdapter(status, username, uri, workingDir, ofsPathPrefix));
        }
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
  public OzoneFsServerDefaults getServerDefaults() throws IOException {
    return objectStore.getServerDefaults();
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
    short replication = (short) keyInfo.getReplicationConfig()
        .getRequiredNodes();
    return new FileStatusAdapter(
        keyInfo.getDataSize(),
        keyInfo.getReplicatedSize(),
        new Path(ofsPathPrefix + OZONE_URI_DELIMITER + keyInfo.getKeyName())
            .makeQualified(defaultUri, workingDir),
        status.isDirectory(),
        replication,
        status.getBlockSize(),
        keyInfo.getModificationTime(),
        keyInfo.getModificationTime(),
        status.isDirectory() ? (short) 00777 : (short) 00666,
        StringUtils.defaultIfEmpty(keyInfo.getOwnerName(), owner),
        owner,
        null,
        getBlockLocations(status),
        OzoneClientUtils.isKeyEncrypted(keyInfo),
        OzoneClientUtils.isKeyErasureCode(keyInfo)
    );
  }

  private FileStatusAdapter toFileStatusAdapter(OzoneFileStatusLight status,
      String owner, URI defaultUri, Path workingDir, String ofsPathPrefix) {
    BasicOmKeyInfo keyInfo = status.getKeyInfo();
    short replication = (short) keyInfo.getReplicationConfig()
        .getRequiredNodes();
    return new FileStatusAdapter(
        keyInfo.getDataSize(),
        keyInfo.getReplicatedSize(),
        new Path(ofsPathPrefix + OZONE_URI_DELIMITER + keyInfo.getKeyName())
            .makeQualified(defaultUri, workingDir),
        status.isDirectory(),
        replication,
        status.getBlockSize(),
        keyInfo.getModificationTime(),
        keyInfo.getModificationTime(),
        status.isDirectory() ? (short) 00777 : (short) 00666,
        StringUtils.defaultIfEmpty(keyInfo.getOwnerName(), owner),
        owner,
        null,
        getBlockLocations(null),
        keyInfo.isEncrypted(),
        OzoneClientUtils.isKeyErasureCode(keyInfo)
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
            int port = dn.getStandalonePort().getValue();
            if (port == 0) {
              port = configuredDnPort;
            }
            nameList.add(dn.getHostName() + ":" + port);
          });

      String[] hosts = hostList.toArray(new String[0]);
      String[] names = nameList.toArray(new String[0]);
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
    return new FileStatusAdapter(0L, 0L, path, true, (short)0, 0L,
        ozoneVolume.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),
        owner, group, null, new BlockLocation[0], false, false
    );
  }

  /**
   * Generate a FileStatusAdapter for a bucket.
   * @param ozoneBucket OzoneBucket object.
   * @param uri Full URI to OFS root.
   * @return FileStatusAdapter for a bucket.
   */
  private static FileStatusAdapter getFileStatusAdapterForBucket(OzoneBucket ozoneBucket, URI uri) {
    String pathStr = uri.toString() +
        OZONE_URI_DELIMITER + ozoneBucket.getVolumeName() +
        OZONE_URI_DELIMITER + ozoneBucket.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFileStatusAdapterForBucket: ozoneBucket={}, pathStr={}",
          ozoneBucket.getVolumeName() + OZONE_URI_DELIMITER +
              ozoneBucket.getName(), pathStr);
    }
    Path path = new Path(pathStr);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(ozoneBucket.getOwner());
    String owner = ugi.getShortUserName();
    String group = getGroupName(ugi);
    return new FileStatusAdapter(0L, 0L, path, true, (short)0, 0L,
        ozoneBucket.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),
        owner, group, null, new BlockLocation[0],
        !StringUtils.isEmpty(ozoneBucket.getEncryptionKeyName()),
        ozoneBucket.getReplicationConfig() != null &&
                    ozoneBucket.getReplicationConfig().getReplicationType() ==
                    HddsProtos.ReplicationType.EC);
  }

  /**
   * Generate a FileStatusAdapter for a snapshot under a bucket.
   * @param ozoneBucket OzoneBucket object.
   * @param ozoneSnapshot OzoneSnapshot object.
   * @param uri Full URI to OFS root.
   * @param owner Owner of the parent volume of the bucket.
   * @param group Group of the parent volume of the bucket.
   * @return FileStatusAdapter for a snapshot.
   */
  private static FileStatusAdapter getFileStatusAdapterForBucketSnapshot(
      OzoneBucket ozoneBucket, OzoneSnapshot ozoneSnapshot,
      URI uri, String owner, String group) {
    String pathStr = uri.toString() +
        OZONE_URI_DELIMITER + ozoneSnapshot.getVolumeName() +
        OZONE_URI_DELIMITER + ozoneSnapshot.getBucketName() +
        OZONE_URI_DELIMITER + OM_SNAPSHOT_INDICATOR +
        OZONE_URI_DELIMITER + ozoneSnapshot.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFileStatusAdapterForBucketSnapshot: " +
              "ozoneSnapshot={}, pathStr={}",
          ozoneSnapshot.getName(), pathStr);
    }
    Path path = new Path(pathStr);
    return new FileStatusAdapter(
        ozoneSnapshot.getReferencedSize(),
        ozoneSnapshot.getReferencedReplicatedSize(),
        path, true, (short) 0, 0L,
        ozoneSnapshot.getCreationTime(), 0L,
        FsPermission.getDirDefault().toShort(),
        owner, group, null, new BlockLocation[0],
        !StringUtils.isEmpty(ozoneBucket.getEncryptionKeyName()),
        ozoneBucket.getReplicationConfig() != null &&
            ozoneBucket.getReplicationConfig().getReplicationType() ==
                HddsProtos.ReplicationType.EC);
  }

  /**
   * Generate a FileStatusAdapter for a bucket
   * followed by a snapshot indicator.
   * @param ozoneVolume OzoneVolume object.
   * @param ozoneBucket OzoneBucket object.
   * @param uri Full URI to OFS root.
   * @return FileStatusAdapter for a snapshot indicator.
   */
  private static FileStatusAdapter getFileStatusAdapterWithSnapshotIndicator(
      OzoneVolume ozoneVolume, OzoneBucket ozoneBucket, URI uri) {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(ozoneVolume.getOwner());
    String owner = ugi.getShortUserName();
    String group = getGroupName(ugi);

    String pathStr = uri.toString() +
        OZONE_URI_DELIMITER + ozoneBucket.getVolumeName() +
        OZONE_URI_DELIMITER + ozoneBucket.getName() +
        OZONE_URI_DELIMITER + OM_SNAPSHOT_INDICATOR;
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFileStatusAdapterWithSnapshotIndicator: " +
              "ozoneBucket={}, pathStr={}",
          ozoneBucket.getVolumeName() + OZONE_URI_DELIMITER +
              ozoneBucket.getName(), pathStr);
    }
    Path path = new Path(pathStr);
    return new FileStatusAdapter(0L, 0L, path, true, (short)0, 0L,
        ozoneBucket.getCreationTime().getEpochSecond() * 1000, 0L,
        FsPermission.getDirDefault().toShort(),
        owner, group, null, new BlockLocation[0],
        !StringUtils.isEmpty(ozoneBucket.getEncryptionKeyName()),
        ozoneBucket.getReplicationConfig() != null &&
            ozoneBucket.getReplicationConfig().getReplicationType() ==
                HddsProtos.ReplicationType.EC);
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
    return new FileStatusAdapter(0L, 0L, path, true, (short)0, 0L,
        System.currentTimeMillis(), 0L,
        FsPermission.getDirDefault().toShort(),
        null, null, null, new BlockLocation[0], false, false);
  }

  @Override
  public boolean isFSOptimizedBucket() {
    // TODO: Need to refine this part.
    return false;
  }

  @Override
  public FileChecksum getFileChecksum(String keyName, long length)
      throws IOException {
    OzoneClientConfig.ChecksumCombineMode combineMode =
        config.getObject(OzoneClientConfig.class).getChecksumCombineMode();
    if (combineMode == null) {
      return null;
    }
    OFSPath ofsPath = new OFSPath(keyName, config);
    OzoneVolume volume = objectStore.getVolume(ofsPath.getVolumeName());
    OzoneBucket bucket = getBucket(ofsPath, false);
    return OzoneClientUtils.getFileChecksumWithCombineMode(
        volume, bucket, ofsPath.getKeyName(),
        length, combineMode,
        ozoneClient.getObjectStore().getClientProxy());

  }

  @Override
  public String createSnapshot(String pathStr, String snapshotName)
          throws IOException {
    OFSPath ofsPath = new OFSPath(pathStr, config);
    return proxy.createSnapshot(ofsPath.getVolumeName(),
            ofsPath.getBucketName(),
            snapshotName);
  }

  @Override
  public void renameSnapshot(String pathStr, String snapshotOldName, String snapshotNewName)
      throws IOException {
    OFSPath ofsPath = new OFSPath(pathStr, config);
    proxy.renameSnapshot(ofsPath.getVolumeName(),
        ofsPath.getBucketName(),
        snapshotOldName,
        snapshotNewName);
  }

  @Override
  public void deleteSnapshot(String pathStr, String snapshotName)
      throws IOException {
    OFSPath ofsPath = new OFSPath(pathStr, config);
    proxy.deleteSnapshot(ofsPath.getVolumeName(),
        ofsPath.getBucketName(),
        snapshotName);
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(Path snapshotDir,
      String fromSnapshot, String toSnapshot)
      throws IOException, InterruptedException {
    boolean takeTemporaryToSnapshot = false;
    boolean takeTemporaryFromSnapshot = false;
    if (toSnapshot.isEmpty()) {
      // empty toSnapshot implies diff b/w the fromSnapshot &
      // current state.
      takeTemporaryToSnapshot = true;
      toSnapshot = createSnapshot(snapshotDir.toString(),
          OzoneFSUtils.generateUniqueTempSnapshotName());
    }
    if (fromSnapshot.isEmpty()) {
      // empty fromSnapshot implies diff b/w the current state
      // & the toSnapshot
      takeTemporaryFromSnapshot = true;
      fromSnapshot = createSnapshot(snapshotDir.toString(),
          OzoneFSUtils.generateUniqueTempSnapshotName());
    }
    OFSPath ofsPath = new OFSPath(snapshotDir, config);
    String volume = ofsPath.getVolumeName();
    String bucket = ofsPath.getBucketName();
    try {
      SnapshotDiffReportOzone aggregated;
      SnapshotDiffReportOzone report =
          getSnapshotDiffReportOnceComplete(fromSnapshot, toSnapshot, volume,
              bucket, "");
      aggregated = report;
      while (StringUtils.isNotEmpty(report.getToken())) {
        LOG.info(
            "Total Snapshot Diff length between snapshot {} and {} exceeds"
                + " max page size, Performing another" +
                " snapdiff with index at {}",
            fromSnapshot, toSnapshot, report.getToken());
        report =
            getSnapshotDiffReportOnceComplete(fromSnapshot, toSnapshot, volume,
                bucket, report.getToken());
        aggregated.aggregate(report);
      }
      return aggregated;
    } finally {
      // delete the temp snapshot
      if (takeTemporaryToSnapshot) {
        OzoneClientUtils.deleteSnapshot(objectStore, toSnapshot, volume, bucket);
      }
      if (takeTemporaryFromSnapshot) {
        OzoneClientUtils.deleteSnapshot(objectStore, fromSnapshot, volume, bucket);
      }
    }
  }

  private SnapshotDiffReportOzone getSnapshotDiffReportOnceComplete(
      String fromSnapshot, String toSnapshot, String volume, String bucket,
      String token) throws IOException, InterruptedException {
    SnapshotDiffResponse snapshotDiffResponse;
    while (true) {
      snapshotDiffResponse =
          objectStore.snapshotDiff(volume, bucket, fromSnapshot, toSnapshot,
              token, -1, false, false);
      if (snapshotDiffResponse.getJobStatus() == DONE) {
        break;
      }
      Thread.sleep(snapshotDiffResponse.getWaitTimeInMs());
    }
    return snapshotDiffResponse.getSnapshotDiffReport();
  }

  @Override
  public boolean isFileClosed(String pathStr) throws IOException {
    incrementCounter(Statistic.INVOCATION_IS_FILE_CLOSED, 1);
    OFSPath ofsPath = new OFSPath(pathStr, config);
    String key = ofsPath.getKeyName();
    if (ofsPath.isRoot() || ofsPath.isVolume()) {
      throw new FileNotFoundException("Path is not a file.");
    } else {
      try {
        OzoneBucket bucket = getBucket(ofsPath, false);
        if (ofsPath.isSnapshotPath()) {
          throw new IOException("file is in a snapshot.");
        } else {
          OzoneFileStatus status = bucket.getFileStatus(key);
          if (!status.isFile()) {
            throw new FileNotFoundException("Path is not a file.");
          }
          return !status.getKeyInfo().isHsync();
        }
      } catch (OMException ome) {
        if (ome.getResult() == FILE_NOT_FOUND ||
            ome.getResult() == VOLUME_NOT_FOUND ||
            ome.getResult() == BUCKET_NOT_FOUND) {
          throw new FileNotFoundException("File does not exist. " + ome.getMessage());
        }
        throw ome;
      }
    }
  }

  @Override
  public LeaseKeyInfo recoverFilePrepare(final String pathStr, boolean force) throws IOException {
    incrementCounter(Statistic.INVOCATION_RECOVER_FILE_PREPARE, 1);
    OFSPath ofsPath = new OFSPath(pathStr, config);

    try {
      OzoneBucket bucket = getBucket(ofsPath, false);
      ClientProtocol clientProtocol = ozoneClient.getProxy();
      return clientProtocol.recoverLease(
          bucket.getVolumeName(), bucket.getName(), ofsPath.getKeyName(), force);
    } catch (OMException ome) {
      if (ome.getResult() == NOT_A_FILE) {
        throw new FileNotFoundException("Path is not a file. " + ome.getMessage());
      } else if (ome.getResult() == KEY_NOT_FOUND ||
          ome.getResult() == DIRECTORY_NOT_FOUND ||
          ome.getResult() == VOLUME_NOT_FOUND ||
          ome.getResult() == BUCKET_NOT_FOUND) {
        throw new FileNotFoundException("File does not exist. " + ome.getMessage());
      }
      throw ome;
    }
  }

  @Override
  public void recoverFile(OmKeyArgs keyArgs) throws IOException {
    incrementCounter(Statistic.INVOCATION_RECOVER_FILE, 1);

    ClientProtocol clientProtocol = ozoneClient.getProxy();
    clientProtocol.recoverKey(keyArgs, 0L);
  }

  @Override
  public long finalizeBlock(OmKeyLocationInfo block) throws IOException {
    incrementCounter(Statistic.INVOCATION_FINALIZE_BLOCK, 1);
    RpcClient rpcClient = (RpcClient) ozoneClient.getProxy();
    XceiverClientFactory xceiverClientFactory = rpcClient.getXceiverClientManager();
    Pipeline pipeline = block.getPipeline();
    XceiverClientSpi client = null;
    try {
      // If pipeline is still open
      if (pipeline.isOpen()) {
        client = xceiverClientFactory.acquireClient(pipeline);
        ContainerProtos.FinalizeBlockResponseProto finalizeBlockResponseProto =
            ContainerProtocolCalls.finalizeBlock(client, block.getBlockID().getDatanodeBlockIDProtobuf(),
                block.getToken());
        return BlockData.getFromProtoBuf(finalizeBlockResponseProto.getBlockData()).getSize();
      }
    } catch (IOException e) {
      LOG.warn("Failed to execute finalizeBlock command", e);
    } finally {
      if (client != null) {
        xceiverClientFactory.releaseClient(client, false);
      }
    }

    // Try fetch block committed length from DN
    ReplicationConfig replicationConfig = pipeline.getReplicationConfig();
    if (!(replicationConfig instanceof ReplicatedReplicationConfig)) {
      throw new IOException("ReplicationConfig type " + replicationConfig.getClass().getSimpleName() +
          " is not supported in finalizeBlock");
    }
    StandaloneReplicationConfig newConfig = StandaloneReplicationConfig.getInstance(
        ((ReplicatedReplicationConfig) replicationConfig).getReplicationFactor());
    Pipeline.Builder builder = Pipeline.newBuilder().setReplicationConfig(newConfig).setId(PipelineID.randomId())
        .setNodes(block.getPipeline().getNodes()).setState(Pipeline.PipelineState.OPEN);
    try {
      client = xceiverClientFactory.acquireClientForReadData(builder.build());
      ContainerProtos.GetCommittedBlockLengthResponseProto responseProto =
          ContainerProtocolCalls.getCommittedBlockLength(client, block.getBlockID(), block.getToken());
      return responseProto.getBlockLength();
    } finally {
      if (client != null) {
        xceiverClientFactory.releaseClient(client, false);
      }
    }
  }

  @Override
  public void setTimes(String key, long mtime, long atime) throws IOException {
    incrementCounter(Statistic.INVOCATION_SET_TIMES, 1);
    OFSPath ofsPath = new OFSPath(key, config);

    OzoneBucket bucket = getBucket(ofsPath, false);
    bucket.setTimes(ofsPath.getKeyName(), mtime, atime);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_SET_SAFE_MODE, 1);

    return ozoneClient.getProxy().getOzoneManagerClient().setSafeMode(
        action, isChecked);
  }
}
