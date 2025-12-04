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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneKey;
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
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Implementation of the OzoneFileSystem calls.
 * <p>
 * This is the minimal version which doesn't include any statistics.
 * <p>
 * For full featured version use OzoneClientAdapterImpl.
 */
public class BasicOzoneClientAdapterImpl implements OzoneClientAdapter {

  static final Logger LOG =
      LoggerFactory.getLogger(BasicOzoneClientAdapterImpl.class);
  public static final String ACTIVE_FS_SNAPSHOT_NAME = ".";

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private ReplicationConfig bucketReplicationConfig;
  // Client side configured replication config.
  private ReplicationConfig clientConfiguredReplicationConfig;
  private boolean securityEnabled;
  private int configuredDnPort;
  private OzoneConfiguration config;
  private long nextReplicationConfigRefreshTime;
  private long bucketRepConfigRefreshPeriodMS;
  private java.time.Clock clock = Clock.system(ZoneOffset.UTC);

  /**
   * Create new OzoneClientAdapter implementation.
   *
   * @param volumeStr Name of the volume to use.
   * @param bucketStr Name of the bucket to use
   * @throws IOException In case of a problem.
   */
  public BasicOzoneClientAdapterImpl(String volumeStr, String bucketStr)
      throws IOException {
    this(new OzoneConfiguration(), volumeStr, bucketStr);
  }

  public BasicOzoneClientAdapterImpl(OzoneConfiguration conf, String volumeStr,
      String bucketStr)
      throws IOException {
    this(null, -1, conf, volumeStr, bucketStr);
  }

  public BasicOzoneClientAdapterImpl(String omHost, int omPort,
      ConfigurationSource hadoopConf, String volumeStr, String bucketStr)
      throws IOException {

    OzoneConfiguration conf = OzoneConfiguration.of(hadoopConf);
    bucketRepConfigRefreshPeriodMS = conf.getLong(
        OzoneConfigKeys
            .OZONE_CLIENT_BUCKET_REPLICATION_CONFIG_REFRESH_PERIOD_MS,
        OzoneConfigKeys
            .OZONE_CLIENT_BUCKET_REPLICATION_CONFIG_REFRESH_PERIOD_DEFAULT_MS);
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
    try {
      this.volume = objectStore.getVolume(volumeStr);
      this.bucket = volume.getBucket(bucketStr);
      bucketReplicationConfig = this.bucket.getReplicationConfig();
      nextReplicationConfigRefreshTime = clock.millis() + bucketRepConfigRefreshPeriodMS;

      // resolve the bucket layout in case of Link Bucket
      BucketLayout resolvedBucketLayout =
          OzoneClientUtils.resolveLinkBucketLayout(bucket, objectStore, new HashSet<>());

      OzoneFSUtils.validateBucketLayout(bucket.getName(), resolvedBucketLayout);
    } catch (IOException | RuntimeException exception) {
      // in case of exception, the adapter object will not be
      // initialised making the client object unreachable, close the client
      // to release resources in this case and rethrow.
      ozoneClient.close();
      throw exception;
    }

    this.configuredDnPort = conf.getInt(
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
    this.config = conf;
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
  public InputStream readFile(String key) throws IOException {
    incrementCounter(Statistic.OBJECTS_READ, 1);
    try {
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
    //noop: Use OzoneClientAdapterImpl which supports statistics.
  }

  @Override
  public OzoneFSOutputStream createFile(String key, short replication,
      boolean overWrite, boolean recursive) throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    try {
      OzoneOutputStream ozoneOutputStream = bucket.createFile(key, 0,
          OzoneClientUtils.resolveClientSideReplicationConfig(replication,
              this.clientConfiguredReplicationConfig,
              getReplicationConfigWithRefreshCheck(), config), overWrite,
          recursive);
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

  private ReplicationConfig getReplicationConfigWithRefreshCheck()
      throws IOException {
    if (clock.millis() > nextReplicationConfigRefreshTime) {
      this.bucketReplicationConfig =
          volume.getBucket(bucket.getName()).getReplicationConfig();
      nextReplicationConfigRefreshTime =
          clock.millis() + bucketRepConfigRefreshPeriodMS;
    }
    return this.bucketReplicationConfig;
  }

  @Override
  public OzoneFSDataStreamOutput createStreamFile(String key, short replication,
      boolean overWrite, boolean recursive) throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    try {
      final ReplicationConfig replicationConfig
          = OzoneClientUtils.resolveClientSideReplicationConfig(
          replication, clientConfiguredReplicationConfig,
          getReplicationConfigWithRefreshCheck(), config);
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
    incrementCounter(Statistic.OBJECTS_RENAMED, 1);
    bucket.renameKey(key, newKeyName);
  }

  @Override
  public void rename(String pathStr, String newPath) throws IOException {
    throw new IOException("Please use renameKey instead for o3fs.");
  }

  /**
   * Helper method to create an directory specified by key name in bucket.
   *
   * @param keyName key name to be created as directory
   * @return true if the key is created, false otherwise
   */
  @Override
  public boolean createDirectory(String keyName) throws IOException {
    LOG.trace("creating dir for key:{}", keyName);
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    try {
      bucket.createDirectory(keyName);
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
   * @param keyName key name to be deleted
   * @return true if the key is deleted, false otherwise
   */
  @Override
  public boolean deleteObject(String keyName) throws IOException {
    return deleteObject(keyName, false);
  }

  /**
   * Helper method to delete an object specified by key name in bucket.
   *
   * @param keyName key name to be deleted
   * @param recursive recursive deletion of all sub path keys if true,
   *                  otherwise non-recursive
   * @return true if the key is deleted, false otherwise
   */
  @Override
  public boolean deleteObject(String keyName, boolean recursive)
      throws IOException {
    LOG.trace("issuing delete for key {}", keyName);
    try {
      incrementCounter(Statistic.OBJECTS_DELETED, 1);
      bucket.deleteDirectory(keyName, recursive);
      return true;
    } catch (OMException ome) {
      if (OMException.ResultCodes.KEY_NOT_FOUND == ome.getResult()) {
        LOG.warn("delete key failed {}", ome.getMessage());
        return false;
      }
      LOG.error("delete key failed {}", ome.getMessage());
      if (OMException.ResultCodes.DIRECTORY_NOT_EMPTY == ome.getResult()) {
        throw new PathIsNotEmptyDirectoryException(ome.getMessage());
      }
      return false;
    } catch (IOException ioe) {
      LOG.error("delete key failed {}", ioe.getMessage());
      return false;
    }
  }

  /**
   * Helper method to delete an object specified by key name in bucket.
   *
   * @param keyNameList key name list to be deleted
   * @return true if the key is deleted, false otherwise
   */
  @Override
  public boolean deleteObjects(List<String> keyNameList) {
    try {
      incrementCounter(Statistic.OBJECTS_DELETED, keyNameList.size());
      bucket.deleteKeys(keyNameList);
      return true;
    } catch (IOException ioe) {
      LOG.error("delete key failed {}", ioe.getMessage());
      return false;
    }
  }

  @Override
  public FileStatusAdapter getFileStatus(String key, URI uri,
      Path qualifiedPath, String userName)
      throws IOException {
    try {
      incrementCounter(Statistic.OBJECTS_QUERY, 1);
      OzoneFileStatus status = bucket.getFileStatus(key);
      return toFileStatusAdapter(status, userName, uri, qualifiedPath);

    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        throw new
            FileNotFoundException(key + ": No such file or directory!");
      }
      throw e;
    }
  }

  @Override
  public Iterator<BasicKeyInfo> listKeys(String pathKey) throws IOException {
    incrementCounter(Statistic.OBJECTS_LIST, 1);
    return new IteratorAdapter(bucket.listKeys(pathKey));
  }

  @Override
  public List<FileStatusAdapter> listStatus(String keyName, boolean recursive,
      String startKey, long numEntries, URI uri,
      Path workingDir, String username, boolean lite) throws IOException {
    try {
      incrementCounter(Statistic.OBJECTS_LIST, 1);
      List<FileStatusAdapter> result = new ArrayList<>();
      if (lite) {
        List<OzoneFileStatusLight> statuses = bucket
            .listStatusLight(keyName, recursive, startKey, numEntries);
        for (OzoneFileStatusLight status : statuses) {
          result.add(toFileStatusAdapter(status, username, uri, workingDir));
        }
      } else {
        List<OzoneFileStatus> statuses = bucket
            .listStatus(keyName, recursive, startKey, numEntries);
        for (OzoneFileStatus status : statuses) {
          result.add(toFileStatusAdapter(status, username, uri, workingDir));
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

  @VisibleForTesting
  void setClock(Clock monotonicClock) {
    this.clock = monotonicClock;
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
      String owner, URI defaultUri, Path workingDir) {
    OmKeyInfo keyInfo = status.getKeyInfo();
    short replication = (short) keyInfo.getReplicationConfig()
        .getRequiredNodes();
    return new FileStatusAdapter(
        keyInfo.getDataSize(),
        keyInfo.getReplicatedSize(),
        new Path(OZONE_URI_DELIMITER + keyInfo.getKeyName())
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
                                                String owner, URI defaultUri, Path workingDir) {
    BasicOmKeyInfo keyInfo = status.getKeyInfo();
    short replication = (short) keyInfo.getReplicationConfig()
        .getRequiredNodes();
    return new FileStatusAdapter(
        keyInfo.getDataSize(),
        keyInfo.getReplicatedSize(),
        new Path(OZONE_URI_DELIMITER + keyInfo.getKeyName())
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

  @Override
  public boolean isFSOptimizedBucket() {
    return bucket.getBucketLayout().isFileSystemOptimized();
  }

  @Override
  public FileChecksum getFileChecksum(String keyName, long length)
      throws IOException {
    OzoneClientConfig.ChecksumCombineMode combineMode =
        config.getObject(OzoneClientConfig.class).getChecksumCombineMode();
    if (combineMode == null) {
      return null;
    }
    return OzoneClientUtils.getFileChecksumWithCombineMode(
        volume, bucket, keyName,
        length, combineMode,
        ozoneClient.getObjectStore().getClientProxy());
  }

  @Override
  public String createSnapshot(String pathStr, String snapshotName)
      throws IOException {
    return objectStore.createSnapshot(volume.getName(), bucket.getName(), snapshotName);
  }

  @Override
  public void renameSnapshot(String pathStr, String snapshotOldName, String snapshotNewName)
      throws IOException {
    objectStore.renameSnapshot(volume.getName(),
        bucket.getName(),
        snapshotOldName,
        snapshotNewName);
  }

  @Override
  public void deleteSnapshot(String pathStr, String snapshotName)
      throws IOException {
    objectStore.deleteSnapshot(volume.getName(),
        bucket.getName(),
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
    try {
      SnapshotDiffReportOzone aggregated;
      SnapshotDiffReportOzone report =
          getSnapshotDiffReportOnceComplete(fromSnapshot, toSnapshot, "");
      aggregated = report;
      while (StringUtils.isNotEmpty(report.getToken())) {
        LOG.info(
            "Total Snapshot Diff length between snapshot {} and {} exceeds"
                + " max page size, Performing another " +
                "snapdiff with index at {}",
            fromSnapshot, toSnapshot, report.getToken());
        report = getSnapshotDiffReportOnceComplete(fromSnapshot, toSnapshot,
            report.getToken());
        aggregated.aggregate(report);
      }
      return aggregated;
    } finally {
      // delete the temp snapshot
      if (takeTemporaryToSnapshot || takeTemporaryFromSnapshot) {
        if (takeTemporaryToSnapshot) {
          OzoneClientUtils.deleteSnapshot(objectStore, toSnapshot, volume.getName(), bucket.getName());
        }
        if (takeTemporaryFromSnapshot) {
          OzoneClientUtils.deleteSnapshot(objectStore, fromSnapshot, volume.getName(), bucket.getName());
        }
      }
    }
  }

  private SnapshotDiffReportOzone getSnapshotDiffReportOnceComplete(
      String fromSnapshot, String toSnapshot, String token)
      throws IOException, InterruptedException {
    SnapshotDiffResponse snapshotDiffResponse;
    while (true) {
      snapshotDiffResponse =
          objectStore.snapshotDiff(volume.getName(), bucket.getName(),
              fromSnapshot, toSnapshot, token, -1, false, false);
      if (snapshotDiffResponse.getJobStatus() == DONE) {
        break;
      }
      Thread.sleep(snapshotDiffResponse.getWaitTimeInMs());
    }
    return snapshotDiffResponse.getSnapshotDiffReport();
  }

  @Override
  public LeaseKeyInfo recoverFilePrepare(final String pathStr, boolean force) throws IOException {
    incrementCounter(Statistic.INVOCATION_RECOVER_FILE_PREPARE, 1);

    try {
      ClientProtocol clientProtocol = ozoneClient.getProxy();
      return clientProtocol.recoverLease(volume.getName(), bucket.getName(), pathStr, force);
    } catch (OMException ome) {
      if (ome.getResult() == NOT_A_FILE) {
        throw new FileNotFoundException("Path is not a file. " + ome.getMessage());
      } else if (ome.getResult() == KEY_NOT_FOUND ||
          ome.getResult() == DIRECTORY_NOT_FOUND) {
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
    bucket.setTimes(key, mtime, atime);
  }

  @Override
  public boolean isFileClosed(String pathStr) throws IOException {
    incrementCounter(Statistic.INVOCATION_IS_FILE_CLOSED, 1);
    if (StringUtils.isEmpty(pathStr)) {
      throw new IOException("not a file");
    }
    try {
      OzoneFileStatus status = bucket.getFileStatus(pathStr);
      if (!status.isFile()) {
        throw new FileNotFoundException("Path is not a file.");
      }
      return !status.getKeyInfo().isHsync();
    } catch (OMException ome) {
      if (ome.getResult() == FILE_NOT_FOUND) {
        throw new FileNotFoundException("File does not exist. " + ome.getMessage());
      }
      throw ome;
    }
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_SET_SAFE_MODE, 1);

    return ozoneClient.getProxy().getOzoneManagerClient().setSafeMode(
        action, isChecked);
  }
}
