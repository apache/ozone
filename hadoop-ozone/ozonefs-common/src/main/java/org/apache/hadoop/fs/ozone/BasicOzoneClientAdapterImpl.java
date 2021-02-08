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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
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

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private ReplicationType replicationType;
  private ReplicationFactor replicationFactor;
  private boolean securityEnabled;
  private int configuredDnPort;

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
    this.volume = objectStore.getVolume(volumeStr);
    this.bucket = volume.getBucket(bucketStr);
    this.replicationType = ReplicationType.valueOf(replicationTypeConf);
    this.replicationFactor = ReplicationFactor.valueOf(replicationCountConf);
    this.configuredDnPort = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
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
  public InputStream readFile(String key) throws IOException {
    incrementCounter(Statistic.OBJECTS_READ, 1);
    try {
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
    //noop: Use OzoneClientAdapterImpl which supports statistics.
  }

  @Override
  public OzoneFSOutputStream createFile(String key, short replication,
      boolean overWrite, boolean recursive) throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED, 1);
    try {
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
  public boolean deleteObject(String keyName) {
    LOG.trace("issuing delete for key {}", keyName);
    try {
      incrementCounter(Statistic.OBJECTS_DELETED, 1);
      bucket.deleteKey(keyName);
      return true;
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
  public Iterator<BasicKeyInfo> listKeys(String pathKey) throws IOException{
    incrementCounter(Statistic.OBJECTS_LIST, 1);
    return new IteratorAdapter(bucket.listKeys(pathKey));
  }

  @Override
  public List<FileStatusAdapter> listStatus(String keyName, boolean recursive,
      String startKey, long numEntries, URI uri,
      Path workingDir, String username) throws IOException {
    try {
      incrementCounter(Statistic.OBJECTS_LIST, 1);
      List<OzoneFileStatus> statuses = bucket
          .listStatus(keyName, recursive, startKey, numEntries);

      List<FileStatusAdapter> result = new ArrayList<>();
      for (OzoneFileStatus status : statuses) {
        result.add(toFileStatusAdapter(status, username, uri, workingDir));
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
      String owner, URI defaultUri, Path workingDir) {
    OmKeyInfo keyInfo = status.getKeyInfo();
    short replication = (short) keyInfo.getFactor().getNumber();
    return new FileStatusAdapter(
        keyInfo.getDataSize(),
        new Path(OZONE_URI_DELIMITER + keyInfo.getKeyName())
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

}
