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

import static org.apache.hadoop.ozone.OzoneConsts.FORCE_LEASE_RECOVERY_ENV;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderTokenIssuer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.DelegationTokenIssuer;

/**
 * The Rooted Ozone Filesystem (OFS) implementation.
 * <p>
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(org.apache.hadoop.conf.Configuration)}
 * and variants to create one. If cast to {@link RootedOzoneFileSystem}, extra
 * methods and features may be accessed. Consider those private and unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RootedOzoneFileSystem extends BasicRootedOzoneFileSystem
    implements KeyProviderTokenIssuer, LeaseRecoverable, SafeMode {

  private OzoneFSStorageStatistics storageStatistics;
  private boolean forceRecovery;

  public RootedOzoneFileSystem() {
    this.storageStatistics = new OzoneFSStorageStatistics();
    String force = System.getProperty(FORCE_LEASE_RECOVERY_ENV);
    forceRecovery = Boolean.parseBoolean(force);
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return getAdapter().getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return getAdapter().getKeyProviderUri();
  }

  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    KeyProvider keyProvider;
    try {
      keyProvider = getKeyProvider();
    } catch (IOException ioe) {
      LOG.debug("Error retrieving KeyProvider.", ioe);
      return null;
    }
    if (keyProvider instanceof DelegationTokenIssuer) {
      return new DelegationTokenIssuer[]{(DelegationTokenIssuer)keyProvider};
    }
    return null;
  }

  StorageStatistics getOzoneFSOpsCountStatistics() {
    return storageStatistics;
  }

  @Override
  protected void incrementCounter(Statistic statistic, long count) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(statistic, count);
    }
  }

  @Override
  protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
      String omHost, int omPort) throws IOException {
    return new RootedOzoneClientAdapterImpl(omHost, omPort, conf,
        storageStatistics);
  }

  @Override
  protected InputStream createFSInputStream(InputStream inputStream) {
    return new CapableOzoneFSInputStream(inputStream, statistics);
  }

  @Override
  protected OzoneFSOutputStream createFSOutputStream(
          OzoneFSOutputStream outputStream) {
    return new CapableOzoneFSOutputStream(outputStream, isHsyncEnabled());
  }

  @Override
  protected OzoneFSDataStreamOutput createFSDataStreamOutput(
      OzoneFSDataStreamOutput outputDataStream) {
    return new CapableOzoneFSDataStreamOutput(outputDataStream, isHsyncEnabled());
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    // qualify the path to make sure that it refers to the current FS.
    final Path p = makeQualified(path);
    boolean cap =
        OzonePathCapabilities.hasPathCapability(p, capability);
    if (cap) {
      return cap;
    }
    return super.hasPathCapability(p, capability);
  }

  @Override
  public boolean recoverLease(final Path f) throws IOException {
    return TracingUtil.executeInNewSpan("ofs recoverLease",
        () -> recoverLeaseTraced(f));
  }

  private boolean recoverLeaseTraced(final Path f) throws IOException {
    TracingUtil.getActiveSpan().setAttribute("path", f.toString());
    statistics.incrementWriteOps(1);
    LOG.trace("recoverLease() path:{}", f);
    Path qualifiedPath = makeQualified(f);
    String key = pathToKey(qualifiedPath);
    LeaseKeyInfo leaseKeyInfo;
    try {
      leaseKeyInfo = getAdapter().recoverFilePrepare(key, forceRecovery);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.KEY_ALREADY_CLOSED) {
        // key is already closed, let's just return success
        return true;
      }
      throw e;
    }


    // Get keyLocationInfo
    List<OmKeyLocationInfo> keyLocationInfoList = LeaseRecoveryClientDNHandler.getOmKeyLocationInfos(
        leaseKeyInfo, getAdapter(), forceRecovery);
    // recover and commit file
    long keyLength = keyLocationInfoList.stream().mapToLong(OmKeyLocationInfo::getLength).sum();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(leaseKeyInfo.getKeyInfo().getVolumeName())
        .setBucketName(leaseKeyInfo.getKeyInfo().getBucketName()).setKeyName(leaseKeyInfo.getKeyInfo().getKeyName())
        .setReplicationConfig(leaseKeyInfo.getKeyInfo().getReplicationConfig()).setDataSize(keyLength)
        .setLocationInfoList(keyLocationInfoList)
        .build();
    getAdapter().recoverFile(keyArgs);
    return true;
  }

  @Override
  public boolean isFileClosed(Path f) throws IOException {
    return TracingUtil.executeInNewSpan("ofs isFileClosed",
        () -> isFileClosedTraced(f));
  }

  private boolean isFileClosedTraced(Path f) throws IOException {
    TracingUtil.getActiveSpan().setAttribute("fs.operation", "isFileClosed");
    statistics.incrementWriteOps(1);
    LOG.trace("isFileClosed() path:{}", f);
    Path qualifiedPath = makeQualified(f);
    String key = pathToKey(qualifiedPath);
    return getAdapter().isFileClosed(key);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    return setSafeModeUtil(action, isChecked);
  }
}
